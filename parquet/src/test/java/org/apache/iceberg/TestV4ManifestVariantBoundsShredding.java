/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.data.parquet.InternalWriter;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetManifestTestUtil;
import org.apache.iceberg.parquet.VariantShreddingAnalyzer;
import org.apache.iceberg.parquet.VariantShreddingFunction;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantObject;
import org.apache.iceberg.variants.VariantTestUtil;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;

/**
 * End-to-end: write a V4 manifest whose variant {@code content_stats} lower/upper bounds are
 * <em>shredded</em> (using the same {@code variantShreddingFunc} path data files use), then read
 * it back through the real {@link V4ManifestReader} (from PR #17433) and confirm the bounds
 * reconstruct correctly and can prune files.
 *
 * <p>Lives in the parquet module (needs {@code Parquet.write} + shredding) but in package {@code
 * org.apache.iceberg} to reach the package-private manifest/stats builders. Mirrors the write/read
 * shape of {@code TestV4ManifestReaderStats}, adding shredding to the write.
 */
public class TestV4ManifestVariantBoundsShredding {
  private static final int VAR_FIELD_ID = 12;
  private static final Schema TABLE_SCHEMA =
      new Schema(optional(VAR_FIELD_ID, "var", Types.VariantType.get()));
  private static final Types.StructType STATS_TYPE =
      StatsUtil.statsWriteSchema(
          TABLE_SCHEMA,
          MetricsConfig.from(
              ImmutableMap.of(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "var", "full"),
              TABLE_SCHEMA,
              null));
  private static final Types.StructType VAR_STATS_TYPE = STATS_TYPE.fieldType("var").asStructType();
  private static final int LOWER_BOUND_ID = VAR_STATS_TYPE.field("lower_bound").fieldId();
  private static final int UPPER_BOUND_ID = VAR_STATS_TYPE.field("upper_bound").fieldId();

  private static final Types.StructType EMPTY_PARTITION = Types.StructType.of();
  private static final PartitionData EMPTY_PARTITION_DATA = new PartitionData(EMPTY_PARTITION);
  private static final Map<Integer, PartitionSpec> UNPARTITIONED_SPECS =
      ImmutableMap.of(PartitionSpec.unpartitioned().specId(), PartitionSpec.unpartitioned());
  private static final String TABLE_LOCATION = "s3://bucket/db/table";
  private static final String X = "$['x']";

  @Test
  public void shreddedVariantBoundsRoundTripThroughV4ManifestReader() throws IOException {
    VariantMetadata metadata = Variants.metadata(X);
    // three files, each a data file whose variant "var" has an x-range recorded in its bounds
    List<Variant> lowers =
        IntStream.range(0, 3).mapToObj(i -> bound(metadata, i * 10)).collect(Collectors.toList());
    List<Variant> uppers =
        IntStream.range(0, 3)
            .mapToObj(i -> bound(metadata, i * 10 + 5))
            .collect(Collectors.toList());

    InputFile manifest = writeShreddedManifest(lowers, uppers);

    // the lower/upper bound variant columns are physically shredded on x
    MessageType schema = ParquetManifestTestUtil.parquetSchema(manifest);
    assertThat(
            ParquetManifestTestUtil.hasLeaf(
                schema, "content_stats", "var", "lower_bound", "typed_value", X, "typed_value"))
        .as("lower_bound shredded on x")
        .isTrue();
    assertThat(
            ParquetManifestTestUtil.hasLeaf(
                schema, "content_stats", "var", "upper_bound", "typed_value", X, "typed_value"))
        .as("upper_bound shredded on x")
        .isTrue();

    // the real V4 manifest reader reconstructs the shredded bounds intact
    List<TrackedFile> files = readManifest(manifest);
    assertThat(files).hasSize(3);
    for (int i = 0; i < files.size(); i += 1) {
      FieldStats<?> stats = files.get(i).contentStats().statsFor(VAR_FIELD_ID);
      VariantTestUtil.assertEqual(lowers.get(i).value(), ((Variant) stats.lowerBound()).value());
      VariantTestUtil.assertEqual(uppers.get(i).value(), ((Variant) stats.upperBound()).value());
    }
  }

  @Test
  public void shreddedVariantBoundsPruneFiles() throws IOException {
    VariantMetadata metadata = Variants.metadata(X);
    List<Variant> lowers =
        IntStream.range(0, 6).mapToObj(i -> bound(metadata, i * 10)).collect(Collectors.toList());
    List<Variant> uppers =
        IntStream.range(0, 6)
            .mapToObj(i -> bound(metadata, i * 10 + 5))
            .collect(Collectors.toList());

    List<TrackedFile> files = readManifest(writeShreddedManifest(lowers, uppers));

    // prune with `var.x >= 35`: keep files whose upper bound reaches the threshold
    List<Integer> kept = Lists.newArrayList();
    for (int i = 0; i < files.size(); i += 1) {
      Variant upper = (Variant) files.get(i).contentStats().statsFor(VAR_FIELD_ID).upperBound();
      int x = (int) upper.value().asObject().get(X).asPrimitive().get();
      if (x >= 35) {
        kept.add(i);
      }
    }

    assertThat(kept).containsExactly(3, 4, 5);
  }

  @Test
  public void projectStatsPathsReadsOnlyRequestedPath() throws IOException {
    // bounds carry two shredded paths; the reader is asked for only x
    VariantMetadata metadata = Variants.metadata(X, "$['y']");
    Variant lower = twoPathBound(metadata, 1, 100);
    Variant upper = twoPathBound(metadata, 10, 200);
    InputFile manifest = writeShreddedManifest(List.of(lower), List.of(upper));

    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, TABLE_SCHEMA, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .projectStatsPaths(ImmutableMap.of(VAR_FIELD_ID, java.util.Set.of(X)))
            .build()) {
      FieldStats<?> stats =
          reader.iterator().next().contentStats().statsFor(VAR_FIELD_ID);
      VariantObject lowerBounds = ((Variant) stats.lowerBound()).value().asObject();

      // only the requested path was materialized; y (and any residual) was skipped
      assertThat((int) lowerBounds.get(X).asPrimitive().get()).isEqualTo(1);
      assertThat(lowerBounds.get("$['y']")).isNull();
      assertThat(lowerBounds.numFields()).isEqualTo(1);
    }
  }

  private static Variant twoPathBound(VariantMetadata metadata, int x, int y) {
    ShreddedObject object = Variants.object(metadata);
    object.put(X, Variants.of(x));
    object.put("$['y']", Variants.of(y));
    return Variant.of(metadata, object);
  }

  private static Variant bound(VariantMetadata metadata, int x) {
    ShreddedObject object = Variants.object(metadata);
    object.put(X, Variants.of(x));
    return Variant.of(metadata, object);
  }

  /** Writes a TrackedFile manifest with the variant bound columns shredded. */
  private static InputFile writeShreddedManifest(List<Variant> lowers, List<Variant> uppers)
      throws IOException {
    Schema writeSchema = TrackedFile.schema(EMPTY_PARTITION, STATS_TYPE);

    // derive one shred schema covering both bounds (uniform here), keyed to the bound field ids
    List<VariantValue> boundValues = Lists.newArrayList();
    lowers.forEach(v -> boundValues.add(v.value()));
    uppers.forEach(v -> boundValues.add(v.value()));
    Type shredded = analyze(boundValues);
    VariantShreddingFunction shreddingFunc =
        (fieldId, name) ->
            fieldId == LOWER_BOUND_ID || fieldId == UPPER_BOUND_ID ? shredded : null;

    OutputFile out = new InMemoryOutputFile("manifest.parquet");
    try (FileAppender<StructLike> appender =
        Parquet.write(out)
            .schema(writeSchema)
            .variantShreddingFunc(shreddingFunc)
            .createWriterFunc(msgType -> InternalWriter.create(writeSchema.asStruct(), msgType))
            .named("tracked_file")
            .build()) {
      for (int i = 0; i < lowers.size(); i += 1) {
        ContentStatsStruct stats = new ContentStatsStruct(STATS_TYPE);
        stats.setStats(
            VAR_FIELD_ID,
            new FieldStatsStruct<>(
                VAR_STATS_TYPE, lowers.get(i), uppers.get(i), false, 26L, 2L, 0L, 32));
        appender.add((StructLike) fileWithStats("s3://bucket/f" + i + ".parquet", stats));
      }
    }

    return out.toInputFile();
  }

  private static List<TrackedFile> readManifest(InputFile manifest) throws IOException {
    try (V4ManifestReader reader =
        V4ManifestReader.builder(manifest, TABLE_SCHEMA, UNPARTITIONED_SPECS, TABLE_LOCATION)
            .build()) {
      return Lists.newArrayList(reader);
    }
  }

  private static TrackedFile fileWithStats(String location, ContentStats stats) {
    return new TrackedFileStruct(
        new TrackingStruct(EntryStatus.ADDED, 42L, null, null, null, null, null, null),
        FileContent.DATA,
        4,
        location,
        FileFormat.PARQUET,
        100L,
        1024L,
        0,
        EMPTY_PARTITION_DATA,
        stats,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  private static Type analyze(List<VariantValue> boundValues) {
    VariantShreddingAnalyzer<VariantValue, Void> analyzer =
        new VariantShreddingAnalyzer<>() {
          @Override
          protected List<VariantValue> extractVariantValues(List<VariantValue> rows, int idx) {
            return rows;
          }

          @Override
          protected int resolveColumnIndex(Void engineSchema, String columnName) {
            throw new UnsupportedOperationException("not used");
          }
        };

    return analyzer.analyzeAndCreateSchema(boundValues, 0);
  }
}
