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
package org.apache.iceberg.parquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.InternalReader;
import org.apache.iceberg.data.parquet.InternalWriter;
import org.apache.iceberg.expressions.PathUtil;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantObject;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;

/**
 * Prototype: shred the variant bound column of a V4 leaf manifest exactly as a data file shreds a
 * variant column, and read a single field's bound back by its normalized JSON path. See the class
 * discussion in the accompanying design notes; scope is leaf manifests only (no cross-file
 * aggregation) and only when the manifest is written as Parquet.
 *
 * <p>These tests exercise every storage path a bound field can take, and the read-side guarantee
 * that the reconstruction never drops a value on the residual fallback:
 *
 * <ul>
 *   <li>uniform frequent path -> shredded typed column
 *   <li>widened path with a narrower-width row -> shredded, but that row lands in the field's
 *       per-field residual {@code value} (not {@code typed_value})
 *   <li>mixed-type path -> not admitted -> object-level residual {@code value}
 *   <li>infrequent path (&lt;10%) -> pruned -> object-level residual {@code value}
 *   <li>path absent in a file -> {@code null}
 *   <li>path present but null-valued -> a NULL-typed variant (distinct from absent)
 * </ul>
 *
 * <p>It also exercises the intersection rule for choosing one shred schema shared by the lower and
 * upper bounds: a path is shredded only where both bounds admit it at the same leaf type.
 */
public class TestVariantBoundsShredding {
  /** Stand-in for a Parquet manifest: one row per data file, holding that file's bound object. */
  private static final Schema MANIFEST_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "file_ordinal", Types.IntegerType.get()),
          Types.NestedField.required(2, "lower_bounds", Types.VariantType.get()));

  private static final GenericRecord ROW = GenericRecord.create(MANIFEST_SCHEMA);

  /** Dual-bound manifest: lower and upper bound columns share one intersected shred schema. */
  private static final Schema DUAL_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "file_ordinal", Types.IntegerType.get()),
          Types.NestedField.required(2, "lower_bounds", Types.VariantType.get()),
          Types.NestedField.required(3, "upper_bounds", Types.VariantType.get()));

  private static final GenericRecord DUAL_ROW = GenericRecord.create(DUAL_SCHEMA);

  private static final int FILE_COUNT = 11;

  private static final String EVENT_ID = path("event_id"); // uniform int64 -> shredded
  private static final String NAME = path("name"); // uniform string -> shredded
  private static final String COUNT = path("count"); // int64 + one int32 row -> per-field residual
  private static final String MEASUREMENT = path("measurement"); // int64|string -> object residual
  private static final String RARE = path("rare"); // present in 1/11 files -> object residual
  private static final String NULLVAL = path("nullable"); // present-but-null in one file
  private static final String SCORE = path("score"); // used only by the intersection test

  private static final int NARROW_ROW = 5; // the file whose COUNT is int32

  /** Bounds shredding is a Parquet-only physical encoding; other formats keep the blob. */
  private static boolean shouldShredBounds(FileFormat manifestFormat) {
    return manifestFormat == FileFormat.PARQUET;
  }

  @Test
  public void shreddingDecisionsMatchDataProfiles() throws IOException {
    assertThat(shouldShredBounds(FileFormat.AVRO)).isFalse();
    assertThat(shouldShredBounds(FileFormat.PARQUET)).isTrue();

    MessageType schema = fileSchema(writeShreddedBounds(sampleLowerBounds()));

    // admitted: uniform frequent paths (and a widened family) get their own typed column
    assertThat(isShreddedColumn(schema, EVENT_ID)).as("uniform int64").isTrue();
    assertThat(isShreddedColumn(schema, NAME)).as("uniform string").isTrue();
    assertThat(isShreddedColumn(schema, COUNT)).as("widened int family").isTrue();

    // not admitted: everything else stays in the residual value blob
    assertThat(isShreddedColumn(schema, MEASUREMENT)).as("mixed type").isFalse();
    assertThat(isShreddedColumn(schema, RARE)).as("infrequent").isFalse();
    assertThat(isShreddedColumn(schema, NULLVAL)).as("only-null observations").isFalse();

    // a shredded field always carries BOTH channels, so a row can fall back per-field
    GroupType countField = shreddedField(schema, COUNT);
    assertThat(countField.containsField("value")).as("per-field residual channel").isTrue();
    assertThat(countField.containsField("typed_value")).as("typed channel").isTrue();
  }

  @Test
  public void everyPathRoundTripsRegardlessOfStorage() throws IOException {
    List<Record> rows = readAll(writeShreddedBounds(sampleLowerBounds()));
    assertThat(rows).hasSize(FILE_COUNT);

    // shredded typed columns
    assertThat(lowerBoundFor(rows.get(0), EVENT_ID)).isEqualTo(0L);
    assertThat(lowerBoundFor(rows.get(10), EVENT_ID)).isEqualTo(100L);
    assertThat(lowerBoundFor(rows.get(3), NAME)).isEqualTo("v3");

    // object-level residual (mixed type): read back with the file's actual per-row type
    assertThat(lowerBoundFor(rows.get(0), MEASUREMENT)).isEqualTo(0L);
    assertThat(lowerBoundFor(rows.get(1), MEASUREMENT)).isEqualTo("m1");

    // object-level residual (infrequent) and absent paths
    assertThat(lowerBoundFor(rows.get(0), RARE)).isEqualTo(555L);
    assertThat(lowerBoundFor(rows.get(1), RARE)).isNull();

    // present-but-null is distinct from absent: the field resolves to a NULL-typed variant
    assertThat(boundVariant(rows.get(0), NULLVAL)).isNotNull();
    assertThat(boundVariant(rows.get(0), NULLVAL).type()).isEqualTo(PhysicalType.NULL);
    assertThat(boundVariant(rows.get(1), NULLVAL)).isNull();
  }

  @Test
  public void narrowerRowFallsToPerFieldResidualAndIsNotDropped() throws IOException {
    OutputFile file = writeShreddedBounds(sampleLowerBounds());
    List<Record> rows = readAll(file);

    // the int32 row is stored in COUNT's per-field residual, so the typed column omits it...
    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(file.toInputFile()))) {
      Statistics<?> typed = shreddedColumnStats(reader, COUNT);
      assertThat(typed.getNumNulls())
          .as("the narrower row is not in the typed column")
          .isEqualTo(1L);
      assertThat(((Number) typed.genericGetMin()).longValue())
          .as("typed-only min excludes the residual row")
          .isEqualTo(1000L);
    }

    // ...but full reconstruction (value + typed_value) still returns the row's true bound.
    assertThat(lowerBoundFor(rows.get(NARROW_ROW), COUNT)).isEqualTo(5); // int32, from residual
    assertThat(lowerBoundFor(rows.get(4), COUNT)).isEqualTo(1004L); // int64, from typed_value
  }

  @Test
  public void shreddedPathIsIndependentlyProjectable() throws IOException {
    // read-side pushdown basis: a shredded path is its own column chunk, readable in isolation
    OutputFile file = writeShreddedBounds(sampleLowerBounds());

    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(file.toInputFile()))) {
      Statistics<?> stats = shreddedColumnStats(reader, EVENT_ID);
      assertThat(stats).as("event_id has its own column chunk").isNotNull();
      assertThat(((Number) stats.genericGetMin()).longValue()).isEqualTo(0L);
      assertThat(((Number) stats.genericGetMax()).longValue()).isEqualTo(100L);
    }
  }

  @Test
  public void intersectionSharesOneSchemaAndDemotesTypeDivergentPaths() {
    // event_id is int64 in both bounds; score is int64 in lower but string in upper.
    List<VariantValue> lower = Lists.newArrayList();
    List<VariantValue> upper = Lists.newArrayList();
    for (int i = 0; i < 4; i += 1) {
      lower.add(
          bounds(ImmutableMap.of(EVENT_ID, Variants.of((long) i), SCORE, Variants.of((long) i)))
              .value());
      upper.add(
          bounds(ImmutableMap.of(EVENT_ID, Variants.of((long) i), SCORE, Variants.of("s" + i)))
              .value());
    }

    Type shreddedLower = analyze(lower);
    Type shreddedUpper = analyze(upper);
    Type shared = intersectShredSchemas(shreddedLower, shreddedUpper);

    GroupType object = shared.asGroupType();
    assertThat(object.containsField(EVENT_ID)).as("agreed type is kept").isTrue();
    assertThat(object.containsField(SCORE)).as("type-divergent path is demoted").isFalse();
  }

  @Test
  public void endToEndWriteReadPrune() throws IOException {
    // 6 "files"; file i covers event_id range [10*i, 10*i + 5].
    int files = 6;
    List<Variant> lowers = Lists.newArrayList();
    List<Variant> uppers = Lists.newArrayList();
    for (int i = 0; i < files; i += 1) {
      lowers.add(bounds(ImmutableMap.of(EVENT_ID, Variants.of((long) i * 10), NAME, Variants.of("a" + i))));
      uppers.add(bounds(ImmutableMap.of(EVENT_ID, Variants.of((long) i * 10 + 5), NAME, Variants.of("z" + i))));
    }

    OutputFile file = writeDualShreddedBounds(lowers, uppers);

    // event_id is shredded under BOTH bound columns, from the single shared schema
    MessageType schema = fileSchema(file);
    assertThat(isShreddedLeafUnder(schema, "lower_bounds", EVENT_ID)).isTrue();
    assertThat(isShreddedLeafUnder(schema, "upper_bounds", EVENT_ID)).isTrue();

    List<Record> rows = readAll(file, DUAL_SCHEMA);
    assertThat(rows).hasSize(files);

    // lower/upper round-trip as a matched pair
    assertThat(boundOf(rows.get(3), "lower_bounds", EVENT_ID)).isEqualTo(30L);
    assertThat(boundOf(rows.get(3), "upper_bounds", EVENT_ID)).isEqualTo(35L);

    // prune with `event_id >= 35`: keep files whose upper bound reaches the threshold
    assertThat(keepIfUpperAtLeast(rows, EVENT_ID, 35L)).containsExactly(3, 4, 5);
    // prune with `event_id <= 22`: keep files whose lower bound is within the threshold
    assertThat(keepIfLowerAtMost(rows, EVENT_ID, 22L)).containsExactly(0, 1, 2);
  }

  // --- read-path sub-variant projection (design section 10) ------------------------------------

  @Test
  public void prunedProjectionKeepsOnlyRequestedPathAndDropsResidual() throws IOException {
    MessageType full = fileSchema(writeShreddedBounds(sampleLowerBounds()));
    int lowerId = variantFieldId(full, "lower_bounds");

    // request only event_id; all requested paths are shredded -> drop the object residual value
    MessageType pruned =
        ParquetSchemaUtil.pruneVariantPaths(full, ImmutableMap.of(lowerId, ImmutableSet.of(EVENT_ID)));
    assertThat(hasLeaf(pruned, "file_ordinal")).isTrue();
    assertThat(hasLeaf(pruned, "lower_bounds", "metadata")).isTrue();
    assertThat(hasLeaf(pruned, "lower_bounds", "typed_value", EVENT_ID, "value")).isTrue();
    assertThat(hasLeaf(pruned, "lower_bounds", "typed_value", EVENT_ID, "typed_value")).isTrue();
    // the cold residual blob and the sibling column are gone from what Parquet would read
    assertThat(hasLeaf(pruned, "lower_bounds", "value")).as("object residual dropped").isFalse();
    assertThat(anyLeafContains(pruned, NAME)).as("sibling field dropped").isFalse();

    // if a requested path is residual, the object value must be kept (fallback needs it)
    MessageType keptResidual =
        ParquetSchemaUtil.pruneVariantPaths(full, ImmutableMap.of(lowerId, ImmutableSet.of(MEASUREMENT)));
    assertThat(hasLeaf(keptResidual, "lower_bounds", "value")).isTrue();
  }

  @Test
  public void prunedProjectionSkipsResidualColumnChunks() throws IOException {
    // byte-level proof: with the pruned schema as the Parquet requested schema (exactly what
    // ReadConf.reader() does via setRequestedSchema), the row group loads only the requested
    // column chunks -- the cold residual blob and unrequested siblings are never read.
    OutputFile file = writeShreddedBounds(sampleLowerBounds());

    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(file.toInputFile()))) {
      MessageType full = reader.getFileMetaData().getSchema();
      int lowerId = variantFieldId(full, "lower_bounds");
      reader.setRequestedSchema(
          ParquetSchemaUtil.pruneVariantPaths(full, ImmutableMap.of(lowerId, ImmutableSet.of(EVENT_ID))));

      PageReadStore pages = reader.readNextRowGroup();
      ColumnDescriptor eventId =
          full.getColumnDescription(new String[] {"lower_bounds", "typed_value", EVENT_ID, "typed_value"});
      ColumnDescriptor residual = full.getColumnDescription(new String[] {"lower_bounds", "value"});
      ColumnDescriptor sibling =
          full.getColumnDescription(new String[] {"lower_bounds", "typed_value", NAME, "typed_value"});

      assertThat(pages.getPageReader(eventId)).as("requested chunk loaded").isNotNull();
      assertThatThrownBy(() -> pages.getPageReader(residual))
          .as("cold residual blob not loaded")
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> pages.getPageReader(sibling))
          .as("unrequested sibling not loaded")
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  public void readerReconstructsPartialObjectFromPrunedGroup() throws IOException {
    OutputFile file = writeShreddedBounds(sampleLowerBounds());

    // build the variant reader over a group pruned to just event_id (no residual, no siblings)
    List<Record> rows = readWithPrunedVariant(file, ImmutableSet.of(EVENT_ID));

    assertThat(rows).hasSize(FILE_COUNT);
    VariantObject object = ((Variant) rows.get(0).getField("lower_bounds")).value().asObject();
    assertThat(object.get(EVENT_ID).asPrimitive().get()).as("requested path reconstructs").isEqualTo(0L);
    assertThat(object.get(NAME)).as("unrequested sibling absent").isNull();
    assertThat(object.get(MEASUREMENT)).as("residual field not read").isNull();
    assertThat(object.numFields()).as("only the requested path is materialized").isEqualTo(1);
  }

  @Test
  public void readBuilderVariantProjectionReadsOnlyRequestedPath() throws IOException {
    // full wiring through the public API: Parquet.read().withVariantProjection(...) prunes both
    // the physical projection and the reader model to just event_id.
    OutputFile file = writeShreddedBounds(sampleLowerBounds());
    int lowerId = variantFieldId(fileSchema(file), "lower_bounds");

    List<Record> rows;
    try (CloseableIterable<Record> reader =
        Parquet.read(file.toInputFile())
            .project(MANIFEST_SCHEMA)
            .withVariantProjection(ImmutableMap.of(lowerId, ImmutableSet.of(EVENT_ID)))
            .createReaderFunc(fileSchema -> InternalReader.create(MANIFEST_SCHEMA, fileSchema))
            .build()) {
      rows = Lists.newArrayList(reader);
    }

    assertThat(rows).hasSize(FILE_COUNT);
    VariantObject object = ((Variant) rows.get(0).getField("lower_bounds")).value().asObject();
    assertThat(object.get(EVENT_ID).asPrimitive().get()).isEqualTo(0L);
    assertThat(object.get(NAME)).isNull();
    assertThat(object.get(MEASUREMENT)).isNull();
    assertThat(object.numFields()).isEqualTo(1);
  }

  /** event_id/name uniform; count widened with one int32 row; measurement mixed; rare/nullable rare. */
  private static List<Variant> sampleLowerBounds() {
    List<Variant> perFile = Lists.newArrayList();
    for (int i = 0; i < FILE_COUNT; i += 1) {
      ImmutableMap.Builder<String, VariantValue> byPath = ImmutableMap.builder();
      byPath.put(EVENT_ID, Variants.of((long) i * 10));
      byPath.put(NAME, Variants.of("v" + i));
      byPath.put(COUNT, i == NARROW_ROW ? Variants.of(5) : Variants.of(1000L + i));
      byPath.put(MEASUREMENT, i % 2 == 0 ? Variants.of((long) i) : Variants.of("m" + i));
      if (i == 0) {
        byPath.put(RARE, Variants.of(555L));
        byPath.put(NULLVAL, Variants.ofNull());
      }

      perFile.add(bounds(byPath.build()));
    }

    return perFile;
  }

  private static Variant bounds(Map<String, VariantValue> byPath) {
    VariantMetadata metadata = Variants.metadata(byPath.keySet());
    ShreddedObject object = Variants.object(metadata);
    byPath.forEach(object::put);
    return Variant.of(metadata, object);
  }

  private static String path(String... fields) {
    return PathUtil.toNormalizedPath(ImmutableList.copyOf(fields));
  }

  /** Analyzes bound objects into a shredded typed_value schema (the data-file analysis, reused). */
  private static Type analyze(List<VariantValue> boundsValues) {
    VariantShreddingAnalyzer<VariantValue, Void> analyzer =
        new VariantShreddingAnalyzer<>() {
          @Override
          protected List<VariantValue> extractVariantValues(List<VariantValue> rows, int idx) {
            return rows;
          }

          @Override
          protected int resolveColumnIndex(Void engineSchema, String columnName) {
            throw new UnsupportedOperationException("prototype does not resolve by name");
          }
        };

    return analyzer.analyzeAndCreateSchema(boundsValues, 0);
  }

  private static OutputFile writeShreddedBounds(List<Variant> perFileBounds) throws IOException {
    List<VariantValue> boundsValues =
        perFileBounds.stream().map(Variant::value).collect(Collectors.toList());
    Type shredded = analyze(boundsValues);
    OutputFile outputFile = new InMemoryOutputFile();

    try (FileAppender<Record> writer =
        Parquet.write(outputFile)
            .schema(MANIFEST_SCHEMA)
            .variantShreddingFunc((fieldId, name) -> shredded)
            .createWriterFunc(
                fileSchema -> InternalWriter.create(MANIFEST_SCHEMA.asStruct(), fileSchema))
            .build()) {
      for (int i = 0; i < perFileBounds.size(); i += 1) {
        writer.add(ROW.copy("file_ordinal", i, "lower_bounds", perFileBounds.get(i)));
      }
    }

    return outputFile;
  }

  /** Writes lower and upper bound columns shredded against one intersected schema. */
  private static OutputFile writeDualShreddedBounds(List<Variant> lowers, List<Variant> uppers)
      throws IOException {
    Type shared =
        intersectShredSchemas(
            analyze(lowers.stream().map(Variant::value).collect(Collectors.toList())),
            analyze(uppers.stream().map(Variant::value).collect(Collectors.toList())));
    OutputFile outputFile = new InMemoryOutputFile();

    try (FileAppender<Record> writer =
        Parquet.write(outputFile)
            .schema(DUAL_SCHEMA)
            .variantShreddingFunc((fieldId, name) -> shared) // same schema for lower and upper
            .createWriterFunc(
                fileSchema -> InternalWriter.create(DUAL_SCHEMA.asStruct(), fileSchema))
            .build()) {
      for (int i = 0; i < lowers.size(); i += 1) {
        writer.add(
            DUAL_ROW.copy(
                "file_ordinal", i, "lower_bounds", lowers.get(i), "upper_bounds", uppers.get(i)));
      }
    }

    return outputFile;
  }

  private static List<Record> readAll(OutputFile file) throws IOException {
    return readAll(file, MANIFEST_SCHEMA);
  }

  private static List<Record> readAll(OutputFile file, Schema schema) throws IOException {
    try (CloseableIterable<Record> reader =
        Parquet.read(file.toInputFile())
            .project(schema)
            .createReaderFunc(fileSchema -> InternalReader.create(schema, fileSchema))
            .build()) {
      return Lists.newArrayList(reader);
    }
  }

  /** Keep files whose upper bound reaches the threshold (mirrors `col >= threshold` pruning). */
  private static List<Integer> keepIfUpperAtLeast(List<Record> rows, String path, long threshold) {
    List<Integer> kept = Lists.newArrayList();
    for (int i = 0; i < rows.size(); i += 1) {
      Object upper = boundOf(rows.get(i), "upper_bounds", path);
      if (upper == null || ((Number) upper).longValue() >= threshold) {
        kept.add(i);
      }
    }

    return kept;
  }

  /** Keep files whose lower bound is within the threshold (mirrors `col <= threshold` pruning). */
  private static List<Integer> keepIfLowerAtMost(List<Record> rows, String path, long threshold) {
    List<Integer> kept = Lists.newArrayList();
    for (int i = 0; i < rows.size(); i += 1) {
      Object lower = boundOf(rows.get(i), "lower_bounds", path);
      if (lower == null || ((Number) lower).longValue() <= threshold) {
        kept.add(i);
      }
    }

    return kept;
  }

  private static Object boundOf(Record row, String fieldName, String normalizedPath) {
    Variant bound = (Variant) row.getField(fieldName);
    VariantValue value = bound.value().asObject().get(normalizedPath);
    return value == null || value.type() == PhysicalType.NULL ? null : value.asPrimitive().get();
  }

  /** Mirrors {@code InclusiveStatsEvaluator.extractLowerBound}: look up a path in a file's bounds. */
  private static Object lowerBoundFor(Record row, String normalizedPath) {
    VariantValue value = boundVariant(row, normalizedPath);
    return value == null || value.type() == PhysicalType.NULL ? null : value.asPrimitive().get();
  }

  private static VariantValue boundVariant(Record row, String normalizedPath) {
    Variant lowerBounds = (Variant) row.getField("lower_bounds");
    return lowerBounds.value().asObject().get(normalizedPath);
  }

  private static MessageType fileSchema(OutputFile file) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(file.toInputFile()))) {
      return reader.getFileMetaData().getSchema();
    }
  }

  private static int variantFieldId(MessageType schema, String fieldName) {
    return schema.getType(fieldName).getId().intValue();
  }

  /** Reads with the variant reader built over a group pruned to {@code keepPaths}. */
  private static List<Record> readWithPrunedVariant(OutputFile file, Set<String> keepPaths)
      throws IOException {
    try (CloseableIterable<Record> reader =
        Parquet.read(file.toInputFile())
            .project(MANIFEST_SCHEMA)
            .createReaderFunc(
                fileSchema ->
                    InternalReader.create(
                        MANIFEST_SCHEMA,
                        ParquetSchemaUtil.pruneVariantPaths(
                            fileSchema,
                            ImmutableMap.of(variantFieldId(fileSchema, "lower_bounds"), keepPaths))))
            .build()) {
      return Lists.newArrayList(reader);
    }
  }

  private static boolean hasLeaf(MessageType schema, String... columnPath) {
    return schema.getColumns().stream()
        .anyMatch(column -> Arrays.equals(column.getPath(), columnPath));
  }

  private static boolean anyLeafContains(MessageType schema, String pathElement) {
    return schema.getColumns().stream()
        .anyMatch(column -> Arrays.asList(column.getPath()).contains(pathElement));
  }

  private static boolean isShreddedColumn(MessageType schema, String normalizedPath) {
    return schema.getColumns().stream()
        .anyMatch(column -> isShreddedLeaf(column.getPath(), normalizedPath));
  }

  /** Returns the shredded field group ".../typed_value/<normalizedPath>". */
  private static GroupType shreddedField(MessageType schema, String normalizedPath) {
    return schema
        .getType("lower_bounds")
        .asGroupType()
        .getType("typed_value")
        .asGroupType()
        .getType(normalizedPath)
        .asGroupType();
  }

  private static Statistics<?> shreddedColumnStats(ParquetFileReader reader, String normalizedPath) {
    for (BlockMetaData block : reader.getFooter().getBlocks()) {
      for (ColumnChunkMetaData column : block.getColumns()) {
        if (isShreddedLeaf(column.getPath().toArray(), normalizedPath)) {
          return column.getStatistics();
        }
      }
    }

    return null;
  }

  /** True when the path is a shredded leaf under a specific top-level bound field. */
  private static boolean isShreddedLeafUnder(
      MessageType schema, String boundField, String normalizedPath) {
    return schema.getColumns().stream()
        .anyMatch(
            column ->
                column.getPath().length > 0
                    && boundField.equals(column.getPath()[0])
                    && isShreddedLeaf(column.getPath(), normalizedPath));
  }

  /** A shredded leaf for a path is ".../<normalizedPath>/typed_value". */
  private static boolean isShreddedLeaf(String[] columnPath, String normalizedPath) {
    return columnPath.length >= 2
        && normalizedPath.equals(columnPath[columnPath.length - 2])
        && "typed_value".equals(columnPath[columnPath.length - 1]);
  }

  /**
   * Keeps a shredded field only where both bounds admit it at the same leaf type, so the lower and
   * upper bounds share one schema and every shredded path has a matched min/max column.
   */
  private static Type intersectShredSchemas(Type lower, Type upper) {
    if (lower == null || upper == null) {
      return null;
    }

    GroupType lowerObject = lower.asGroupType();
    GroupType upperObject = upper.asGroupType();
    List<Type> kept = Lists.newArrayList();
    for (Type field : lowerObject.getFields()) {
      if (upperObject.containsField(field.getName())
          && sameShreddedLeaf(field, upperObject.getType(field.getName()))) {
        kept.add(field);
      }
    }

    return kept.isEmpty() ? null : lowerObject.withNewFields(kept);
  }

  private static boolean sameShreddedLeaf(Type lowerField, Type upperField) {
    GroupType lower = lowerField.asGroupType();
    GroupType upper = upperField.asGroupType();
    return lower.containsField("typed_value")
        && upper.containsField("typed_value")
        && lower.getType("typed_value").equals(upper.getType("typed_value"));
  }
}
