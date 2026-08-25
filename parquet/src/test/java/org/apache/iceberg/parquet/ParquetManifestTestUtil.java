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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

/**
 * Test-only bridge exposing package-private {@link ParquetIO} so cross-module tests (outside the
 * {@code org.apache.iceberg.parquet} package) can inspect a written Parquet file's physical schema.
 */
public class ParquetManifestTestUtil {
  private ParquetManifestTestUtil() {}

  /** Returns the physical Parquet {@link MessageType} of a written file. */
  public static MessageType parquetSchema(InputFile file) {
    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(file))) {
      return reader.getFileMetaData().getSchema();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** True when the file schema has a leaf column at exactly {@code path}. */
  public static boolean hasLeaf(MessageType schema, String... path) {
    return schema.getColumns().stream()
        .anyMatch(column -> java.util.Arrays.equals(column.getPath(), path));
  }

  /**
   * Runs the real Parquet metrics collection on a written data file and returns the per-field
   * metrics keyed by field id. Exposes the genuine content-stats bounds a data file produces
   * (including variant lower/upper bound objects built from the file's shredded column
   * statistics), so a test can feed real stats into a manifest instead of hand-built bounds.
   */
  public static Map<Integer, FieldMetrics<?>> fieldMetrics(
      InputFile dataFile, Schema schema, MetricsConfig metricsConfig) {
    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(dataFile))) {
      ParquetMetadata footer = reader.getFooter();
      MessageType type = footer.getFileMetaData().getSchema();
      Map<Integer, FieldMetrics<?>> metricsById = Maps.newHashMap();
      for (FieldMetrics<?> metrics :
          ParquetMetrics.fieldMetrics(schema, type, metricsConfig, footer, Stream.empty())) {
        metricsById.put(metrics.id(), metrics);
      }

      return metricsById;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
