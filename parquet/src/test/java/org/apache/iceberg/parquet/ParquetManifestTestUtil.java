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
import org.apache.iceberg.io.InputFile;
import org.apache.parquet.hadoop.ParquetFileReader;
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
}
