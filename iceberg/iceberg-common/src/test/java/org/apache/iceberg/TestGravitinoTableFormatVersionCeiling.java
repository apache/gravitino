/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Guards Gravitino's table format version ceiling against the bundled Iceberg. It lives in
 * Iceberg's package to read {@code TableMetadata.SUPPORTED_TABLE_FORMAT_VERSION}, which is
 * package-private.
 */
public class TestGravitinoTableFormatVersionCeiling {

  private static final String REVIEW_ON_UPGRADE =
      "The bundled Iceberg's highest table format version changed. Review Gravitino's ceiling, "
          + "IcebergConstants.SUPPORTED_TABLE_FORMAT_VERSIONS, and the docs that name it before "
          + "accepting the Iceberg upgrade.";

  @Test
  void testCeilingEqualsBundledIcebergMaximum() {
    Assertions.assertEquals(
        TableMetadata.SUPPORTED_TABLE_FORMAT_VERSION,
        IcebergConstants.DEFAULT_MAX_TABLE_FORMAT_VERSION,
        REVIEW_ON_UPGRADE);
    Set<Integer> bundledRange =
        IntStream.rangeClosed(1, TableMetadata.SUPPORTED_TABLE_FORMAT_VERSION)
            .boxed()
            .collect(Collectors.toSet());
    Assertions.assertEquals(
        bundledRange, IcebergConstants.SUPPORTED_TABLE_FORMAT_VERSIONS, REVIEW_ON_UPGRADE);
  }
}
