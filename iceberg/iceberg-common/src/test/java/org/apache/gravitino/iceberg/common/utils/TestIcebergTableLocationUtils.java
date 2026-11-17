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
package org.apache.gravitino.iceberg.common.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class TestIcebergTableLocationUtils {
  @ParameterizedTest
  @CsvSource({
    "s3://b/a,s3://b/a,true",
    "s3://b/a/,s3://b/a/data,true",
    "s3://b/a/data,s3://b/a,true",
    "s3://b/a,s3://b/ab,false",
    "s3://b/a,s3://other/a,false",
    "s3a://b/a,s3n://b/a/data,true",
    "file:///warehouse/a,file:/warehouse/a/data,true",
    "/warehouse/a,file:///warehouse/a,true",
    "s3://b/a%_!,s3://b/ab,false"
  })
  void testSegmentBoundariesAndAliases(String a, String b, boolean expected) {
    Assertions.assertEquals(expected, IcebergTableLocationUtils.overlaps(a, b));
  }

  @Test
  void testStandardPointerRoot() {
    Assertions.assertEquals(
        "s3://b/a",
        IcebergTableLocationUtils.tableLocation("s3://b/a/metadata/00001-x.metadata.json")
            .orElseThrow());
    Assertions.assertTrue(
        IcebergTableLocationUtils.tableLocation("s3://b/custom/file.json").isEmpty());
    Assertions.assertTrue(
        IcebergTableLocationUtils.tableLocation("s3://b/a/metadata/sub/file.json").isEmpty());
  }

  @Test
  void testSqlPatternsEscapeWildcardsAndIncludeAliases() {
    var patterns = IcebergTableLocationUtils.metadataLocationPatterns("s3://b/a_%!/data");
    Assertions.assertTrue(patterns.contains("s3://b/a!_!%!!/data/%"));
    Assertions.assertTrue(patterns.contains("s3://b/a!_!%!!/metadata/%"));
    Assertions.assertTrue(patterns.contains("s3a://b/a!_!%!!/metadata/%"));
    Assertions.assertTrue(patterns.contains("s3n://b/a!_!%!!/metadata/%"));
  }

  @Test
  void testNormalizeAbsentAndRepeatedTrailingSeparators() {
    Assertions.assertEquals("", IcebergTableLocationUtils.normalize(null));
    Assertions.assertEquals("s3://b/a", IcebergTableLocationUtils.normalize(" s3://b/a/// "));
    Assertions.assertEquals(
        "file:/warehouse/a", IcebergTableLocationUtils.normalize("file:///warehouse/a/"));
  }
}
