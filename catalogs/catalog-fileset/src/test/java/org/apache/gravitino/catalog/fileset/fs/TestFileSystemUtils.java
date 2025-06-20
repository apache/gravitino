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

package org.apache.gravitino.catalog.fileset.fs;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestFileSystemUtils {
  @ParameterizedTest
  @MethodSource("mapArguments")
  void testToHadoopConfigMap(
      Map<String, String> confMap,
      Map<String, String> toHadoopConf,
      Map<String, String> predefineKeys) {
    Map<String, String> result = FileSystemUtils.toHadoopConfigMap(confMap, predefineKeys);
    Assertions.assertEquals(toHadoopConf, result);
  }

  @Test
  void testCreateConfiguration() {
    Map<String, String> confMap =
        ImmutableMap.of(
            "s3a-endpoint", "v1",
            "fs.s3a.impl", "v2",
            "fs.s3a.endpoint", "v3",
            "gravitino.bypass.fs.s3a.endpoint", "v4");
    Configuration configuration = FileSystemUtils.createConfiguration(confMap);
    Assertions.assertEquals("v1", configuration.get("s3a-endpoint"));
    Assertions.assertEquals("v2", configuration.get("fs.s3a.impl"));
    Assertions.assertEquals("v3", configuration.get("fs.s3a.endpoint"));
    Assertions.assertEquals("v4", configuration.get("gravitino.bypass.fs.s3a.endpoint"));
  }

  private static Stream<Arguments> mapArguments() {
    return Stream.of(
        Arguments.of(
            ImmutableMap.of(
                "fs.s3a.endpoint", "v1",
                "fs.s3a.impl", "v2"),
            ImmutableMap.of("fs.s3a.endpoint", "v1", "fs.s3a.impl", "v2"),
            ImmutableMap.of()),
        Arguments.of(
            ImmutableMap.of(
                "gravitino.bypass.fs.s3a.endpoint", "v1",
                "fs.s3a.impl", "v2"),
            ImmutableMap.of("fs.s3a.endpoint", "v1", "fs.s3a.impl", "v2"),
            ImmutableMap.of()),
        Arguments.of(
            ImmutableMap.of(
                "fs.s3a.endpoint", "v1",
                "gravitino.bypass.fs.s3a.endpoint", "v2",
                "fs.s3a.impl", "v2"),
            ImmutableMap.of("fs.s3a.endpoint", "v1", "fs.s3a.impl", "v2"),
            ImmutableMap.of()),
        Arguments.of(
            ImmutableMap.of(
                "s3a-endpoint", "v1",
                "fs.s3a.impl", "v2"),
            ImmutableMap.of("fs.s3a.endpoint", "v1", "fs.s3a.impl", "v2"),
            ImmutableMap.of("s3a-endpoint", "fs.s3a.endpoint")),
        Arguments.of(
            ImmutableMap.of(
                "s3a-endpoint", "v1",
                "fs.s3a.impl", "v2",
                "gravitino.bypass.fs.s3a.endpoint", "v3"),
            ImmutableMap.of("fs.s3a.endpoint", "v1", "fs.s3a.impl", "v2"),
            ImmutableMap.of("s3a-endpoint", "fs.s3a.endpoint")),
        Arguments.of(
            ImmutableMap.of(
                "s3a-endpoint", "v1",
                "fs.s3a.impl", "v2",
                "fs.s3a.endpoint", "v3"),
            ImmutableMap.of("fs.s3a.endpoint", "v1", "fs.s3a.impl", "v2"),
            ImmutableMap.of("s3a-endpoint", "fs.s3a.endpoint")),
        Arguments.of(
            ImmutableMap.of(
                "s3a-endpoint", "v1",
                "fs.s3a.impl", "v2",
                "fs.s3a.endpoint", "v3",
                "gravitino.bypass.fs.s3a.endpoint", "v4"),
            ImmutableMap.of("fs.s3a.endpoint", "v1", "fs.s3a.impl", "v2"),
            ImmutableMap.of("s3a-endpoint", "fs.s3a.endpoint")));
  }
}
