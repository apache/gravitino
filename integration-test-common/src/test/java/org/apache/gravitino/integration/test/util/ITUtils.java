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
package org.apache.gravitino.integration.test.util;

import static org.apache.gravitino.dto.util.DTOConverters.toDTO;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.expressions.LiteralDTO;
import org.apache.gravitino.dto.rel.partitions.IdentityPartitionDTO;
import org.apache.gravitino.dto.rel.partitions.ListPartitionDTO;
import org.apache.gravitino.dto.rel.partitions.PartitionDTO;
import org.apache.gravitino.dto.rel.partitions.RangePartitionDTO;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.partitions.Partition;
import org.junit.jupiter.api.Assertions;

public class ITUtils {

  public static final String TEST_MODE = "testMode";
  public static final String EMBEDDED_TEST_MODE = "embedded";
  public static final String DEPLOY_TEST_MODE = "deploy";

  public static String joinPath(String... dirs) {
    return String.join(File.separator, dirs);
  }

  public static String[] splitPath(String path) {
    return path.split(File.separator);
  }

  public static void rewriteConfigFile(
      String configTempFileName, String configFileName, Map<String, String> configMap)
      throws IOException {
    Properties props = new Properties();
    try (InputStream inputStream = Files.newInputStream(Paths.get(configTempFileName));
        OutputStream outputStream = Files.newOutputStream(Paths.get(configFileName))) {
      props.load(inputStream);
      props.putAll(configMap);
      for (String key : props.stringPropertyNames()) {
        String value = props.getProperty(key);
        // Use customized write functions to avoid escaping `:` into `\:`.
        outputStream.write((key + " = " + value + "\n").getBytes(StandardCharsets.UTF_8));
      }
    }
  }

  public static void overwriteConfigFile(String configFileName, Properties props)
      throws IOException {
    try (OutputStream outputStream = Files.newOutputStream(Paths.get(configFileName))) {
      for (String key : props.stringPropertyNames()) {
        String value = props.getProperty(key);
        // Use customized write functions to avoid escaping `:` into `\:`.
        outputStream.write((key + " = " + value + "\n").getBytes(StandardCharsets.UTF_8));
      }
    }
  }

  public static void assertionsTableInfo(
      String tableName,
      String tableComment,
      List<Column> columns,
      Map<String, String> properties,
      Index[] indexes,
      Transform[] partitioning,
      Table table) {
    Assertions.assertEquals(tableName, table.name());
    Assertions.assertEquals(tableComment, table.comment());
    Assertions.assertEquals(columns.size(), table.columns().length);
    for (int i = 0; i < columns.size(); i++) {
      assertColumn(columns.get(i), table.columns()[i]);
    }
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      Assertions.assertEquals(entry.getValue(), table.properties().get(entry.getKey()));
    }
    if (ArrayUtils.isNotEmpty(indexes)) {
      Assertions.assertEquals(indexes.length, table.index().length);

      Map<String, Index> indexByName =
          Arrays.stream(indexes).collect(Collectors.toMap(Index::name, index -> index));

      for (int i = 0; i < table.index().length; i++) {
        Assertions.assertTrue(indexByName.containsKey(table.index()[i].name()));
        Assertions.assertEquals(
            indexByName.get(table.index()[i].name()).type(), table.index()[i].type());
        for (int j = 0; j < table.index()[i].fieldNames().length; j++) {
          for (int k = 0; k < table.index()[i].fieldNames()[j].length; k++) {
            Assertions.assertEquals(
                indexByName.get(table.index()[i].name()).fieldNames()[j][k],
                table.index()[i].fieldNames()[j][k]);
          }
        }
      }
    }
    Assertions.assertTrue(Arrays.deepEquals(table.partitioning(), partitioning));
  }

  public static void assertColumn(Column expected, Column actual) {
    if (!(actual instanceof ColumnDTO)) {
      actual = toDTO(actual);
    }
    if (!(expected instanceof ColumnDTO)) {
      expected = toDTO(expected);
    }

    Assertions.assertEquals(expected.name(), actual.name());
    Assertions.assertEquals(expected.dataType(), actual.dataType());
    Assertions.assertEquals(expected.nullable(), actual.nullable());
    Assertions.assertEquals(expected.comment(), actual.comment());
    Assertions.assertEquals(expected.autoIncrement(), actual.autoIncrement());
    if (expected.defaultValue().equals(Column.DEFAULT_VALUE_NOT_SET) && expected.nullable()) {
      Assertions.assertEquals(LiteralDTO.NULL, actual.defaultValue());
    } else {
      Assertions.assertEquals(expected.defaultValue(), actual.defaultValue());
    }
  }

  public static void assertPartition(Partition expected, Partition actual) {
    if (!(expected instanceof PartitionDTO)) {
      expected = toDTO(expected);
    }
    if (!(actual instanceof PartitionDTO)) {
      actual = toDTO(actual);
    }

    Assertions.assertEquals(expected.name(), actual.name());
    Assertions.assertEquals(((PartitionDTO) expected).type(), ((PartitionDTO) actual).type());
    if (expected instanceof RangePartitionDTO) {
      Assertions.assertEquals(
          ((RangePartitionDTO) expected).upper(), ((RangePartitionDTO) actual).upper());
      Assertions.assertEquals(
          ((RangePartitionDTO) expected).lower(), ((RangePartitionDTO) actual).lower());
    } else if (expected instanceof ListPartitionDTO) {
      Assertions.assertTrue(
          Arrays.deepEquals(
              ((ListPartitionDTO) expected).lists(), ((ListPartitionDTO) actual).lists()));
    } else if (expected instanceof IdentityPartitionDTO) {
      Assertions.assertTrue(
          Arrays.deepEquals(
              ((IdentityPartitionDTO) expected).fieldNames(),
              ((IdentityPartitionDTO) actual).fieldNames()));
      Assertions.assertTrue(
          Arrays.equals(
              ((IdentityPartitionDTO) expected).values(),
              ((IdentityPartitionDTO) actual).values()));
    }
  }

  public static boolean isEmbedded() {
    String mode =
        System.getProperty(TEST_MODE) == null
            ? EMBEDDED_TEST_MODE
            : System.getProperty(ITUtils.TEST_MODE);

    return Objects.equals(mode, ITUtils.EMBEDDED_TEST_MODE);
  }

  public static String icebergVersion() {
    return System.getProperty("ICEBERG_VERSION");
  }

  public static String getBundleJarSourceFile(String bundleName) {
    String jarName = ITUtils.getBundleJarName(bundleName);
    String gcsJars = ITUtils.joinPath(ITUtils.getBundleJarDirectory(bundleName), jarName);
    return "file://" + gcsJars;
  }

  public static String getBundleJarName(String bundleName) {
    return String.format("gravitino-%s-%s.jar", bundleName, System.getenv("PROJECT_VERSION"));
  }

  public static String getBundleJarDirectory(String bundleName) {
    return ITUtils.joinPath(
        System.getenv("GRAVITINO_ROOT_DIR"), "bundles", bundleName, "build", "libs");
  }

  private ITUtils() {}
}
