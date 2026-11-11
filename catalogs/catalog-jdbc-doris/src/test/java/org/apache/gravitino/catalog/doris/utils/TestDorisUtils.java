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
package org.apache.gravitino.catalog.doris.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestDorisUtils {
  @Test
  public void testGeneratePropertiesSql() {
    // Test when properties is null
    Map<String, String> properties = null;
    String result = DorisUtils.generatePropertiesSql(properties);
    assertEquals("", result);

    // Test when properties is empty
    properties = Collections.emptyMap();
    result = DorisUtils.generatePropertiesSql(properties);
    assertEquals("", result);

    // Test when properties has single entry
    properties = Collections.singletonMap("key", "value");
    result = DorisUtils.generatePropertiesSql(properties);
    assertEquals(" PROPERTIES (\n\"key\"=\"value\"\n)", result);

    properties = Collections.singletonMap("key\"name", "owner's \"comment\" C:\\tmp; --");
    result = DorisUtils.generatePropertiesSql(properties);
    assertEquals(
        " PROPERTIES (\n\"key\"\"name\"=\"owner's \"\"comment\"\" C:\\\\tmp; --\"\n)", result);

    // Test when properties has multiple entries
    properties = new HashMap<>();
    properties.put("key1", "value1");
    properties.put("key2", "value2");

    String expectedStr = " PROPERTIES (\n\"key1\"=\"value1\",\n\"key2\"=\"value2\"\n)";

    result = DorisUtils.generatePropertiesSql(properties);
    assertEquals(expectedStr, result);
  }

  /** Verifies large escaped properties are parsed without recursive regex stack growth. */
  @Test
  public void testLongEscapedPropertiesRoundTrip() {
    Map<String, String> properties = new HashMap<>();
    properties.put("key".repeat(10000), "value\" = \"part\\".repeat(2000));
    assertEquals(
        properties,
        DorisUtils.extractPropertiesFromSql(DorisUtils.generatePropertiesSql(properties)));
  }

  /** Verifies escaped quotes cannot be mistaken for the property key/value separator. */
  @Test
  public void testPropertiesWithQuotedSeparatorsRoundTrip() {
    Map<String, String> properties = new HashMap<>();
    properties.put("k", "a\" = \"b");
    properties.put("key\" = \"part", "value\" = \"part\\");
    properties.put(" spaced key ", " spaced value ");
    properties.put("empty", "");
    properties.put("\\\"", "\\\" = \"\\");

    assertEquals(
        properties,
        DorisUtils.extractPropertiesFromSql(DorisUtils.generatePropertiesSql(properties)));
  }

  /** Verifies backslash-escaped quotes around an equals sign remain in the property value. */
  @Test
  public void testPropertiesWithBackslashEscapedSeparators() {
    String sql = "PROPERTIES (\n\"k\" = \"a\\\" = \\\"b\",\n\"end\" = \"tail\\\\\"\n)";
    Map<String, String> expected = new HashMap<>();
    expected.put("k", "a\" = \"b");
    expected.put("end", "tail\\");
    assertEquals(expected, DorisUtils.extractPropertiesFromSql(sql));
  }

  @Test
  public void testExtractTablePropertiesFromSql() {
    // Test when properties is null
    String createTableSql =
        "CREATE TABLE `testTable` (\n`testColumn` STRING NOT NULL COMMENT 'test comment'\n) ENGINE=OLAP\nCOMMENT \"test comment\"";
    Map<String, String> result = DorisUtils.extractPropertiesFromSql(createTableSql);
    assertTrue(result.isEmpty());

    // Test when properties exist
    createTableSql =
        "CREATE TABLE `testTable` (\n`testColumn` STRING NOT NULL COMMENT 'test comment'\n) ENGINE=OLAP\nCOMMENT \"test comment\"\nPROPERTIES (\n\"test_property\"=\"test_value\"\n)";
    result = DorisUtils.extractPropertiesFromSql(createTableSql);
    assertEquals("test_value", result.get("test_property"));

    // Test when multiple properties exist
    createTableSql =
        "CREATE TABLE `testTable` (\n`testColumn` STRING NOT NULL COMMENT 'test comment'\n) ENGINE=OLAP\nCOMMENT \"test comment\"\nPROPERTIES (\n\"test_property1\"=\"test_value1\",\n\"test_property2\"=\"test_value2\"\n)";
    result = DorisUtils.extractPropertiesFromSql(createTableSql);
    assertEquals("test_value1", result.get("test_property1"));
    assertEquals("test_value2", result.get("test_property2"));

    // test when properties has blank
    createTableSql =
        "CREATE DATABASE `test`\nPROPERTIES (\n\"property1\" = \"value1\",\n\"comment\"= \"comment\"\n)";
    result = DorisUtils.extractPropertiesFromSql(createTableSql);
    assertEquals("value1", result.get("property1"));
    assertEquals("comment", result.get("comment"));

    // Escaped SHOW CREATE output (doubled quotes and backslashes, as generatePropertiesSql
    // emits) round-trips back to the original key/value text.
    createTableSql =
        "CREATE DATABASE `test`\nPROPERTIES (\n"
            + "\"key\"\"name\" = \"owner's \"\"comment\"\" D:\\\\data; --\"\n)";
    result = DorisUtils.extractPropertiesFromSql(createTableSql);
    assertEquals("owner's \"comment\" D:\\data; --", result.get("key\"name"));

    // Backslash-style escaping (\" and \\) is unescaped too, and a lone backslash before an
    // ordinary character passes through unchanged.
    createTableSql =
        "CREATE DATABASE `test`\nPROPERTIES (\n"
            + "\"plain\" = \"owner\\\\s \\\"comment\\\" D:\\data; --\"\n)";
    result = DorisUtils.extractPropertiesFromSql(createTableSql);
    assertEquals("owner\\s \"comment\" D:\\data; --", result.get("plain"));
  }

  @Test
  public void testExtractPartitionInfoFromSql() {
    // test range partition
    String createTableSql =
        "CREATE TABLE `testTable` (\n`col1` date NOT NULL\n) ENGINE=OLAP\n PARTITION BY RANGE(`col1`)\n()\n DISTRIBUTED BY HASH(`col1`) BUCKETS 2";
    Optional<Transform> transform = DorisUtils.extractPartitionInfoFromSql(createTableSql);
    assertTrue(transform.isPresent());
    assertEquals(Transforms.range(new String[] {"col1"}), transform.get());

    // test list partition
    createTableSql =
        "CREATE TABLE `testTable` (\n`col1` int(11) NOT NULL\n) ENGINE=OLAP\n PARTITION BY LIST(`col1`)\n()\n DISTRIBUTED BY HASH(`col1`) BUCKETS 2";
    transform = DorisUtils.extractPartitionInfoFromSql(createTableSql);
    assertTrue(transform.isPresent());
    assertEquals(Transforms.list(new String[][] {{"col1"}}), transform.get());

    // test multi-column list partition
    createTableSql =
        "CREATE TABLE `testTable` (\n`col1` date NOT NULL,\n`col2` int(11) NOT NULL\n) ENGINE=OLAP\n PARTITION BY LIST(`col1`, `col2`)\n()\n DISTRIBUTED BY HASH(`col1`) BUCKETS 2";
    transform = DorisUtils.extractPartitionInfoFromSql(createTableSql);
    assertTrue(transform.isPresent());
    assertEquals(Transforms.list(new String[][] {{"col1"}, {"col2"}}), transform.get());

    // test range partition with space (Doris 3.0+ SHOW CREATE TABLE format)
    createTableSql =
        "CREATE TABLE `testTable` (\n`col1` date NOT NULL\n) ENGINE=OLAP\n PARTITION BY RANGE (`col1`)\n()\n DISTRIBUTED BY HASH(`col1`) BUCKETS 2";
    transform = DorisUtils.extractPartitionInfoFromSql(createTableSql);
    assertTrue(transform.isPresent());
    assertEquals(Transforms.range(new String[] {"col1"}), transform.get());

    // test list partition with space (Doris 3.0+ SHOW CREATE TABLE format)
    createTableSql =
        "CREATE TABLE `testTable` (\n`col1` int(11) NOT NULL\n) ENGINE=OLAP\n PARTITION BY LIST (`col1`)\n()\n DISTRIBUTED BY HASH(`col1`) BUCKETS 2";
    transform = DorisUtils.extractPartitionInfoFromSql(createTableSql);
    assertTrue(transform.isPresent());
    assertEquals(Transforms.list(new String[][] {{"col1"}}), transform.get());

    // test multi-column list partition with space
    createTableSql =
        "CREATE TABLE `testTable` (\n`col1` date NOT NULL,\n`col2` int(11) NOT NULL\n) ENGINE=OLAP\n PARTITION BY LIST (`col1`, `col2`)\n()\n DISTRIBUTED BY HASH(`col1`) BUCKETS 2";
    transform = DorisUtils.extractPartitionInfoFromSql(createTableSql);
    assertTrue(transform.isPresent());
    assertEquals(Transforms.list(new String[][] {{"col1"}, {"col2"}}), transform.get());

    // test non-partitioned table
    createTableSql =
        "CREATE TABLE `testTable` (\n`testColumn` STRING NOT NULL COMMENT 'test comment'\n) ENGINE=OLAP\nCOMMENT \"test comment\"";
    transform = DorisUtils.extractPartitionInfoFromSql(createTableSql);
    assertFalse(transform.isPresent());
  }

  @Test
  public void testGeneratePartitionSqlFragment() {
    // test range partition
    Partition partition = Partitions.range("p1", Literals.NULL, Literals.NULL, null);
    String partitionSqlFragment = DorisUtils.generatePartitionSqlFragment(partition);
    assertEquals("PARTITION `p1` VALUES LESS THAN MAXVALUE", partitionSqlFragment);

    partition =
        Partitions.range(
            "p2", Literals.of("2024-07-23", Types.DateType.get()), Literals.NULL, null);
    partitionSqlFragment = DorisUtils.generatePartitionSqlFragment(partition);
    assertEquals("PARTITION `p2` VALUES LESS THAN (\"2024-07-23\")", partitionSqlFragment);

    partition =
        Partitions.range(
            "p3",
            Literals.of("2024-07-24", Types.DateType.get()),
            Literals.of("2024-07-23", Types.DateType.get()),
            null);
    partitionSqlFragment = DorisUtils.generatePartitionSqlFragment(partition);
    assertEquals(
        "PARTITION `p3` VALUES [(\"2024-07-23\"), (\"2024-07-24\"))", partitionSqlFragment);

    partition =
        Partitions.range(
            "p4", Literals.NULL, Literals.of("2024-07-24", Types.DateType.get()), null);
    partitionSqlFragment = DorisUtils.generatePartitionSqlFragment(partition);
    assertEquals("PARTITION `p4` VALUES [(\"2024-07-24\"), (MAXVALUE))", partitionSqlFragment);

    // test list partition
    Literal[][] p5values = {{Literals.of("2024-07-24", Types.DateType.get())}};
    partition = Partitions.list("p5", p5values, Collections.emptyMap());
    partitionSqlFragment = DorisUtils.generatePartitionSqlFragment(partition);
    assertEquals("PARTITION `p5` VALUES IN (\"2024-07-24\")", partitionSqlFragment);

    Literal[][] p6values = {{Literals.integerLiteral(1)}, {Literals.integerLiteral(2)}};
    partition = Partitions.list("p6", p6values, Collections.emptyMap());
    partitionSqlFragment = DorisUtils.generatePartitionSqlFragment(partition);
    assertEquals("PARTITION `p6` VALUES IN (\"1\",\"2\")", partitionSqlFragment);

    Literal[][] p7values = {
      {Literals.integerLiteral(1), Literals.integerLiteral(2)},
      {Literals.integerLiteral(3), Literals.integerLiteral(4)}
    };
    partition = Partitions.list("p7", p7values, Collections.emptyMap());
    partitionSqlFragment = DorisUtils.generatePartitionSqlFragment(partition);
    assertEquals("PARTITION `p7` VALUES IN ((\"1\",\"2\"),(\"3\",\"4\"))", partitionSqlFragment);
  }

  @Test
  public void testDistributedInfoPattern() {
    String createTableSql =
        "CREATE TABLE `testTable` (\n`col1` date NOT NULL\n) ENGINE=OLAP\n PARTITION BY RANGE(`col1`)\n()\n DISTRIBUTED BY HASH(`col1`) BUCKETS 2";
    Distribution distribution = DorisUtils.extractDistributionInfoFromSql(createTableSql);
    assertEquals(distribution.number(), 2);

    String createTableSqlWithAuto =
        "CREATE TABLE `testTable` (\n`col1` date NOT NULL\n) ENGINE=OLAP\n PARTITION BY RANGE(`col1`)\n()\n DISTRIBUTED BY HASH(`col1`) BUCKETS AUTO";
    Distribution distribution2 = DorisUtils.extractDistributionInfoFromSql(createTableSqlWithAuto);
    assertEquals(distribution2.number(), -1);

    String createTableSqlWithRandomAuto =
        "CREATE TABLE `testTable` (\n`col1` date NOT NULL\n) ENGINE=OLAP\n PARTITION BY RANGE(`col1`)\n()\n DISTRIBUTED BY RANDOM BUCKETS AUTO";
    Distribution distribution3 =
        DorisUtils.extractDistributionInfoFromSql(createTableSqlWithRandomAuto);
    assertEquals(distribution3.number(), -1);
  }

  @Test
  public void testExtractBucketNumFromSql() {
    String createTableSql =
        "CREATE TABLE `testTable` (\n`col1` int NOT NULL\n) ENGINE=OLAP\n DISTRIBUTED BY HASH(`col1`) BUCKETS 8";
    Distribution distribution = DorisUtils.extractDistributionInfoFromSql(createTableSql);
    assertEquals(8, distribution.number());

    String createTableSqlWithoutBucket =
        "CREATE TABLE `testTable` (\n`col1` int NOT NULL\n) ENGINE=OLAP\n DISTRIBUTED BY HASH(`col1`)";
    Distribution distributionDefault =
        DorisUtils.extractDistributionInfoFromSql(createTableSqlWithoutBucket);
    assertEquals(1, distributionDefault.number());
  }

  @Test
  public void testExtractBucketNumWithWhitespace() {
    String createTableSqlWithWhitespace =
        "CREATE TABLE `testTable` (\n`col1` int NOT NULL\n) ENGINE=OLAP\n DISTRIBUTED BY HASH(`col1`) BUCKETS  16 ";
    Distribution distribution =
        DorisUtils.extractDistributionInfoFromSql(createTableSqlWithWhitespace);
    assertEquals(16, distribution.number());

    String createTableSqlWithAutoWhitespace =
        "CREATE TABLE `testTable` (\n`col1` int NOT NULL\n) ENGINE=OLAP\n DISTRIBUTED BY HASH(`col1`) BUCKETS  AUTO ";
    Distribution distributionAuto =
        DorisUtils.extractDistributionInfoFromSql(createTableSqlWithAutoWhitespace);
    assertEquals(-1, distributionAuto.number());

    String createTableSqlWithAutoLeadingWhitespace =
        "CREATE TABLE `testTable` (\n`col1` int NOT NULL\n) ENGINE=OLAP\n DISTRIBUTED BY HASH(`col1`) BUCKETS  AUTO";
    Distribution distributionAutoLeading =
        DorisUtils.extractDistributionInfoFromSql(createTableSqlWithAutoLeadingWhitespace);
    assertEquals(-1, distributionAutoLeading.number());
  }

  @ParameterizedTest
  @MethodSource("partitionLiteralValues")
  void testPartitionLiteralEscaping(String value, String escaped) {
    Literal<String> literal = Literals.stringLiteral(value);
    assertEquals(
        "PARTITION `p` VALUES LESS THAN (\"" + escaped + "\")",
        DorisUtils.generatePartitionSqlFragment(
            Partitions.range("p", literal, Literals.NULL, Collections.emptyMap())));
    assertEquals(
        "PARTITION `p` VALUES [(\"" + escaped + "\"), (MAXVALUE))",
        DorisUtils.generatePartitionSqlFragment(
            Partitions.range("p", Literals.NULL, literal, Collections.emptyMap())));
    assertEquals(
        "PARTITION `p` VALUES [(\"" + escaped + "\"), (\"" + escaped + "\"))",
        DorisUtils.generatePartitionSqlFragment(
            Partitions.range("p", literal, literal, Collections.emptyMap())));

    assertEquals(
        "PARTITION `p` VALUES IN (\"" + escaped + "\",\"plain\")",
        DorisUtils.generatePartitionSqlFragment(
            Partitions.list(
                "p",
                new Literal<?>[][] {{literal}, {Literals.stringLiteral("plain")}},
                Collections.emptyMap())));
    assertEquals(
        "PARTITION `p` VALUES IN ((\"" + escaped + "\",\"plain\"),(\"plain\",\"" + escaped + "\"))",
        DorisUtils.generatePartitionSqlFragment(
            Partitions.list(
                "p",
                new Literal<?>[][] {
                  {literal, Literals.stringLiteral("plain")},
                  {Literals.stringLiteral("plain"), literal}
                },
                Collections.emptyMap())));
  }

  private static Stream<Arguments> partitionLiteralValues() {
    return Stream.of(
        Arguments.of("say \"hi\"", "say \"\"hi\"\""),
        Arguments.of("C:\\temp\\", "C:\\\\temp\\\\"),
        Arguments.of("5\" pipe' ; --", "5\"\" pipe' ; --"),
        Arguments.of("slash\\\"quote", "slash\\\\\"\"quote"),
        Arguments.of("line\nnext\tvalue", "line\nnext\tvalue"));
  }
}
