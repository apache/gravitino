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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDorisUtils {
  @Test
  public void testGeneratePropertiesSql() {
    // Test when properties is null
    Map<String, String> properties = null;
    String result = DorisUtils.generatePropertiesSql(properties);
    Assertions.assertEquals("", result);

    // Test when properties is empty
    properties = Collections.emptyMap();
    result = DorisUtils.generatePropertiesSql(properties);
    Assertions.assertEquals("", result);

    // Test when properties has single entry
    properties = Collections.singletonMap("key", "value");
    result = DorisUtils.generatePropertiesSql(properties);
    Assertions.assertEquals(" PROPERTIES (\n\"key\"=\"value\"\n)", result);

    // Test when properties has multiple entries
    properties = new HashMap<>();
    properties.put("key1", "value1");
    properties.put("key2", "value2");

    String expectedStr = " PROPERTIES (\n\"key1\"=\"value1\",\n\"key2\"=\"value2\"\n)";

    result = DorisUtils.generatePropertiesSql(properties);
    Assertions.assertEquals(expectedStr, result);
  }

  @Test
  public void testExtractTablePropertiesFromSql() {
    // Test when properties is null
    String createTableSql =
        "CREATE TABLE `testTable` (\n`testColumn` STRING NOT NULL COMMENT 'test comment'\n) ENGINE=OLAP\nCOMMENT \"test comment\"";
    Map<String, String> result = DorisUtils.extractPropertiesFromSql(createTableSql);
    Assertions.assertTrue(result.isEmpty());

    // Test when properties exist
    createTableSql =
        "CREATE TABLE `testTable` (\n`testColumn` STRING NOT NULL COMMENT 'test comment'\n) ENGINE=OLAP\nCOMMENT \"test comment\"\nPROPERTIES (\n\"test_property\"=\"test_value\"\n)";
    result = DorisUtils.extractPropertiesFromSql(createTableSql);
    Assertions.assertEquals("test_value", result.get("test_property"));

    // Test when multiple properties exist
    createTableSql =
        "CREATE TABLE `testTable` (\n`testColumn` STRING NOT NULL COMMENT 'test comment'\n) ENGINE=OLAP\nCOMMENT \"test comment\"\nPROPERTIES (\n\"test_property1\"=\"test_value1\",\n\"test_property2\"=\"test_value2\"\n)";
    result = DorisUtils.extractPropertiesFromSql(createTableSql);
    Assertions.assertEquals("test_value1", result.get("test_property1"));
    Assertions.assertEquals("test_value2", result.get("test_property2"));

    // test when properties has blank
    createTableSql =
        "CREATE DATABASE `test`\nPROPERTIES (\n\"property1\" = \"value1\",\n\"comment\"= \"comment\"\n)";
    result = DorisUtils.extractPropertiesFromSql(createTableSql);
    Assertions.assertEquals("value1", result.get("property1"));
    Assertions.assertEquals("comment", result.get("comment"));
  }

  @Test
  public void testExtractPartitionInfoFromSql() {
    // test range partition
    String createTableSql =
        "CREATE TABLE `testTable` (\n`col1` date NOT NULL\n) ENGINE=OLAP\n PARTITION BY RANGE(`col1`)\n()\n DISTRIBUTED BY HASH(`col1`) BUCKETS 2";
    Optional<Transform> transform = DorisUtils.extractPartitionInfoFromSql(createTableSql);
    Assertions.assertTrue(transform.isPresent());
    Assertions.assertEquals(Transforms.range(new String[] {"col1"}), transform.get());

    // test list partition (no space between LIST and parenthesis)
    createTableSql =
        "CREATE TABLE `testTable` (\n`col1` int(11) NOT NULL\n) ENGINE=OLAP\n PARTITION BY LIST(`col1`)\n()\n DISTRIBUTED BY HASH(`col1`) BUCKETS 2";
    transform = DorisUtils.extractPartitionInfoFromSql(createTableSql);
    Assertions.assertTrue(transform.isPresent());
    Assertions.assertEquals(Transforms.list(new String[][] {{"col1"}}), transform.get());

    // test list partition with space (Doris 3.0+ format: "PARTITION BY LIST (`col1`)")
    createTableSql =
        "CREATE TABLE `testTable` (\n`col1` int(11) NOT NULL\n) ENGINE=OLAP\n PARTITION BY LIST (`col1`)\n()\n DISTRIBUTED BY HASH(`col1`) BUCKETS 2";
    transform = DorisUtils.extractPartitionInfoFromSql(createTableSql);
    Assertions.assertTrue(transform.isPresent());
    Assertions.assertEquals(Transforms.list(new String[][] {{"col1"}}), transform.get());

    // test range partition with space (Doris 3.0+ may also add space for RANGE)
    createTableSql =
        "CREATE TABLE `testTable` (\n`col1` date NOT NULL\n) ENGINE=OLAP\n PARTITION BY RANGE (`col1`)\n()\n DISTRIBUTED BY HASH(`col1`) BUCKETS 2";
    transform = DorisUtils.extractPartitionInfoFromSql(createTableSql);
    Assertions.assertTrue(transform.isPresent());
    Assertions.assertEquals(Transforms.range(new String[] {"col1"}), transform.get());

    // test multi-column list partition
    createTableSql =
        "CREATE TABLE `testTable` (\n`col1` date NOT NULL,\n`col2` int(11) NOT NULL\n) ENGINE=OLAP\n PARTITION BY LIST(`col1`, `col2`)\n()\n DISTRIBUTED BY HASH(`col1`) BUCKETS 2";
    transform = DorisUtils.extractPartitionInfoFromSql(createTableSql);
    Assertions.assertTrue(transform.isPresent());
    Assertions.assertEquals(Transforms.list(new String[][] {{"col1"}, {"col2"}}), transform.get());

    // test non-partitioned table
    createTableSql =
        "CREATE TABLE `testTable` (\n`testColumn` STRING NOT NULL COMMENT 'test comment'\n) ENGINE=OLAP\nCOMMENT \"test comment\"";
    transform = DorisUtils.extractPartitionInfoFromSql(createTableSql);
    Assertions.assertFalse(transform.isPresent());

    // test multi-column list partition WITHOUT space after comma (C3 fix)
    createTableSql =
        "CREATE TABLE `testTable` (\n`col1` int(11) NOT NULL,\n`col2` int(11) NOT NULL\n) ENGINE=OLAP\n PARTITION BY LIST(`col1`,`col2`)\n()\n DISTRIBUTED BY HASH(`col1`) BUCKETS 2";
    transform = DorisUtils.extractPartitionInfoFromSql(createTableSql);
    Assertions.assertTrue(transform.isPresent());
    Assertions.assertEquals(Transforms.list(new String[][] {{"col1"}, {"col2"}}), transform.get());

    // test list partition with backtick-quoted partition names and assignments (C4 fix)
    createTableSql =
        "CREATE TABLE `testTable` (\n`city` varchar(50) NOT NULL\n) ENGINE=OLAP\n"
            + " PARTITION BY LIST (`city`)\n"
            + "(\n"
            + " PARTITION `p1` VALUES IN (\"beijing\"),\n"
            + " PARTITION `p2` VALUES IN (\"shanghai\")\n"
            + ")\n"
            + " DISTRIBUTED BY HASH(`city`) BUCKETS 1";
    transform = DorisUtils.extractPartitionInfoFromSql(createTableSql);
    Assertions.assertTrue(transform.isPresent());
    Assertions.assertEquals("list", transform.get().name());
    Transforms.ListTransform listTransform = (Transforms.ListTransform) transform.get();
    Assertions.assertEquals(2, listTransform.assignments().length);
    Assertions.assertEquals("p1", listTransform.assignments()[0].name());
    Assertions.assertEquals("p2", listTransform.assignments()[1].name());

    // test backtick-quoted partition names with hyphens and dots (valid Doris identifiers)
    createTableSql =
        "CREATE TABLE `testTable` (\n`city` varchar(50) NOT NULL\n) ENGINE=OLAP\n"
            + " PARTITION BY LIST (`city`)\n"
            + "(\n"
            + " PARTITION `p-2024_07` VALUES IN (\"beijing\"),\n"
            + " PARTITION `p-2024.08` VALUES IN (\"shanghai\")\n"
            + ")\n"
            + " DISTRIBUTED BY HASH(`city`) BUCKETS 1";
    transform = DorisUtils.extractPartitionInfoFromSql(createTableSql);
    Assertions.assertTrue(transform.isPresent());
    Assertions.assertEquals("list", transform.get().name());
    listTransform = (Transforms.ListTransform) transform.get();
    Assertions.assertEquals(2, listTransform.assignments().length);
    Assertions.assertEquals("p-2024_07", listTransform.assignments()[0].name());
    Assertions.assertEquals("p-2024.08", listTransform.assignments()[1].name());

    // test list partition with empty assignment list — should return type without assignments
    createTableSql =
        "CREATE TABLE `testTable` (\n`col1` int(11) NOT NULL\n) ENGINE=OLAP\n"
            + " PARTITION BY LIST(`col1`)\n()\n"
            + " DISTRIBUTED BY HASH(`col1`) BUCKETS 2";
    transform = DorisUtils.extractPartitionInfoFromSql(createTableSql);
    Assertions.assertTrue(transform.isPresent());
    listTransform = (Transforms.ListTransform) transform.get();
    Assertions.assertEquals(0, listTransform.assignments().length);

    // test multi-line SQL — partition definition split across many lines
    createTableSql =
        "CREATE TABLE `testTable` (\n"
            + "  `city` varchar(50) NOT NULL\n"
            + ") ENGINE=OLAP\n"
            + "PARTITION BY LIST (`city`)\n"
            + "(\n"
            + "  PARTITION `p1` VALUES IN (\"beijing\"),\n"
            + "  PARTITION `p2` VALUES IN (\"shanghai\"),\n"
            + "  PARTITION `p3` VALUES IN (\"guangzhou\")\n"
            + ")\n"
            + "DISTRIBUTED BY HASH(`city`) BUCKETS 1";
    transform = DorisUtils.extractPartitionInfoFromSql(createTableSql);
    Assertions.assertTrue(transform.isPresent());
    listTransform = (Transforms.ListTransform) transform.get();
    Assertions.assertEquals(3, listTransform.assignments().length);
    Assertions.assertEquals("p3", listTransform.assignments()[2].name());

    // test backtick-quoted column name with hyphen in PARTITION BY clause
    createTableSql =
        "CREATE TABLE `testTable` (\n`col-1` int(11) NOT NULL\n) ENGINE=OLAP\n"
            + " PARTITION BY LIST(`col-1`)\n()\n"
            + " DISTRIBUTED BY HASH(`col-1`) BUCKETS 2";
    transform = DorisUtils.extractPartitionInfoFromSql(createTableSql);
    Assertions.assertTrue(transform.isPresent());
    Assertions.assertEquals(Transforms.list(new String[][] {{"col-1"}}), transform.get());

    // test partition name with spaces inside backticks
    createTableSql =
        "CREATE TABLE `testTable` (\n`city` varchar(50) NOT NULL\n) ENGINE=OLAP\n"
            + " PARTITION BY LIST (`city`)\n"
            + "(\n"
            + " PARTITION `p beijing` VALUES IN (\"beijing\")\n"
            + ")\n"
            + " DISTRIBUTED BY HASH(`city`) BUCKETS 1";
    transform = DorisUtils.extractPartitionInfoFromSql(createTableSql);
    Assertions.assertTrue(transform.isPresent());
    listTransform = (Transforms.ListTransform) transform.get();
    Assertions.assertEquals(1, listTransform.assignments().length);
    Assertions.assertEquals("p beijing", listTransform.assignments()[0].name());
  }

  @Test
  public void testGeneratePartitionSqlFragment() {
    // test range partition
    Partition partition = Partitions.range("p1", Literals.NULL, Literals.NULL, null);
    String partitionSqlFragment = DorisUtils.generatePartitionSqlFragment(partition);
    Assertions.assertEquals("PARTITION `p1` VALUES LESS THAN MAXVALUE", partitionSqlFragment);

    partition =
        Partitions.range(
            "p2", Literals.of("2024-07-23", Types.DateType.get()), Literals.NULL, null);
    partitionSqlFragment = DorisUtils.generatePartitionSqlFragment(partition);
    Assertions.assertEquals(
        "PARTITION `p2` VALUES LESS THAN (\"2024-07-23\")", partitionSqlFragment);

    partition =
        Partitions.range(
            "p3",
            Literals.of("2024-07-24", Types.DateType.get()),
            Literals.of("2024-07-23", Types.DateType.get()),
            null);
    partitionSqlFragment = DorisUtils.generatePartitionSqlFragment(partition);
    Assertions.assertEquals(
        "PARTITION `p3` VALUES [(\"2024-07-23\"), (\"2024-07-24\"))", partitionSqlFragment);

    partition =
        Partitions.range(
            "p4", Literals.NULL, Literals.of("2024-07-24", Types.DateType.get()), null);
    partitionSqlFragment = DorisUtils.generatePartitionSqlFragment(partition);
    Assertions.assertEquals(
        "PARTITION `p4` VALUES [(\"2024-07-24\"), (MAXVALUE))", partitionSqlFragment);

    // test list partition
    Literal[][] p5values = {{Literals.of("2024-07-24", Types.DateType.get())}};
    partition = Partitions.list("p5", p5values, Collections.emptyMap());
    partitionSqlFragment = DorisUtils.generatePartitionSqlFragment(partition);
    Assertions.assertEquals("PARTITION `p5` VALUES IN (\"2024-07-24\")", partitionSqlFragment);

    Literal[][] p6values = {{Literals.integerLiteral(1)}, {Literals.integerLiteral(2)}};
    partition = Partitions.list("p6", p6values, Collections.emptyMap());
    partitionSqlFragment = DorisUtils.generatePartitionSqlFragment(partition);
    Assertions.assertEquals("PARTITION `p6` VALUES IN (\"1\",\"2\")", partitionSqlFragment);

    Literal[][] p7values = {
      {Literals.integerLiteral(1), Literals.integerLiteral(2)},
      {Literals.integerLiteral(3), Literals.integerLiteral(4)}
    };
    partition = Partitions.list("p7", p7values, Collections.emptyMap());
    partitionSqlFragment = DorisUtils.generatePartitionSqlFragment(partition);
    Assertions.assertEquals(
        "PARTITION `p7` VALUES IN ((\"1\",\"2\"),(\"3\",\"4\"))", partitionSqlFragment);
  }

  @Test
  public void testDistributedInfoPattern() {
    String createTableSql =
        "CREATE TABLE `testTable` (\n`col1` date NOT NULL\n) ENGINE=OLAP\n PARTITION BY RANGE(`col1`)\n()\n DISTRIBUTED BY HASH(`col1`) BUCKETS 2";
    Distribution distribution = DorisUtils.extractDistributionInfoFromSql(createTableSql);
    Assertions.assertEquals(2, distribution.number());

    String createTableSqlWithAuto =
        "CREATE TABLE `testTable` (\n`col1` date NOT NULL\n) ENGINE=OLAP\n PARTITION BY RANGE(`col1`)\n()\n DISTRIBUTED BY HASH(`col1`) BUCKETS AUTO";
    Distribution distribution2 = DorisUtils.extractDistributionInfoFromSql(createTableSqlWithAuto);
    Assertions.assertEquals(-1, distribution2.number());

    String createTableSqlWithRandomAuto =
        "CREATE TABLE `testTable` (\n`col1` date NOT NULL\n) ENGINE=OLAP\n PARTITION BY RANGE(`col1`)\n()\n DISTRIBUTED BY RANDOM BUCKETS AUTO";
    Distribution distribution3 =
        DorisUtils.extractDistributionInfoFromSql(createTableSqlWithRandomAuto);
    Assertions.assertEquals(-1, distribution3.number());
  }

  @Test
  public void testExtractBucketNumFromSql() {
    String createTableSql =
        "CREATE TABLE `testTable` (\n`col1` int NOT NULL\n) ENGINE=OLAP\n DISTRIBUTED BY HASH(`col1`) BUCKETS 8";
    Distribution distribution = DorisUtils.extractDistributionInfoFromSql(createTableSql);
    Assertions.assertEquals(8, distribution.number());

    String createTableSqlWithoutBucket =
        "CREATE TABLE `testTable` (\n`col1` int NOT NULL\n) ENGINE=OLAP\n DISTRIBUTED BY HASH(`col1`)";
    Distribution distributionDefault =
        DorisUtils.extractDistributionInfoFromSql(createTableSqlWithoutBucket);
    Assertions.assertEquals(1, distributionDefault.number());
  }

  @Test
  public void testExtractBucketNumWithWhitespace() {
    String createTableSqlWithWhitespace =
        "CREATE TABLE `testTable` (\n`col1` int NOT NULL\n) ENGINE=OLAP\n DISTRIBUTED BY HASH(`col1`) BUCKETS  16 ";
    Distribution distribution =
        DorisUtils.extractDistributionInfoFromSql(createTableSqlWithWhitespace);
    Assertions.assertEquals(16, distribution.number());

    String createTableSqlWithAutoWhitespace =
        "CREATE TABLE `testTable` (\n`col1` int NOT NULL\n) ENGINE=OLAP\n DISTRIBUTED BY HASH(`col1`) BUCKETS  AUTO ";
    Distribution distributionAuto =
        DorisUtils.extractDistributionInfoFromSql(createTableSqlWithAutoWhitespace);
    Assertions.assertEquals(-1, distributionAuto.number());

    String createTableSqlWithAutoLeadingWhitespace =
        "CREATE TABLE `testTable` (\n`col1` int NOT NULL\n) ENGINE=OLAP\n DISTRIBUTED BY HASH(`col1`) BUCKETS  AUTO";
    Distribution distributionAutoLeading =
        DorisUtils.extractDistributionInfoFromSql(createTableSqlWithAutoLeadingWhitespace);
    Assertions.assertEquals(-1, distributionAutoLeading.number());
  }
}
