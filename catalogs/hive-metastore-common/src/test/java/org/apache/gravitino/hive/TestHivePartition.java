/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.hive;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.hadoop.hive.common.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link HivePartition} name escaping against HMS conventions. */
public class TestHivePartition {

  private static final List<String> SPECIAL_VALUES =
      ImmutableList.of("a/b", "x=y", "100%", "a b", "plain");

  /**
   * Names built from values must match what HMS (and the converter read path via
   * FileUtils.makePartName) produces, and identity(name) must round-trip the raw values back. On
   * the unfixed tree the plain concatenation produced names that HMS parses into different values
   * (e.g. "ds=a/b" splits into two), breaking get/drop lookups.
   */
  @Test
  public void testPartitionNamesEscapeSpecialCharacters() {
    String[][] fieldNames = {{"ds"}};
    for (String value : SPECIAL_VALUES) {
      Literal<?>[] values = {Literals.stringLiteral(value)};
      HivePartition partition = HivePartition.identity(fieldNames, values);

      String expected = FileUtils.makePartName(ImmutableList.of("ds"), ImmutableList.of(value));
      Assertions.assertEquals(expected, partition.name(), "value: " + value);

      HivePartition parsed = HivePartition.identity(partition.name());
      Assertions.assertEquals(value, parsed.values()[0].value(), "value: " + value);
      Assertions.assertEquals("ds", parsed.fieldNames()[0][0]);
    }
  }

  /**
   * The invariant behind both directions: buildPartitionName equals makePartName for the same
   * inputs, including multi-key partitions.
   */
  @Test
  public void testMultiKeyPartitionNamesMatchHmsConvention() {
    String[][] fieldNames = {{"ds"}, {"region"}};
    List<String> rawValues = SPECIAL_VALUES.subList(0, 2);
    Literal<?>[] values = rawValues.stream().map(Literals::stringLiteral).toArray(Literal[]::new);
    HivePartition partition = HivePartition.identity(fieldNames, values);

    Assertions.assertEquals(
        FileUtils.makePartName(ImmutableList.of("ds", "region"), rawValues), partition.name());

    List<String> parsed =
        IntStream.range(0, parsedValues(partition).size())
            .mapToObj(i -> parsedValues(partition).get(i))
            .collect(Collectors.toList());
    Assertions.assertEquals(rawValues, parsed);
  }

  /**
   * The two-arg extract (drop's partial-value filter) must unescape HMS-canonical names too, or a
   * drop of ds=a%2Fb filters against the escaped string and never matches the stored raw a/b.
   */
  @Test
  public void testDropPathFilterUnescapesCanonicalNames() {
    HivePartition partition =
        HivePartition.identity(
            new String[][] {{"ds"}}, new Literal[] {Literals.stringLiteral("a/b")});
    List<String> filter =
        HivePartition.extractPartitionValues(ImmutableList.of("ds"), partition.name());
    Assertions.assertEquals(ImmutableList.of("a/b"), filter);
  }

  /** The copied escape rule must match Hive's for every character. */
  @Test
  public void testEscapingMatchesHiveFileUtils() {
    for (char c = 0; c < Character.MAX_VALUE; c++) {
      String value = "a" + c + "b";
      String escaped = HivePartition.escapePathName(value);
      Assertions.assertEquals(FileUtils.escapePathName(value), escaped, "char: " + (int) c);
      Assertions.assertEquals(value, HivePartition.unescapePathName(escaped), "char: " + (int) c);
    }
  }

  @Test
  public void testDropPathFilterUnescapesFieldNames() {
    Assertions.assertEquals(
        ImmutableList.of("v"),
        HivePartition.extractPartitionValues(ImmutableList.of("p:1"), "p%3A1=v"));
  }

  private static List<String> parsedValues(HivePartition partition) {
    return HivePartition.extractPartitionValues(partition.name());
  }
}
