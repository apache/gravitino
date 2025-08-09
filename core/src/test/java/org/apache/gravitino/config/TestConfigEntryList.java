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
package org.apache.gravitino.config;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestConfigEntryList {

  private final ConcurrentMap<String, String> configMap = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, String> configMapEmpty = new ConcurrentHashMap<>();

  @BeforeEach
  public void initializeConfigMap() {
    configMap.put("gravitino.test.string.list", "test-string-1,test-string-2,test-string-3");
  }

  @AfterEach
  public void clearConfigMap() {
    configMap.clear();
    configMapEmpty.clear();
  }

  @Test
  public void testConfWithDefaultValue() {
    ConfigEntry<List<String>> testConf =
        new ConfigBuilder("gravitino.test.string.list")
            .doc("test")
            .internal()
            .stringConf()
            .toSequence()
            .checkValue(valueList -> valueList.stream().allMatch("test-string"::equals), "error")
            .createWithDefault(Lists.newArrayList("test-string", "test-string", "test-string"));
    List<String> valueList = testConf.readFrom(configMapEmpty);
    Assertions.assertEquals(null, configMapEmpty.get("gravitino.test.string.list"));
    Assertions.assertEquals("test-string", valueList.get(0));
    Assertions.assertEquals("test-string", valueList.get(1));
    Assertions.assertEquals("test-string", valueList.get(2));

    ConfigEntry<List<Integer>> testConf1 =
        new ConfigBuilder("gravitino.test.int.list")
            .doc("test")
            .version("1.0")
            .intConf()
            .toSequence()
            .createWithDefault(Lists.newArrayList(10));
    List<Integer> valueList1 = testConf1.readFrom(configMap);
    Assertions.assertEquals(10, valueList1.get(0));

    ConfigEntry<List<Boolean>> testConf2 =
        new ConfigBuilder("gravitino.test.boolean.list")
            .booleanConf()
            .toSequence()
            .createWithDefault(Lists.newArrayList(true, false));
    List<Boolean> valueList2 = testConf2.readFrom(configMap);
    Assertions.assertTrue(valueList2.get(0));
    Assertions.assertFalse(valueList2.get(1));
  }

  @Test
  public void testConfWithoutDefaultValue() {
    ConfigEntry<List<String>> testConf =
        new ConfigBuilder("gravitino.test.string.list")
            .doc("test")
            .internal()
            .stringConf()
            .toSequence()
            .create();
    List<String> valueList = testConf.readFrom(configMap);
    Assertions.assertEquals("test-string-1", valueList.get(0));
    Assertions.assertEquals("test-string-2", valueList.get(1));
    Assertions.assertEquals("test-string-3", valueList.get(2));

    ConfigEntry<List<Integer>> testConf1 =
        new ConfigBuilder("gravitino.test.int.no-exist").intConf().toSequence();
    Throwable exception =
        Assertions.assertThrows(NoSuchElementException.class, () -> testConf1.readFrom(configMap));
    Assertions.assertEquals(
        "No configuration found for key gravitino.test.int.no-exist", exception.getMessage());
  }

  @Test
  public void testSetConf() {
    ConfigEntry<List<Integer>> testConf =
        new ConfigBuilder("gravitino.test.int.list")
            .intConf()
            .toSequence()
            .createWithDefault(Lists.newArrayList(1));

    testConf.writeTo(configMap, Lists.newArrayList(10));
    Assertions.assertEquals("10", configMap.get("gravitino.test.int.list"));

    ConfigEntry<Optional<List<Integer>>> testConf1 =
        new ConfigBuilder("gravitino.test.int1.list").intConf().toSequence().createWithOptional();

    testConf1.writeTo(configMap, Optional.of(Lists.newArrayList(11)));
    Assertions.assertEquals("11", configMap.get("gravitino.test.int1.list"));

    testConf1.writeTo(configMap, Optional.empty());
    Assertions.assertEquals("11", configMap.get("gravitino.test.int1.list"));
  }

  @Test
  public void testCheckValue() {
    ConfigEntry<List<Integer>> testConfDefault =
        new ConfigBuilder("gravitino.test.default")
            .intConf()
            .toSequence()
            .checkValue(valueList -> valueList.stream().allMatch(element -> element > 0), "error")
            .createWithDefault(Lists.newArrayList(1, 2));
    testConfDefault.writeTo(configMap, Lists.newArrayList(-10, 2));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> testConfDefault.readFrom(configMap));

    ConfigEntry<List<String>> testConfNoDefault =
        new ConfigBuilder("gravitino.test.no.default")
            .stringConf()
            .toSequence()
            .checkValue(Objects::nonNull, "error")
            .create();
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> testConfNoDefault.readFrom(configMap));
  }

  @Test
  public void testSeqToStrWithNullElement() {
    ConfigEntry<List<Integer>> testConf =
        new ConfigBuilder("gravitino.seq.null")
            .intConf()
            .toSequence()
            .createWithDefault(Lists.newArrayList());

    Assertions.assertDoesNotThrow(
        () -> testConf.writeTo(configMapEmpty, Lists.newArrayList(1, null, 2)));
    Assertions.assertEquals("1,2", configMapEmpty.get("gravitino.seq.null"));
  }

  @Test
  public void testSequenceParsing_trimsAndIgnoresEmptyElements() {
    ConfigEntry<List<String>> conf =
        new ConfigBuilder("gravitino.test.seq.trim")
            .doc("test")
            .internal()
            .stringConf()
            .toSequence()
            .create();

    conf.writeTo(configMapEmpty, Lists.newArrayList(" A", "B ", "", " C", "   ", "D", " E F "));
    List<String> valueList = conf.readFrom(configMapEmpty);
    Assertions.assertEquals(Lists.newArrayList("A", "B", "C", "D", "E F"), valueList);
  }

  @Test
  public void testBlankOnlyInput_returnsEmptyList() {
    ConfigEntry<List<String>> conf =
        new ConfigBuilder("gravitino.test.seq.blank")
            .doc("test")
            .internal()
            .stringConf()
            .toSequence()
            .create();

    conf.writeTo(configMapEmpty, Lists.newArrayList("   ", "\t", ""));
    List<String> res = conf.readFrom(configMapEmpty);
    Assertions.assertTrue(res.isEmpty());
  }
}
