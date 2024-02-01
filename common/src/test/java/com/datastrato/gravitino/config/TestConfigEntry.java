/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.config;

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

public class TestConfigEntry {

  private final ConcurrentMap<String, String> configMap = new ConcurrentHashMap<>();

  @BeforeEach
  public void initializeConfigMap() {
    configMap.put("gravitino.test.string", "test-string");
    configMap.put("gravitino.test.string.alt1", "test-string1");
    configMap.put("gravitino.test.string.alt2", "test-string2");
    configMap.put("gravitino.test.stringList", "test-string1,test-string2,test-string3");
    configMap.put("gravitino.test.integerList", "1,2,3");
    configMap.put("gravitino.test.integerList.invalid", "1,xxx,3");
  }

  @AfterEach
  public void clearConfigMap() {
    configMap.clear();
  }

  @Test
  public void testStrConfValueList() {
    ConfigEntry<List<String>> testConf =
        new ConfigBuilder("gravitino.test.stringList")
            .doc("test")
            .internal()
            .stringConf()
            .toSequence()
            .checkValue(
                valueList ->
                    (!Objects.equals(valueList.get(0), "test-string0")
                        && !Objects.equals(valueList.get(1), "test-string0")
                        && !Objects.equals(valueList.get(2), "test-string0")),
                "error")
            .checkValue(
                valueList ->
                    (Objects.equals(valueList.get(0), "test-string1")
                        && Objects.equals(valueList.get(1), "test-string2")
                        && Objects.equals(valueList.get(2), "test-string3")),
                "error")
            .create();

    List<String> valueList = testConf.readFrom(configMap);
    Assertions.assertEquals("test-string1", valueList.get(0));
    Assertions.assertEquals("test-string2", valueList.get(1));
    Assertions.assertEquals("test-string3", valueList.get(2));

    valueList.set(0, "test-string4");
    valueList.set(1, "test-string5");
    valueList.set(2, "test-string6");
    testConf.writeTo(configMap, valueList);
    Assertions.assertEquals(
        "test-string4,test-string5,test-string6", configMap.get("gravitino.test.stringList"));
  }

  @Test
  public void testIntConfValueList() {
    ConfigEntry<List<Integer>> testConf =
        new ConfigBuilder("gravitino.test.integerList")
            .doc("test")
            .internal()
            .intConf()
            .toSequence()
            .checkValue(
                valueList ->
                    (!Objects.equals(valueList.get(0), 4)
                        && !Objects.equals(valueList.get(1), 5)
                        && !Objects.equals(valueList.get(2), 6)),
                "error")
            .checkValue(
                valueList ->
                    (Objects.equals(valueList.get(0), 1)
                        && Objects.equals(valueList.get(1), 2)
                        && Objects.equals(valueList.get(2), 3)),
                "error")
            .create();

    List<Integer> valueList = testConf.readFrom(configMap);
    Assertions.assertEquals(1, valueList.get(0));
    Assertions.assertEquals(2, valueList.get(1));
    Assertions.assertEquals(3, valueList.get(2));

    valueList.set(0, 4);
    valueList.set(1, 5);
    valueList.set(2, 6);
    testConf.writeTo(configMap, valueList);
    Assertions.assertEquals("4,5,6", configMap.get("gravitino.test.integerList"));
  }

  @Test
  public void testIntConfInvalidValueList() {
    ConfigEntry<List<Integer>> testConf =
        new ConfigBuilder("gravitino.test.integerList.invalid")
            .doc("test")
            .internal()
            .intConf()
            .toSequence()
            .create();
    Assertions.assertThrows(
        NumberFormatException.class,
        () -> {
          List<Integer> valueList = testConf.readFrom(configMap);
          Assertions.assertEquals(1, valueList.get(0));
          Assertions.assertEquals("xxx", valueList.get(1));
          Assertions.assertEquals(3, valueList.get(2));
        });
  }

  @Test
  public void testConfWithDefaultValue() {
    ConfigEntry<String> testConf =
        new ConfigBuilder("gravitino.test.string")
            .doc("test")
            .internal()
            .stringConf()
            .createWithDefault("test");
    String value = testConf.readFrom(configMap);
    Assertions.assertEquals("test-string", value);

    ConfigEntry<Integer> testConf1 =
        new ConfigBuilder("gravitino.test.int")
            .doc("test")
            .version("1.0")
            .intConf()
            .createWithDefault(10);
    int value1 = testConf1.readFrom(configMap);
    Assertions.assertEquals(10, value1);

    ConfigEntry<Boolean> testConf2 =
        new ConfigBuilder("gravitino.test.boolean").booleanConf().createWithDefault(true);
    boolean value2 = testConf2.readFrom(configMap);
    Assertions.assertTrue(value2);
  }

  @Test
  public void testConfWithoutDefaultValue() {
    ConfigEntry<String> testConf =
        new ConfigBuilder("gravitino.test.string").doc("test").internal().stringConf();
    String value = testConf.readFrom(configMap);
    Assertions.assertEquals("test-string", value);

    ConfigEntry<Integer> testConf1 = new ConfigBuilder("gravitino.test.int.no-exist").intConf();
    Throwable exception =
        Assertions.assertThrows(NoSuchElementException.class, () -> testConf1.readFrom(configMap));
    Assertions.assertEquals(
        "No configuration found for key gravitino.test.int.no-exist", exception.getMessage());
  }

  @Test
  public void testConfWithOptionalValue() {
    ConfigEntry<Optional<String>> testConf =
        new ConfigBuilder("gravitino.test.no-exist-string").stringConf().createWithOptional();

    Optional<String> value = testConf.readFrom(configMap);
    Assertions.assertEquals(Optional.empty(), value);

    ConfigEntry<Optional<Integer>> testConf1 =
        new ConfigBuilder("gravitino.test.no-exist-int").intConf().createWithOptional();

    Optional<Integer> value1 = testConf1.readFrom(configMap);
    Assertions.assertEquals(Optional.empty(), value1);
  }

  @Test
  public void testConfWithAlternatives() {
    ConfigEntry<String> testConf =
        new ConfigBuilder("gravitino.test.string")
            .alternatives(
                Lists.newArrayList("gravitino.test.string.alt1", "gravitino.test.string.alt1"))
            .stringConf()
            .createWithDefault("test");

    String value = testConf.readFrom(configMap);
    Assertions.assertEquals("test-string", value);

    ConfigEntry<String> testConf1 =
        new ConfigBuilder("gravitino.test.string.no-exist")
            .alternatives(
                Lists.newArrayList("gravitino.test.string.alt1", "gravitino.test.string.alt1"))
            .stringConf()
            .createWithDefault("test");

    String value1 = testConf1.readFrom(configMap);
    Assertions.assertEquals("test-string1", value1);
  }

  @Test
  public void testSetConf() {
    ConfigEntry<Integer> testConf =
        new ConfigBuilder("gravitino.test.int").intConf().createWithDefault(1);

    testConf.writeTo(configMap, 10);
    Assertions.assertEquals("10", configMap.get("gravitino.test.int"));

    ConfigEntry<Optional<Integer>> testConf1 =
        new ConfigBuilder("gravitino.test.int1").intConf().createWithOptional();

    testConf1.writeTo(configMap, Optional.of(11));
    Assertions.assertEquals("11", configMap.get("gravitino.test.int1"));

    testConf1.writeTo(configMap, Optional.empty());
    Assertions.assertEquals("11", configMap.get("gravitino.test.int1"));
  }

  @Test
  public void testCheckValue() {
    ConfigEntry<Integer> testConfDefault =
        new ConfigBuilder("gravitino.test.default")
            .intConf()
            .checkValue(value -> value > 2, "error")
            .createWithDefault(1);
    testConfDefault.writeTo(configMap, -10);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> testConfDefault.readFrom(configMap));
    ConfigEntry<String> testConfNoDefault =
        new ConfigBuilder("gravitino.test.no.default")
            .stringConf()
            .checkValue(Objects::nonNull, "error")
            .create();
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> testConfNoDefault.readFrom(configMap));
    ConfigEntry<Optional<String>> testConfOptional =
        new ConfigBuilder("gravitino.test.optional")
            .stringConf()
            .checkValue(value -> !Objects.equals(value, "test"), "error")
            .createWithOptional();
    Assertions.assertDoesNotThrow(() -> testConfOptional.readFrom(configMap));
    testConfOptional.writeTo(configMap, Optional.of("test"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> testConfOptional.readFrom(configMap));
  }
}
