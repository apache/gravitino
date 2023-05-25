<<<<<<< HEAD:core/src/test/java/com/datastrato/graviton/config/TestConfigEntry.java
package com.datastrato.graviton.config;
=======
package com.datastrato.unified_catalog.config;
>>>>>>> b8675ae (Add Config system for Unified Catalog):core/src/test/java/com/datastrato/unified_catalog/config/TestConfigEntry.java

import com.google.common.collect.Lists;
import java.util.NoSuchElementException;
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
<<<<<<< HEAD:core/src/test/java/com/datastrato/graviton/config/TestConfigEntry.java
    configMap.put("graviton.test.string", "test-string");
    configMap.put("graviton.test.string.alt1", "test-string1");
    configMap.put("graviton.test.string.alt2", "test-string2");
=======
    configMap.put("unified-catalog.test.string", "test-string");
    configMap.put("unified-catalog.test.string.alt1", "test-string1");
    configMap.put("unified-catalog.test.string.alt2", "test-string2");
>>>>>>> b8675ae (Add Config system for Unified Catalog):core/src/test/java/com/datastrato/unified_catalog/config/TestConfigEntry.java
  }

  @AfterEach
  public void clearConfigMap() {
    configMap.clear();
  }

  @Test
  public void testConfWithDefaultValue() {
    ConfigEntry<String> testConf =
<<<<<<< HEAD:core/src/test/java/com/datastrato/graviton/config/TestConfigEntry.java
        new ConfigBuilder("graviton.test.string")
=======
        new ConfigBuilder("unified-catalog.test.string")
>>>>>>> b8675ae (Add Config system for Unified Catalog):core/src/test/java/com/datastrato/unified_catalog/config/TestConfigEntry.java
            .doc("test")
            .internal()
            .stringConf()
            .createWithDefault("test");
    String value = testConf.readFrom(configMap);
    Assertions.assertEquals("test-string", value);

    ConfigEntry<Integer> testConf1 =
<<<<<<< HEAD:core/src/test/java/com/datastrato/graviton/config/TestConfigEntry.java
        new ConfigBuilder("graviton.test.int")
=======
        new ConfigBuilder("unified-catalog.test.int")
>>>>>>> b8675ae (Add Config system for Unified Catalog):core/src/test/java/com/datastrato/unified_catalog/config/TestConfigEntry.java
            .doc("test")
            .version("1.0")
            .intConf()
            .createWithDefault(10);
    int value1 = testConf1.readFrom(configMap);
    Assertions.assertEquals(10, value1);

    ConfigEntry<Boolean> testConf2 =
<<<<<<< HEAD:core/src/test/java/com/datastrato/graviton/config/TestConfigEntry.java
        new ConfigBuilder("graviton.test.boolean").booleanConf().createWithDefault(true);
=======
        new ConfigBuilder("unified-catalog.test.boolean").booleanConf().createWithDefault(true);
>>>>>>> b8675ae (Add Config system for Unified Catalog):core/src/test/java/com/datastrato/unified_catalog/config/TestConfigEntry.java
    boolean value2 = testConf2.readFrom(configMap);
    Assertions.assertTrue(value2);
  }

  @Test
  public void testConfWithoutDefaultValue() {
    ConfigEntry<String> testConf =
<<<<<<< HEAD:core/src/test/java/com/datastrato/graviton/config/TestConfigEntry.java
        new ConfigBuilder("graviton.test.string").doc("test").internal().stringConf();
    String value = testConf.readFrom(configMap);
    Assertions.assertEquals("test-string", value);

    ConfigEntry<Integer> testConf1 = new ConfigBuilder("graviton.test.int.no-exist").intConf();
    Throwable exception =
        Assertions.assertThrows(NoSuchElementException.class, () -> testConf1.readFrom(configMap));
    Assertions.assertEquals(
        "No configuration found for key graviton.test.int.no-exist", exception.getMessage());
=======
        new ConfigBuilder("unified-catalog.test.string").doc("test").internal().stringConf();
    String value = testConf.readFrom(configMap);
    Assertions.assertEquals("test-string", value);

    ConfigEntry<Integer> testConf1 =
        new ConfigBuilder("unified-catalog.test.int.no-exist").intConf();
    Throwable exception =
        Assertions.assertThrows(NoSuchElementException.class, () -> testConf1.readFrom(configMap));
    Assertions.assertEquals(
        "No configuration found for key unified-catalog.test.int.no-exist", exception.getMessage());
>>>>>>> b8675ae (Add Config system for Unified Catalog):core/src/test/java/com/datastrato/unified_catalog/config/TestConfigEntry.java
  }

  @Test
  public void testConfWithOptionalValue() {
    ConfigEntry<Optional<String>> testConf =
<<<<<<< HEAD:core/src/test/java/com/datastrato/graviton/config/TestConfigEntry.java
        new ConfigBuilder("graviton.test.no-exist-string").stringConf().createWithOptional();
=======
        new ConfigBuilder("unified-catalog.test.no-exist-string").stringConf().createWithOptional();
>>>>>>> b8675ae (Add Config system for Unified Catalog):core/src/test/java/com/datastrato/unified_catalog/config/TestConfigEntry.java

    Optional<String> value = testConf.readFrom(configMap);
    Assertions.assertEquals(Optional.empty(), value);

    ConfigEntry<Optional<Integer>> testConf1 =
<<<<<<< HEAD:core/src/test/java/com/datastrato/graviton/config/TestConfigEntry.java
        new ConfigBuilder("graviton.test.no-exist-int").intConf().createWithOptional();
=======
        new ConfigBuilder("unified-catalog.test.no-exist-int").intConf().createWithOptional();
>>>>>>> b8675ae (Add Config system for Unified Catalog):core/src/test/java/com/datastrato/unified_catalog/config/TestConfigEntry.java

    Optional<Integer> value1 = testConf1.readFrom(configMap);
    Assertions.assertEquals(Optional.empty(), value1);
  }

  @Test
  public void testConfWithAlternatives() {
    ConfigEntry<String> testConf =
<<<<<<< HEAD:core/src/test/java/com/datastrato/graviton/config/TestConfigEntry.java
        new ConfigBuilder("graviton.test.string")
            .alternatives(
                Lists.newArrayList("graviton.test.string.alt1", "graviton.test.string.alt1"))
=======
        new ConfigBuilder("unified-catalog.test.string")
            .alternatives(
                Lists.newArrayList(
                    "unified-catalog.test.string.alt1", "unified-catalog.test.string.alt1"))
>>>>>>> b8675ae (Add Config system for Unified Catalog):core/src/test/java/com/datastrato/unified_catalog/config/TestConfigEntry.java
            .stringConf()
            .createWithDefault("test");

    String value = testConf.readFrom(configMap);
    Assertions.assertEquals("test-string", value);

    ConfigEntry<String> testConf1 =
<<<<<<< HEAD:core/src/test/java/com/datastrato/graviton/config/TestConfigEntry.java
        new ConfigBuilder("graviton.test.string.no-exist")
            .alternatives(
                Lists.newArrayList("graviton.test.string.alt1", "graviton.test.string.alt1"))
=======
        new ConfigBuilder("unified-catalog.test.string.no-exist")
            .alternatives(
                Lists.newArrayList(
                    "unified-catalog.test.string.alt1", "unified-catalog.test.string.alt1"))
>>>>>>> b8675ae (Add Config system for Unified Catalog):core/src/test/java/com/datastrato/unified_catalog/config/TestConfigEntry.java
            .stringConf()
            .createWithDefault("test");

    String value1 = testConf1.readFrom(configMap);
    Assertions.assertEquals("test-string1", value1);
  }

  @Test
  public void testSetConf() {
    ConfigEntry<Integer> testConf =
<<<<<<< HEAD:core/src/test/java/com/datastrato/graviton/config/TestConfigEntry.java
        new ConfigBuilder("graviton.test.int").intConf().createWithDefault(1);

    testConf.writeTo(configMap, 10);
    Assertions.assertEquals("10", configMap.get("graviton.test.int"));

    ConfigEntry<Optional<Integer>> testConf1 =
        new ConfigBuilder("graviton.test.int1").intConf().createWithOptional();

    testConf1.writeTo(configMap, Optional.of(11));
    Assertions.assertEquals("11", configMap.get("graviton.test.int1"));

    testConf1.writeTo(configMap, Optional.empty());
    Assertions.assertEquals("11", configMap.get("graviton.test.int1"));
=======
        new ConfigBuilder("unified-catalog.test.int").intConf().createWithDefault(1);

    testConf.writeTo(configMap, 10);
    Assertions.assertEquals("10", configMap.get("unified-catalog.test.int"));

    ConfigEntry<Optional<Integer>> testConf1 =
        new ConfigBuilder("unified-catalog.test.int1").intConf().createWithOptional();

    testConf1.writeTo(configMap, Optional.of(11));
    Assertions.assertEquals("11", configMap.get("unified-catalog.test.int1"));

    testConf1.writeTo(configMap, Optional.empty());
    Assertions.assertEquals("11", configMap.get("unified-catalog.test.int1"));
>>>>>>> b8675ae (Add Config system for Unified Catalog):core/src/test/java/com/datastrato/unified_catalog/config/TestConfigEntry.java
  }
}
