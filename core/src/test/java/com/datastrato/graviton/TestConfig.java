package com.datastrato.unified_catalog;

import com.datastrato.unified_catalog.config.ConfigBuilder;
import com.datastrato.unified_catalog.config.ConfigEntry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestConfig {

  private final Properties props = System.getProperties();

  @BeforeEach
  public void setUp() {
    props.setProperty("test", "test");
    props.setProperty("unified-catalog.test.test-string", "test-string");
    props.setProperty("unified-catalog.test.test-int", "  1  ");
    props.setProperty("unified-catalog.test.test-boolean", "true");
  }

  @AfterEach
  public void tearDown() {
    props.remove("test");
    props.remove("unified-catalog.test.test-string");
    props.remove("unified-catalog.test.test-int");
    props.remove("unified-catalog.test.test-boolean");
  }

  @Test
  public void testLoadProperties() {
    ConfigEntry<String> stringConf =
        new ConfigBuilder("test").stringConf().createWithDefault("test-default");
    ConfigEntry<Optional<Integer>> intConf =
        new ConfigBuilder("unified-catalog.test.test-int").intConf().createWithOptional();
    ConfigEntry<String> stringConf1 = new ConfigBuilder("test").stringConf();

    Config config = new Config(true);

    // Config "stringConf" will not load into Config, so it will return the default value
    String value = config.get(stringConf);
    Assertions.assertEquals("test-default", value);

    // Config "stringConf1" will not load into Config and it has no default value, so it will
    // throw an exception
    Throwable exception =
        Assertions.assertThrows(NoSuchElementException.class, () -> config.get(stringConf1));
    Assertions.assertEquals("No configuration found for key test", exception.getMessage());

    Optional<Integer> intValue = config.get(intConf);
    Assertions.assertEquals(Optional.of(1), intValue);
  }

  @Test
  public void testGetAndSet() {
    ConfigEntry<Optional<Integer>> intConf =
        new ConfigBuilder("unified-catalog.test.test-int").intConf().createWithOptional();
    ConfigEntry<Boolean> booleanConf =
        new ConfigBuilder("unified-catalog.test.test-boolean")
            .booleanConf()
            .createWithDefault(false);

    Config config = new Config(true);

    // Config "intConf" will load into Config, so it will return the value from the system
    // properties
    Optional<Integer> intValue = config.get(intConf);
    Assertions.assertEquals(Optional.of(1), intValue);

    // Config "booleanConf" will load into Config, so it will return the value from the system
    // properties
    Boolean booleanValue = config.get(booleanConf);
    Assertions.assertEquals(true, booleanValue);

    // Set a new value for "booleanConf"
    config.set(booleanConf, false);
    booleanValue = config.get(booleanConf);
    Assertions.assertEquals(false, booleanValue);

    // Set a new value for "intConf"
    config.set(intConf, Optional.of(2));
    intValue = config.get(intConf);
    Assertions.assertEquals(Optional.of(2), intValue);
  }
}
