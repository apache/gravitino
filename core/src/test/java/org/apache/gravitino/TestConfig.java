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
package org.apache.gravitino;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Properties;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestConfig {

  static class DummyConfig extends Config {
    public DummyConfig(boolean loadSystemProperties) {
      super(loadSystemProperties);
    }

    public DummyConfig() {
      this(true);
    }
  }

  private final Properties props = new Properties();

  @BeforeEach
  public void setUp() {
    props.setProperty("test", "test");
    props.setProperty("gravitino.test.test-string", "test-string");
    props.setProperty("gravitino.test.test-int", "  1  ");
    props.setProperty("gravitino.test.test-boolean", "true");

    props.forEach((k, v) -> System.setProperty((String) k, (String) v));
  }

  @AfterEach
  public void tearDown() {
    props.forEach((k, v) -> System.clearProperty((String) k));
  }

  @Test
  public void testLoadProperties() {
    ConfigEntry<String> stringConf =
        new ConfigBuilder("test").stringConf().createWithDefault("test-default");
    ConfigEntry<Optional<Integer>> intConf =
        new ConfigBuilder("gravitino.test.test-int").intConf().createWithOptional();
    ConfigEntry<String> stringConf1 = new ConfigBuilder("test").stringConf();

    DummyConfig config = new DummyConfig(true);

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
  public void testLoadFormFile() throws Exception {
    FileOutputStream fos = null;
    try {
      File propsFile = Files.createTempFile("tmp_test", ".properties").toFile();
      fos = new FileOutputStream(propsFile);
      props.store(fos, "test");

      ConfigEntry<String> stringConf =
          new ConfigBuilder("test").stringConf().createWithDefault("test-default");
      ConfigEntry<Integer> intConf = new ConfigBuilder("gravitino.test.test-int").intConf();
      ConfigEntry<Boolean> booleanConf =
          new ConfigBuilder("gravitino.test.test-boolean").booleanConf();

      // Do not load default system properties, loading from file.
      DummyConfig config = new DummyConfig(false);
      config.loadFromProperties(config.loadPropertiesFromFile(propsFile));

      // Config "stringConf" will not load into Config, so it will return the value from the file
      String value = config.get(stringConf);
      Assertions.assertEquals("test-default", value);

      // Config "intConf" will load into Config, so it will return the value from the file
      Integer intValue = config.get(intConf);
      Assertions.assertEquals(1, intValue);

      // Config "booleanConf" will load into Config, so it will return the value from the file
      Boolean booleanValue = config.get(booleanConf);
      Assertions.assertEquals(true, booleanValue);
    } finally {
      if (fos != null) {
        fos.close();
        fos = null;
      }
    }
  }

  @Test
  public void testGetAndSet() {
    ConfigEntry<Optional<Integer>> intConf =
        new ConfigBuilder("gravitino.test.test-int").intConf().createWithOptional();
    ConfigEntry<Boolean> booleanConf =
        new ConfigBuilder("gravitino.test.test-boolean").booleanConf().createWithDefault(false);

    DummyConfig config = new DummyConfig(true);

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

  @Test
  public void testTrimmedValues() {
    Map<String, String> props = new HashMap<>();

    props.put("int.key", " 1 ");
    ConfigEntry<Integer> intEntry = new ConfigBuilder("int.key").intConf();
    Assertions.assertEquals(1, intEntry.readFrom(props));

    props.put("long.key", " 2 ");
    ConfigEntry<Long> longEntry = new ConfigBuilder("long.key").longConf();
    Assertions.assertEquals(2L, longEntry.readFrom(props));

    props.put("double.key", " 3.5 ");
    ConfigEntry<Double> doubleEntry = new ConfigBuilder("double.key").doubleConf();
    Assertions.assertEquals(3.5d, doubleEntry.readFrom(props));

    props.put("boolean.key", " true ");
    ConfigEntry<Boolean> boolEntry = new ConfigBuilder("boolean.key").booleanConf();
    Assertions.assertTrue(boolEntry.readFrom(props));
  }
}
