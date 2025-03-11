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

package org.apache.gravitino.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestGravitinoConfig {

  private static final String TEMP_FILE_PATH =
      System.getProperty("java.io.tmpdir") + "/gravitino.properties";
  private static final String NON_EXISTENT_FILE_PATH =
      System.getProperty("java.io.tmpdir") + "/non_existent.properties";
  private static final String METALAKE_KEY = "metalake";
  private static final String METALAKE_VALUE = "metalake_demo";
  private static final String URL_KEY = "URL";
  private static final String URL_VALUE = "http://10.0.0.1:8090";
  private static final String IGNORE_KEY = "ignore";
  private static final String IGNORE_VALUE = "true";

  @BeforeEach
  public void setUp() throws IOException {
    // Set up a temporary config file
    File tempFile = new File(TEMP_FILE_PATH);
    if (tempFile.exists()) {
      tempFile.delete();
    }

    Properties props = new Properties();
    props.setProperty(METALAKE_KEY, METALAKE_VALUE);
    props.setProperty(URL_KEY, URL_VALUE);
    props.setProperty(IGNORE_KEY, IGNORE_VALUE);

    try (Writer writer = Files.newBufferedWriter(tempFile.toPath(), StandardCharsets.UTF_8)) {
      props.store(writer, "Test Config");
    }
  }

  @Test
  public void defaultPath() {
    GravitinoConfig config = new GravitinoConfig(null);
    String expectedFilePath = System.getProperty("user.home") + "/.gravitino";
    assertEquals(
        expectedFilePath,
        config.getConfigFile(),
        "Config file path should default to the home directory with '.gravitino'");
  }

  @Test
  public void fileDoesExist() {
    GravitinoConfig config = new GravitinoConfig(TEMP_FILE_PATH);
    assertTrue(config.fileExists(), "Config file should exist");
  }

  @Test
  public void fileDoesNotExist() {
    GravitinoConfig config = new GravitinoConfig(NON_EXISTENT_FILE_PATH);
    assertFalse(config.fileExists(), "Config file should not exist");
  }

  @Test
  public void validConfigFile() {
    GravitinoConfig config = new GravitinoConfig(TEMP_FILE_PATH);
    config.read();
    assertEquals(
        METALAKE_VALUE,
        config.getMetalakeName(),
        "Should read the metalake value from the config file");
    assertEquals(
        URL_VALUE, config.getGravitinoURL(), "Should read the URL value from the config file");
    assertTrue(config.getIgnore(), "Should read the ignore value from the config file");
  }

  @Test
  public void fileNotFound() {
    GravitinoConfig config = new GravitinoConfig(NON_EXISTENT_FILE_PATH);
    config.read(); // No exception should be thrown, config file is optional
    assertNull(config.getMetalakeName(), "Metalake should be null if config file is not found");
  }

  @Test
  public void configFileMissingMetalake() throws IOException {
    // Create a config file without the "metalake" key
    File tempFileWithoutMetalake =
        new File(System.getProperty("java.io.tmpdir") + "/no_metalake.properties");
    try (Writer writer =
        Files.newBufferedWriter(tempFileWithoutMetalake.toPath(), StandardCharsets.UTF_8)) {
      writer.write("# This config file has no metalake key\n");
    }

    GravitinoConfig config = new GravitinoConfig(tempFileWithoutMetalake.getAbsolutePath());
    config.read();
    assertNull(
        config.getMetalakeName(),
        "Metalake should be null if the key is missing in the config file");

    tempFileWithoutMetalake.delete();
  }

  @Test
  public void configFileMissingURL() throws IOException {
    // Create a config file without the "url" key
    File tempFileWithoutURL = new File(System.getProperty("java.io.tmpdir") + "/no_url.properties");
    try (Writer writer =
        Files.newBufferedWriter(tempFileWithoutURL.toPath(), StandardCharsets.UTF_8)) {
      writer.write("# This config file has no url key\n");
    }

    GravitinoConfig config = new GravitinoConfig(tempFileWithoutURL.getAbsolutePath());
    config.read();
    assertNull(
        config.getGravitinoURL(), "URL should be null if the key is missing in the config file");

    tempFileWithoutURL.delete();
  }

  @Test
  public void configFileMissingIgnore() throws IOException {
    // Create a config file without the "ignore" key
    File tempFileWithoutIgnore =
        new File(System.getProperty("java.io.tmpdir") + "/no_url.properties");
    try (Writer writer =
        Files.newBufferedWriter(tempFileWithoutIgnore.toPath(), StandardCharsets.UTF_8)) {
      writer.write("# This config file has no ignore key\n");
    }

    GravitinoConfig config = new GravitinoConfig(tempFileWithoutIgnore.getAbsolutePath());
    config.read();
    assertFalse(
        config.getIgnore(), "Ignore should be false if the key is missing in the config file");

    tempFileWithoutIgnore.delete();
  }

  @Test
  public void invalidConfigFile() throws IOException {
    // Create a corrupt config file
    File corruptConfigFile =
        new File(System.getProperty("java.io.tmpdir") + "/config_corrupt.properties");
    try (Writer writer =
        Files.newBufferedWriter(corruptConfigFile.toPath(), StandardCharsets.UTF_8)) {
      writer.write("corrupt content = @@%@");
    }

    GravitinoConfig config = new GravitinoConfig(corruptConfigFile.getAbsolutePath());
    config.read(); // Should not throw an exception but handle it gracefully
    assertNull(config.getMetalakeName(), "Metalake should be null if the config file is corrupt");

    corruptConfigFile.delete();
  }

  @Test
  public void withoutReadingConfig() {
    GravitinoConfig config = new GravitinoConfig(TEMP_FILE_PATH);
    assertNull(config.getMetalakeName(), "Metalake should be null before reading the config file");
    assertNull(config.getGravitinoURL(), "URL should be null before reading the config file");
    assertFalse(config.getIgnore(), "Ignore should be null before reading the config file");
  }
}
