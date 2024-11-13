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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

public class PropertiesTest {

  @Test
  public void testDefaultDelimiterAndSeparator() {
    Properties properties = new Properties();
    String[] input = {"key1=value1,key2=value2,key3=value3"};

    Map<String, String> result = properties.parse(input);

    assertEquals(3, result.size());
    assertEquals("value1", result.get("key1"));
    assertEquals("value2", result.get("key2"));
    assertEquals("value3", result.get("key3"));
  }

  @Test
  public void testDefaultDelimiterAndSeparatorArray() {
    Properties properties = new Properties();
    String[] input = {"key1=value1", "key2=value2", "key3=value3"};

    Map<String, String> result = properties.parse(input);

    assertEquals(3, result.size());
    assertEquals("value1", result.get("key1"));
    assertEquals("value2", result.get("key2"));
    assertEquals("value3", result.get("key3"));
  }

  @Test
  public void testCustomDelimiterAndSeparator() {
    Properties properties = new Properties(";", ":");
    String[] input = {"key1:value1;key2:value2;key3:value3"};

    Map<String, String> result = properties.parse(input);

    assertEquals(3, result.size());
    assertEquals("value1", result.get("key1"));
    assertEquals("value2", result.get("key2"));
    assertEquals("value3", result.get("key3"));
  }

  @Test
  public void testCustomDelimiterAndSeparatorArray() {
    Properties properties = new Properties(";", ":");
    String[] input = {"key1:value1;key2:value2", "key3:value3"};

    Map<String, String> result = properties.parse(input);

    assertEquals(3, result.size());
    assertEquals("value1", result.get("key1"));
    assertEquals("value2", result.get("key2"));
    assertEquals("value3", result.get("key3"));
  }

  @Test
  public void testEmptyInput() {
    Properties properties = new Properties();
    String input[] = {""};

    Map<String, String> result = properties.parse(input);

    assertTrue(result.isEmpty(), "Result should be empty for empty input");
  }

  @Test
  public void testEmptyArray() {
    Properties properties = new Properties();
    String input[] = {};

    Map<String, String> result = properties.parse(input);

    assertTrue(result.isEmpty(), "Result should be empty for empty array");
  }

  @Test
  public void testSinglePair() {
    Properties properties = new Properties();
    String[] input = {"key1=value1"};

    Map<String, String> result = properties.parse(input);

    assertEquals(1, result.size());
    assertEquals("value1", result.get("key1"));
  }

  @Test
  public void testMalformedPair() {
    Properties properties = new Properties();
    String[] input = {"key1=value1,key2,key3=value3"};

    Map<String, String> result = properties.parse(input);

    assertEquals(2, result.size());
    assertEquals("value1", result.get("key1"));
    assertEquals("value3", result.get("key3"));
  }

  @Test
  public void testMalformedPairArray() {
    Properties properties = new Properties();
    String[] input = {"key1=value1", "key2", "key3=value3"};

    Map<String, String> result = properties.parse(input);

    assertEquals(2, result.size());
    assertEquals("value1", result.get("key1"));
    assertEquals("value3", result.get("key3"));
  }

  @Test
  public void testWhitespaceHandling() {
    Properties properties = new Properties();
    String[] input = {" key1 = value1 , key2 = value2 "};

    Map<String, String> result = properties.parse(input);

    assertEquals(2, result.size());
    assertEquals("value1", result.get("key1"));
    assertEquals("value2", result.get("key2"));
  }

  @Test
  public void testWhitespaceHandlingArray() {
    Properties properties = new Properties();
    String[] input = {" key1 = value1 ", " key2 = value2 "};

    Map<String, String> result = properties.parse(input);

    assertEquals(2, result.size());
    assertEquals("value1", result.get("key1"));
    assertEquals("value2", result.get("key2"));
  }

  @Test
  public void testDuplicateKeys() {
    Properties properties = new Properties();
    String[] input = {"key1=value1,key1=value2,key2=value3"};

    Map<String, String> result = properties.parse(input);

    assertEquals(2, result.size());
    assertEquals("value2", result.get("key1"), "Last value should overwrite previous ones");
    assertEquals("value3", result.get("key2"));
  }

  @Test
  public void testDuplicateKeysArray() {
    Properties properties = new Properties();
    String[] input = {"key1=value1", "key1=value2", "key2=value3"};

    Map<String, String> result = properties.parse(input);

    assertEquals(2, result.size());
    assertEquals("value2", result.get("key1"), "Last value should overwrite previous ones");
    assertEquals("value3", result.get("key2"));
  }
}
