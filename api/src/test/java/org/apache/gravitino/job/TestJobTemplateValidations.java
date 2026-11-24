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

package org.apache.gravitino.job;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

public class TestJobTemplateValidations {

  private static final List<String> NULL_ELEMENT = Arrays.asList("a", null, "b");

  @Test
  public void testShellJobTemplateRejectsNullElements() {
    assertRejected(
        "\"arguments\" must not contain null elements, found one at index 1",
        () -> shellTemplate().withArguments(NULL_ELEMENT).build());
    assertRejected(
        "\"scripts\" must not contain null elements, found one at index 1",
        () -> shellTemplate().withScripts(NULL_ELEMENT).build());
  }

  @Test
  public void testJobTemplateRejectsNullMapEntries() {
    assertRejected(
        "\"environments\" must not contain null values, found one for key \"Z\"",
        () -> shellTemplate().withEnvironments(nullValue("Z")).build());
    assertRejected(
        "\"customFields\" must not contain null keys",
        () -> shellTemplate().withCustomFields(nullKey()).build());
  }

  @Test
  public void testSparkJobTemplateRejectsNulls() {
    assertRejected(
        "\"jars\" must not contain null elements",
        () -> sparkTemplate().withJars(NULL_ELEMENT).build());
    assertRejected(
        "\"files\" must not contain null elements",
        () -> sparkTemplate().withFiles(NULL_ELEMENT).build());
    assertRejected(
        "\"archives\" must not contain null elements",
        () -> sparkTemplate().withArchives(NULL_ELEMENT).build());
    assertRejected(
        "\"configs\" must not contain null values, found one for key \"spark.master\"",
        () -> sparkTemplate().withConfigs(nullValue("spark.master")).build());
  }

  @Test
  public void testJobTemplateKeepsEmptyAndAbsentFields() {
    ShellJobTemplate template =
        shellTemplate()
            .withArguments(Arrays.asList("a", ""))
            .withEnvironments(Collections.singletonMap("K", ""))
            .build();
    Assertions.assertEquals(Arrays.asList("a", ""), template.arguments());
    Assertions.assertEquals(Collections.singletonMap("K", ""), template.environments());
    Assertions.assertTrue(template.customFields().isEmpty());
    Assertions.assertTrue(template.scripts().isEmpty());
  }

  @Test
  public void testShellTemplateUpdateRejectsNulls() {
    assertRejected(
        "\"newArguments\" must not contain null elements, found one at index 1",
        () ->
            JobTemplateChange.ShellTemplateUpdate.builder().withNewArguments(NULL_ELEMENT).build());
    assertRejected(
        "\"newEnvironments\" must not contain null values, found one for key \"Z\"",
        () ->
            JobTemplateChange.ShellTemplateUpdate.builder()
                .withNewEnvironments(nullValue("Z"))
                .build());
    assertRejected(
        "\"newCustomFields\" must not contain null keys",
        () ->
            JobTemplateChange.ShellTemplateUpdate.builder().withNewCustomFields(nullKey()).build());
    assertRejected(
        "\"newScripts\" must not contain null elements",
        () -> JobTemplateChange.ShellTemplateUpdate.builder().withNewScripts(NULL_ELEMENT).build());
  }

  @Test
  public void testSparkTemplateUpdateRejectsNulls() {
    assertRejected(
        "\"newJars\" must not contain null elements",
        () -> JobTemplateChange.SparkTemplateUpdate.builder().withNewJars(NULL_ELEMENT).build());
    assertRejected(
        "\"newFiles\" must not contain null elements",
        () -> JobTemplateChange.SparkTemplateUpdate.builder().withNewFiles(NULL_ELEMENT).build());
    assertRejected(
        "\"newArchives\" must not contain null elements",
        () ->
            JobTemplateChange.SparkTemplateUpdate.builder().withNewArchives(NULL_ELEMENT).build());
    assertRejected(
        "\"newConfigs\" must not contain null values",
        () ->
            JobTemplateChange.SparkTemplateUpdate.builder()
                .withNewConfigs(nullValue("spark.master"))
                .build());
  }

  @Test
  public void testTemplateUpdateKeepsAbsentFieldsAsNoChange() {
    JobTemplateChange.ShellTemplateUpdate update =
        JobTemplateChange.ShellTemplateUpdate.builder().withNewExecutable("/bin/echo").build();
    Assertions.assertNull(update.getNewArguments());
    Assertions.assertNull(update.getNewEnvironments());
    Assertions.assertNull(update.getNewScripts());
  }

  private static ShellJobTemplate.Builder shellTemplate() {
    return ShellJobTemplate.builder().withName("shell_template").withExecutable("/bin/echo");
  }

  private static SparkJobTemplate.Builder sparkTemplate() {
    return SparkJobTemplate.builder()
        .withName("spark_template")
        .withExecutable("/path/to/app.jar")
        .withClassName("org.example.Main");
  }

  private static Map<String, String> nullValue(String key) {
    Map<String, String> map = new HashMap<>();
    map.put(key, null);
    return map;
  }

  private static Map<String, String> nullKey() {
    Map<String, String> map = new HashMap<>();
    map.put(null, "v");
    return map;
  }

  private static void assertRejected(String expectedMessage, Executable executable) {
    IllegalArgumentException e =
        Assertions.assertThrows(IllegalArgumentException.class, executable);
    Assertions.assertTrue(
        e.getMessage().contains(expectedMessage),
        () -> "Expected message to contain [" + expectedMessage + "] but was: " + e.getMessage());
  }
}
