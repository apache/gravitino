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
package org.apache.gravitino.dto.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTemplateUpdateDTO {

  @Test
  public void testShellTemplateUpdateDTO() throws JsonProcessingException {
    ShellTemplateUpdateDTO shellTemplateUpdateDTO =
        ShellTemplateUpdateDTO.builder()
            .withNewExecutable("/bin/bash")
            .withNewArguments(ImmutableList.of("-c", "echo Hello World"))
            .withNewEnvironments(ImmutableMap.of("ENV_VAR", "value"))
            .withNewCustomFields(ImmutableMap.of("customKey", "customValue"))
            .withNewScripts(ImmutableList.of("/path/to/script1.sh", "/path/to/script2.sh"))
            .build();

    String serJson = JsonUtils.objectMapper().writeValueAsString(shellTemplateUpdateDTO);
    ShellTemplateUpdateDTO deserDTO =
        JsonUtils.objectMapper().readValue(serJson, ShellTemplateUpdateDTO.class);
    Assertions.assertEquals(shellTemplateUpdateDTO, deserDTO);

    shellTemplateUpdateDTO =
        ShellTemplateUpdateDTO.builder()
            .withNewEnvironments(ImmutableMap.of("ENV_VAR", "value"))
            .withNewCustomFields(ImmutableMap.of("customKey", "customValue"))
            .withNewScripts(ImmutableList.of("/path/to/script1.sh", "/path/to/script2.sh"))
            .build();

    serJson = JsonUtils.objectMapper().writeValueAsString(shellTemplateUpdateDTO);
    deserDTO = JsonUtils.objectMapper().readValue(serJson, ShellTemplateUpdateDTO.class);
    Assertions.assertNull(deserDTO.getNewExecutable());
    Assertions.assertNull(deserDTO.getNewArguments());
    Assertions.assertEquals(shellTemplateUpdateDTO, deserDTO);

    shellTemplateUpdateDTO =
        ShellTemplateUpdateDTO.builder()
            .withNewScripts(ImmutableList.of("/path/to/script1.sh", "/path/to/script2.sh"))
            .build();

    serJson = JsonUtils.objectMapper().writeValueAsString(shellTemplateUpdateDTO);
    deserDTO = JsonUtils.objectMapper().readValue(serJson, ShellTemplateUpdateDTO.class);
    Assertions.assertNull(deserDTO.getNewEnvironments());
    Assertions.assertNull(deserDTO.getNewCustomFields());
    Assertions.assertEquals(shellTemplateUpdateDTO, deserDTO);

    shellTemplateUpdateDTO = ShellTemplateUpdateDTO.builder().build();
    serJson = JsonUtils.objectMapper().writeValueAsString(shellTemplateUpdateDTO);
    deserDTO = JsonUtils.objectMapper().readValue(serJson, ShellTemplateUpdateDTO.class);
    Assertions.assertNull(deserDTO.getNewScripts());
    Assertions.assertEquals(shellTemplateUpdateDTO, deserDTO);
  }

  @Test
  public void testSparkTemplateUpdateDTO() throws JsonProcessingException {
    SparkTemplateUpdateDTO sparkTemplateUpdateDTO =
        SparkTemplateUpdateDTO.builder()
            .withNewExecutable("spark-submit")
            .withNewArguments(ImmutableList.of("--class", "org.example.Main"))
            .withNewEnvironments(ImmutableMap.of("SPARK_ENV", "prod"))
            .withNewCustomFields(ImmutableMap.of("customKey", "customValue"))
            .withNewClassName("org.example.Main")
            .withNewJars(ImmutableList.of("/path/to/jar1.jar", "/path/to/jar2.jar"))
            .withNewFiles(ImmutableList.of("/path/to/file1.txt", "/path/to/file2.txt"))
            .withNewArchives(ImmutableList.of("/path/to/archive1.zip", "/path/to/archive2.zip"))
            .withNewConfigs(ImmutableMap.of("spark.executor.memory", "4g"))
            .build();

    String serJson = JsonUtils.objectMapper().writeValueAsString(sparkTemplateUpdateDTO);
    SparkTemplateUpdateDTO deserDTO =
        JsonUtils.objectMapper().readValue(serJson, SparkTemplateUpdateDTO.class);
    Assertions.assertEquals(sparkTemplateUpdateDTO, deserDTO);

    sparkTemplateUpdateDTO =
        SparkTemplateUpdateDTO.builder()
            .withNewEnvironments(ImmutableMap.of("SPARK_ENV", "prod"))
            .withNewCustomFields(ImmutableMap.of("customKey", "customValue"))
            .withNewClassName("org.example.Main")
            .withNewJars(ImmutableList.of("/path/to/jar1.jar", "/path/to/jar2.jar"))
            .withNewFiles(ImmutableList.of("/path/to/file1.txt", "/path/to/file2.txt"))
            .withNewArchives(ImmutableList.of("/path/to/archive1.zip", "/path/to/archive2.zip"))
            .withNewConfigs(ImmutableMap.of("spark.executor.memory", "4g"))
            .build();

    serJson = JsonUtils.objectMapper().writeValueAsString(sparkTemplateUpdateDTO);
    deserDTO = JsonUtils.objectMapper().readValue(serJson, SparkTemplateUpdateDTO.class);
    Assertions.assertNull(deserDTO.getNewExecutable());
    Assertions.assertNull(deserDTO.getNewArguments());
    Assertions.assertEquals(sparkTemplateUpdateDTO, deserDTO);

    sparkTemplateUpdateDTO =
        SparkTemplateUpdateDTO.builder()
            .withNewClassName("org.example.Main")
            .withNewJars(ImmutableList.of("/path/to/jar1.jar", "/path/to/jar2.jar"))
            .withNewFiles(ImmutableList.of("/path/to/file1.txt", "/path/to/file2.txt"))
            .withNewArchives(ImmutableList.of("/path/to/archive1.zip", "/path/to/archive2.zip"))
            .withNewConfigs(ImmutableMap.of("spark.executor.memory", "4g"))
            .build();

    serJson = JsonUtils.objectMapper().writeValueAsString(sparkTemplateUpdateDTO);
    deserDTO = JsonUtils.objectMapper().readValue(serJson, SparkTemplateUpdateDTO.class);
    Assertions.assertNull(deserDTO.getNewEnvironments());
    Assertions.assertNull(deserDTO.getNewCustomFields());
    Assertions.assertEquals(sparkTemplateUpdateDTO, deserDTO);

    sparkTemplateUpdateDTO =
        SparkTemplateUpdateDTO.builder()
            .withNewConfigs(ImmutableMap.of("spark.executor.memory", "4g"))
            .build();

    serJson = JsonUtils.objectMapper().writeValueAsString(sparkTemplateUpdateDTO);
    deserDTO = JsonUtils.objectMapper().readValue(serJson, SparkTemplateUpdateDTO.class);
    Assertions.assertNull(deserDTO.getNewClassName());
    Assertions.assertNull(deserDTO.getNewJars());
    Assertions.assertNull(deserDTO.getNewFiles());
    Assertions.assertNull(deserDTO.getNewArchives());
    Assertions.assertEquals(sparkTemplateUpdateDTO, deserDTO);

    sparkTemplateUpdateDTO = SparkTemplateUpdateDTO.builder().build();
    serJson = JsonUtils.objectMapper().writeValueAsString(sparkTemplateUpdateDTO);
    deserDTO = JsonUtils.objectMapper().readValue(serJson, SparkTemplateUpdateDTO.class);
    Assertions.assertNull(deserDTO.getNewConfigs());
    Assertions.assertEquals(sparkTemplateUpdateDTO, deserDTO);
  }

  @Test
  public void testDeserializeShellTemplateUpdate() throws JsonProcessingException {
    String json =
        "{"
            + "\"@type\":\"shell\","
            + "\"newExecutable\":\"/bin/bash\","
            + "\"newArguments\":[\"-c\",\"echo Hello World\"],"
            + "\"newEnvironments\":{\"ENV_VAR\":\"value\"},"
            + "\"newCustomFields\":{\"customKey\":\"customValue\"},"
            + "\"newScripts\":[\"/path/to/script1.sh\",\"/path/to/script2.sh\"]"
            + "}";

    TemplateUpdateDTO dto = JsonUtils.objectMapper().readValue(json, TemplateUpdateDTO.class);
    Assertions.assertInstanceOf(ShellTemplateUpdateDTO.class, dto);
    ShellTemplateUpdateDTO shellDto = (ShellTemplateUpdateDTO) dto;
    Assertions.assertEquals("/bin/bash", shellDto.getNewExecutable());
    Assertions.assertEquals(ImmutableList.of("-c", "echo Hello World"), shellDto.getNewArguments());
    Assertions.assertEquals(ImmutableMap.of("ENV_VAR", "value"), shellDto.getNewEnvironments());
    Assertions.assertEquals(
        ImmutableMap.of("customKey", "customValue"), shellDto.getNewCustomFields());
    Assertions.assertEquals(
        ImmutableList.of("/path/to/script1.sh", "/path/to/script2.sh"), shellDto.getNewScripts());

    json =
        "{"
            + "\"@type\":\"shell\","
            + "\"newEnvironments\":{\"ENV_VAR\":\"value\"},"
            + "\"newCustomFields\":{\"customKey\":\"customValue\"},"
            + "\"newScripts\":[\"/path/to/script1.sh\",\"/path/to/script2.sh\"]"
            + "}";

    dto = JsonUtils.objectMapper().readValue(json, TemplateUpdateDTO.class);
    Assertions.assertInstanceOf(ShellTemplateUpdateDTO.class, dto);
    shellDto = (ShellTemplateUpdateDTO) dto;
    Assertions.assertNull(shellDto.getNewExecutable());
    Assertions.assertNull(shellDto.getNewArguments());

    json =
        "{"
            + "\"@type\":\"shell\","
            + "\"newScripts\":[\"/path/to/script1.sh\",\"/path/to/script2.sh\"]"
            + "}";

    dto = JsonUtils.objectMapper().readValue(json, TemplateUpdateDTO.class);
    Assertions.assertInstanceOf(ShellTemplateUpdateDTO.class, dto);
    shellDto = (ShellTemplateUpdateDTO) dto;
    Assertions.assertNull(shellDto.getNewEnvironments());
    Assertions.assertNull(shellDto.getNewCustomFields());
    Assertions.assertEquals(
        ImmutableList.of("/path/to/script1.sh", "/path/to/script2.sh"), shellDto.getNewScripts());

    json = "{" + "\"@type\":\"shell\"" + "}";

    dto = JsonUtils.objectMapper().readValue(json, TemplateUpdateDTO.class);
    Assertions.assertInstanceOf(ShellTemplateUpdateDTO.class, dto);
    shellDto = (ShellTemplateUpdateDTO) dto;
    Assertions.assertNull(shellDto.getNewScripts());
  }

  @Test
  public void testDeserializeSparkTemplateUpdate() throws JsonProcessingException {
    String json =
        "{"
            + "\"@type\":\"spark\","
            + "\"newExecutable\":\"spark-submit\","
            + "\"newArguments\":[\"--class\",\"org.example.Main\"],"
            + "\"newEnvironments\":{\"SPARK_ENV\":\"prod\"},"
            + "\"newCustomFields\":{\"customKey\":\"customValue\"},"
            + "\"newClassName\":\"org.example.Main\","
            + "\"newJars\":[\"/path/to/jar1.jar\",\"/path/to/jar2.jar\"],"
            + "\"newFiles\":[\"/path/to/file1.txt\",\"/path/to/file2.txt\"],"
            + "\"newArchives\":[\"/path/to/archive1.zip\",\"/path/to/archive2.zip\"],"
            + "\"newConfigs\":{\"spark.executor.memory\":\"4g\"}"
            + "}";

    TemplateUpdateDTO dto = JsonUtils.objectMapper().readValue(json, TemplateUpdateDTO.class);
    Assertions.assertInstanceOf(SparkTemplateUpdateDTO.class, dto);
    SparkTemplateUpdateDTO sparkDto = (SparkTemplateUpdateDTO) dto;
    Assertions.assertEquals("spark-submit", sparkDto.getNewExecutable());
    Assertions.assertEquals(
        ImmutableList.of("--class", "org.example.Main"), sparkDto.getNewArguments());
    Assertions.assertEquals(ImmutableMap.of("SPARK_ENV", "prod"), sparkDto.getNewEnvironments());
    Assertions.assertEquals(
        ImmutableMap.of("customKey", "customValue"), sparkDto.getNewCustomFields());
    Assertions.assertEquals("org.example.Main", sparkDto.getNewClassName());
    Assertions.assertEquals(
        ImmutableList.of("/path/to/jar1.jar", "/path/to/jar2.jar"), sparkDto.getNewJars());
    Assertions.assertEquals(
        ImmutableList.of("/path/to/file1.txt", "/path/to/file2.txt"), sparkDto.getNewFiles());
    Assertions.assertEquals(
        ImmutableList.of("/path/to/archive1.zip", "/path/to/archive2.zip"),
        sparkDto.getNewArchives());
    Assertions.assertEquals(
        ImmutableMap.of("spark.executor.memory", "4g"), sparkDto.getNewConfigs());

    json =
        "{"
            + "\"@type\":\"spark\","
            + "\"newEnvironments\":{\"SPARK_ENV\":\"prod\"},"
            + "\"newCustomFields\":{\"customKey\":\"customValue\"},"
            + "\"newClassName\":\"org.example.Main\","
            + "\"newJars\":[\"/path/to/jar1.jar\",\"/path/to/jar2.jar\"],"
            + "\"newFiles\":[\"/path/to/file1.txt\",\"/path/to/file2.txt\"],"
            + "\"newArchives\":[\"/path/to/archive1.zip\",\"/path/to/archive2.zip\"],"
            + "\"newConfigs\":{\"spark.executor.memory\":\"4g\"}"
            + "}";

    dto = JsonUtils.objectMapper().readValue(json, TemplateUpdateDTO.class);
    Assertions.assertInstanceOf(SparkTemplateUpdateDTO.class, dto);
    sparkDto = (SparkTemplateUpdateDTO) dto;
    Assertions.assertNull(sparkDto.getNewExecutable());
    Assertions.assertNull(sparkDto.getNewArguments());
    Assertions.assertEquals(ImmutableMap.of("SPARK_ENV", "prod"), sparkDto.getNewEnvironments());
    Assertions.assertEquals(
        ImmutableMap.of("customKey", "customValue"), sparkDto.getNewCustomFields());
    Assertions.assertEquals("org.example.Main", sparkDto.getNewClassName());
    Assertions.assertEquals(
        ImmutableList.of("/path/to/jar1.jar", "/path/to/jar2.jar"), sparkDto.getNewJars());
    Assertions.assertEquals(
        ImmutableList.of("/path/to/file1.txt", "/path/to/file2.txt"), sparkDto.getNewFiles());
    Assertions.assertEquals(
        ImmutableList.of("/path/to/archive1.zip", "/path/to/archive2.zip"),
        sparkDto.getNewArchives());
    Assertions.assertEquals(
        ImmutableMap.of("spark.executor.memory", "4g"), sparkDto.getNewConfigs());

    json =
        "{"
            + "\"@type\":\"spark\","
            + "\"newClassName\":\"org.example.Main\","
            + "\"newJars\":[\"/path/to/jar1.jar\",\"/path/to/jar2.jar\"],"
            + "\"newFiles\":[\"/path/to/file1.txt\",\"/path/to/file2.txt\"],"
            + "\"newArchives\":[\"/path/to/archive1.zip\",\"/path/to/archive2.zip\"],"
            + "\"newConfigs\":{\"spark.executor.memory\":\"4g\"}"
            + "}";

    dto = JsonUtils.objectMapper().readValue(json, TemplateUpdateDTO.class);
    Assertions.assertInstanceOf(SparkTemplateUpdateDTO.class, dto);
    sparkDto = (SparkTemplateUpdateDTO) dto;
    Assertions.assertNull(sparkDto.getNewExecutable());
    Assertions.assertNull(sparkDto.getNewArguments());
    Assertions.assertNull(sparkDto.getNewEnvironments());
    Assertions.assertNull(sparkDto.getNewCustomFields());

    json = "{" + "\"@type\":\"spark\"," + "\"newConfigs\":{\"spark.executor.memory\":\"4g\"}" + "}";

    dto = JsonUtils.objectMapper().readValue(json, TemplateUpdateDTO.class);
    Assertions.assertInstanceOf(SparkTemplateUpdateDTO.class, dto);
    sparkDto = (SparkTemplateUpdateDTO) dto;
    Assertions.assertNull(sparkDto.getNewClassName());
    Assertions.assertNull(sparkDto.getNewJars());
    Assertions.assertNull(sparkDto.getNewFiles());
    Assertions.assertNull(sparkDto.getNewArchives());
    Assertions.assertEquals(
        ImmutableMap.of("spark.executor.memory", "4g"), sparkDto.getNewConfigs());

    json = "{" + "\"@type\":\"spark\"" + "}";

    dto = JsonUtils.objectMapper().readValue(json, TemplateUpdateDTO.class);
    Assertions.assertInstanceOf(SparkTemplateUpdateDTO.class, dto);
    sparkDto = (SparkTemplateUpdateDTO) dto;
    Assertions.assertNull(sparkDto.getNewConfigs());
    Assertions.assertNull(sparkDto.getNewClassName());
    Assertions.assertNull(sparkDto.getNewJars());
    Assertions.assertNull(sparkDto.getNewFiles());
    Assertions.assertNull(sparkDto.getNewArchives());
    Assertions.assertNull(sparkDto.getNewExecutable());
    Assertions.assertNull(sparkDto.getNewArguments());
    Assertions.assertNull(sparkDto.getNewEnvironments());
    Assertions.assertNull(sparkDto.getNewCustomFields());
  }
}
