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
package org.apache.gravitino.dto.function;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Map;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.rel.expressions.LiteralDTO;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFunctionDTO {

  @Test
  public void testFunctionDTOSerDe() throws JsonProcessingException {
    AuditDTO audit = AuditDTO.builder().withCreator("user1").withCreateTime(Instant.now()).build();

    // Create function parameter with default value
    FunctionParamDTO param1 =
        FunctionParamDTO.builder()
            .withName("x")
            .withDataType(Types.IntegerType.get())
            .withComment("input parameter")
            .build();

    FunctionParamDTO param2 =
        FunctionParamDTO.builder()
            .withName("y")
            .withDataType(Types.FloatType.get())
            .withDefaultValue(
                LiteralDTO.builder().withDataType(Types.FloatType.get()).withValue("1.0").build())
            .build();

    // Create SQL implementation
    SQLImplDTO sqlImpl =
        new SQLImplDTO(
            FunctionImpl.RuntimeType.SPARK.name(),
            null,
            ImmutableMap.of("key", "value"),
            "SELECT x + y");

    // Create function definition
    FunctionDefinitionDTO definition =
        FunctionDefinitionDTO.builder()
            .withParameters(new FunctionParamDTO[] {param1, param2})
            .withImpls(new FunctionImplDTO[] {sqlImpl})
            .build();

    // Create scalar function DTO
    FunctionDTO scalarFunction =
        FunctionDTO.builder()
            .withName("add_func")
            .withFunctionType(FunctionType.SCALAR)
            .withDeterministic(true)
            .withComment("A simple add function")
            .withReturnType(Types.FloatType.get())
            .withDefinitions(new FunctionDefinitionDTO[] {definition})
            .withAudit(audit)
            .build();

    // Serialize and deserialize
    String json = JsonUtils.objectMapper().writeValueAsString(scalarFunction);
    FunctionDTO deserialized = JsonUtils.objectMapper().readValue(json, FunctionDTO.class);

    Assertions.assertEquals(scalarFunction.name(), deserialized.name());
    Assertions.assertEquals(scalarFunction.functionType(), deserialized.functionType());
    Assertions.assertEquals(scalarFunction.deterministic(), deserialized.deterministic());
    Assertions.assertEquals(scalarFunction.comment(), deserialized.comment());
    Assertions.assertEquals(scalarFunction.returnType(), deserialized.returnType());
    Assertions.assertEquals(scalarFunction.definitions().length, deserialized.definitions().length);
  }

  @Test
  public void testTableFunctionDTOSerDe() throws JsonProcessingException {
    AuditDTO audit = AuditDTO.builder().withCreator("user1").withCreateTime(Instant.now()).build();

    // Create return columns for table function
    FunctionColumnDTO col1 =
        FunctionColumnDTO.builder()
            .withName("id")
            .withDataType(Types.IntegerType.get())
            .withComment("id column")
            .build();

    FunctionColumnDTO col2 =
        FunctionColumnDTO.builder().withName("name").withDataType(Types.StringType.get()).build();

    // Create Java implementation
    JavaImplDTO javaImpl =
        new JavaImplDTO(
            FunctionImpl.RuntimeType.SPARK.name(),
            FunctionResourcesDTO.builder()
                .withJars(new String[] {"hdfs://path/to/jar.jar"})
                .build(),
            ImmutableMap.of(),
            "com.example.TableFunction");

    // Create function definition
    FunctionDefinitionDTO definition =
        FunctionDefinitionDTO.builder()
            .withParameters(new FunctionParamDTO[] {})
            .withImpls(new FunctionImplDTO[] {javaImpl})
            .build();

    // Create table function DTO
    FunctionDTO tableFunction =
        FunctionDTO.builder()
            .withName("table_func")
            .withFunctionType(FunctionType.TABLE)
            .withDeterministic(false)
            .withComment("A table function")
            .withReturnColumns(new FunctionColumnDTO[] {col1, col2})
            .withDefinitions(new FunctionDefinitionDTO[] {definition})
            .withAudit(audit)
            .build();

    // Serialize and deserialize
    String json = JsonUtils.objectMapper().writeValueAsString(tableFunction);
    FunctionDTO deserialized = JsonUtils.objectMapper().readValue(json, FunctionDTO.class);

    Assertions.assertEquals(tableFunction.name(), deserialized.name());
    Assertions.assertEquals(FunctionType.TABLE, deserialized.functionType());
    Assertions.assertEquals(2, deserialized.returnColumns().length);
  }

  @Test
  public void testFunctionImplDTOSerDe() throws JsonProcessingException {
    Map<String, String> props = ImmutableMap.of("key", "value");
    FunctionResourcesDTO resources =
        FunctionResourcesDTO.builder()
            .withJars(new String[] {"hdfs://path/to/jar.jar"})
            .withFiles(new String[] {"hdfs://path/to/file.txt"})
            .build();

    // Test SQL implementation
    SQLImplDTO sqlImpl = new SQLImplDTO("SPARK", resources, props, "SELECT 1");
    String sqlJson = JsonUtils.objectMapper().writeValueAsString(sqlImpl);
    FunctionImplDTO deserializedSql =
        JsonUtils.objectMapper().readValue(sqlJson, FunctionImplDTO.class);
    Assertions.assertTrue(deserializedSql instanceof SQLImplDTO);
    Assertions.assertEquals(FunctionImpl.Language.SQL, deserializedSql.language());
    Assertions.assertEquals("SELECT 1", ((SQLImplDTO) deserializedSql).getSql());

    // Test Java implementation
    JavaImplDTO javaImpl = new JavaImplDTO("SPARK", resources, props, "com.example.MyUDF");
    String javaJson = JsonUtils.objectMapper().writeValueAsString(javaImpl);
    FunctionImplDTO deserializedJava =
        JsonUtils.objectMapper().readValue(javaJson, FunctionImplDTO.class);
    Assertions.assertTrue(deserializedJava instanceof JavaImplDTO);
    Assertions.assertEquals(FunctionImpl.Language.JAVA, deserializedJava.language());
    Assertions.assertEquals("com.example.MyUDF", ((JavaImplDTO) deserializedJava).getClassName());

    // Test Python implementation
    PythonImplDTO pythonImpl =
        new PythonImplDTO(
            "SPARK", resources, props, "my_handler", "def my_handler(x): return x * 2");
    String pythonJson = JsonUtils.objectMapper().writeValueAsString(pythonImpl);
    FunctionImplDTO deserializedPython =
        JsonUtils.objectMapper().readValue(pythonJson, FunctionImplDTO.class);
    Assertions.assertTrue(deserializedPython instanceof PythonImplDTO);
    Assertions.assertEquals(FunctionImpl.Language.PYTHON, deserializedPython.language());
    Assertions.assertEquals("my_handler", ((PythonImplDTO) deserializedPython).getHandler());
  }

  @Test
  public void testFunctionParamDTOWithDefaultValue() throws JsonProcessingException {
    // Test parameter without default value
    FunctionParamDTO paramWithoutDefault =
        FunctionParamDTO.builder()
            .withName("x")
            .withDataType(Types.IntegerType.get())
            .withComment("no default")
            .build();

    String json1 = JsonUtils.objectMapper().writeValueAsString(paramWithoutDefault);
    FunctionParamDTO deserialized1 =
        JsonUtils.objectMapper().readValue(json1, FunctionParamDTO.class);
    Assertions.assertEquals("x", deserialized1.name());
    Assertions.assertEquals(Types.IntegerType.get(), deserialized1.dataType());

    // Test parameter with default value
    LiteralDTO defaultValue =
        LiteralDTO.builder().withDataType(Types.IntegerType.get()).withValue("10").build();

    FunctionParamDTO paramWithDefault =
        FunctionParamDTO.builder()
            .withName("y")
            .withDataType(Types.IntegerType.get())
            .withDefaultValue(defaultValue)
            .build();

    String json2 = JsonUtils.objectMapper().writeValueAsString(paramWithDefault);
    FunctionParamDTO deserialized2 =
        JsonUtils.objectMapper().readValue(json2, FunctionParamDTO.class);
    Assertions.assertEquals("y", deserialized2.name());
    Assertions.assertNotNull(deserialized2.defaultValue());
  }

  @Test
  public void testFunctionResourcesDTOSerDe() throws JsonProcessingException {
    FunctionResourcesDTO resources =
        FunctionResourcesDTO.builder()
            .withJars(new String[] {"hdfs://path/to/jar1.jar", "hdfs://path/to/jar2.jar"})
            .withFiles(new String[] {"hdfs://path/to/file.txt"})
            .withArchives(new String[] {"hdfs://path/to/archive.zip"})
            .build();

    String json = JsonUtils.objectMapper().writeValueAsString(resources);
    FunctionResourcesDTO deserialized =
        JsonUtils.objectMapper().readValue(json, FunctionResourcesDTO.class);

    Assertions.assertArrayEquals(resources.getJars(), deserialized.getJars());
    Assertions.assertArrayEquals(resources.getFiles(), deserialized.getFiles());
    Assertions.assertArrayEquals(resources.getArchives(), deserialized.getArchives());
  }

  @Test
  public void testFunctionDefinitionDTOSerDe() throws JsonProcessingException {
    FunctionParamDTO param =
        FunctionParamDTO.builder().withName("input").withDataType(Types.StringType.get()).build();

    SQLImplDTO impl = new SQLImplDTO("SPARK", null, null, "SELECT UPPER(input)");

    FunctionDefinitionDTO definition =
        FunctionDefinitionDTO.builder()
            .withParameters(new FunctionParamDTO[] {param})
            .withImpls(new FunctionImplDTO[] {impl})
            .build();

    String json = JsonUtils.objectMapper().writeValueAsString(definition);
    FunctionDefinitionDTO deserialized =
        JsonUtils.objectMapper().readValue(json, FunctionDefinitionDTO.class);

    Assertions.assertEquals(1, deserialized.parameters().length);
    Assertions.assertEquals("input", deserialized.parameters()[0].name());
    Assertions.assertEquals(1, deserialized.impls().length);
  }
}
