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
package org.apache.gravitino.meta;

import java.time.Instant;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionDefinitions;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.FunctionImpls;
import org.apache.gravitino.function.FunctionParam;
import org.apache.gravitino.function.FunctionParams;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFunctionEntity {

  @Test
  public void testBuildShouldFailWhenNamespaceIsMissing() {
    FunctionParam param = FunctionParams.of("param1", Types.IntegerType.get());
    FunctionImpl impl = FunctionImpls.ofSql(FunctionImpl.RuntimeType.SPARK, "SELECT param1 + 1");
    FunctionDefinition definition =
        FunctionDefinitions.of(
            new FunctionParam[] {param}, Types.IntegerType.get(), new FunctionImpl[] {impl});
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("tester").withCreateTime(Instant.now()).build();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            FunctionEntity.builder()
                .withId(1L)
                .withName("f")
                .withComment("test")
                .withFunctionType(FunctionType.SCALAR)
                .withDeterministic(true)
                .withDefinitions(new FunctionDefinition[] {definition})
                .withAuditInfo(auditInfo)
                .build());
  }

  @Test
  public void testBuildShouldSucceedWhenNamespaceIsPresent() {
    FunctionParam param = FunctionParams.of("param1", Types.IntegerType.get());
    FunctionImpl impl = FunctionImpls.ofSql(FunctionImpl.RuntimeType.SPARK, "SELECT param1 + 1");
    FunctionDefinition definition =
        FunctionDefinitions.of(
            new FunctionParam[] {param}, Types.IntegerType.get(), new FunctionImpl[] {impl});
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("tester").withCreateTime(Instant.now()).build();

    FunctionEntity entity =
        FunctionEntity.builder()
            .withId(1L)
            .withName("f")
            .withNamespace(Namespace.of("m1", "c1", "s1"))
            .withComment("test")
            .withFunctionType(FunctionType.SCALAR)
            .withDeterministic(true)
            .withDefinitions(new FunctionDefinition[] {definition})
            .withAuditInfo(auditInfo)
            .build();

    Assertions.assertEquals(Namespace.of("m1", "c1", "s1"), entity.namespace());
  }
}
