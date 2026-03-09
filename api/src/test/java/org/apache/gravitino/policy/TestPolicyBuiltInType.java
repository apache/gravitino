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

package org.apache.gravitino.policy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPolicyBuiltInType {

  @Test
  void testFromPolicyTypeWithBuiltInTypeValue() {
    Assertions.assertEquals(
        Policy.BuiltInType.ICEBERG_COMPACTION,
        Policy.BuiltInType.fromPolicyType("system_iceberg_compaction"));
  }

  @Test
  void testFromPolicyTypeWithEnumName() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> Policy.BuiltInType.fromPolicyType("ICEBERG_COMPACTION"));
    Assertions.assertTrue(exception.getMessage().contains("Unknown policy type"));
  }

  @Test
  void testBuiltInTypePolicyTypeValue() {
    Assertions.assertEquals(
        "system_iceberg_compaction", Policy.BuiltInType.ICEBERG_COMPACTION.policyType());
  }

  @Test
  void testBuiltInTypeContentClass() {
    Assertions.assertEquals(
        IcebergDataCompactionContent.class, Policy.BuiltInType.ICEBERG_COMPACTION.contentClass());
  }
}
