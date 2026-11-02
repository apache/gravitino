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

package org.apache.gravitino.oss.credential;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import org.junit.jupiter.api.Test;

class TestOSSTokenGenerator {
  @Test
  void listPolicyIncludesEveryPathInTheSameBucket() throws Exception {
    OSSTokenGenerator generator = new OSSTokenGenerator();
    String policy =
        generator.createPolicy(
            Collections.singleton("oss://lake/warehouse/db/orders"),
            new HashSet<>(Arrays.asList("oss://lake/data/orders", "oss://lake/metadata/orders")),
            "cn-hangzhou");
    JsonNode prefixes = new ObjectMapper().readTree(policy).findValue("oss:Prefix");
    assertTrue(prefixes.toString().contains("warehouse/db/orders/*"));
    assertTrue(prefixes.toString().contains("data/orders/*"));
    assertTrue(prefixes.toString().contains("metadata/orders/*"));
  }
}
