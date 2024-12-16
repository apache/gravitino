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
package org.apache.gravitino.authorization.ranger;

import com.google.common.collect.Maps;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRangerAuthorizationProperties {
  @Test
  void testRangerProperties() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("authorization.ranger.auth.type", "simple");
    properties.put("authorization.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.ranger.username", "admin");
    properties.put("authorization.ranger.password", "admin");
    properties.put("authorization.ranger.service.type", "hive");
    properties.put("authorization.ranger.service.name", "hiveDev");
    Assertions.assertDoesNotThrow(() -> RangerAuthorizationProperties.validate(properties));
  }

  @Test
  void testRangerPropertiesLoseAuthType() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("authorization.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.ranger.username", "admin");
    properties.put("authorization.ranger.password", "admin");
    properties.put("authorization.ranger.service.type", "hive");
    properties.put("authorization.ranger.service.name", "hiveDev");
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> RangerAuthorizationProperties.validate(properties));
  }

  @Test
  void testRangerPropertiesLoseAdminUrl() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("authorization.ranger.auth.type", "simple");
    properties.put("authorization.ranger.username", "admin");
    properties.put("authorization.ranger.password", "admin");
    properties.put("authorization.ranger.service.type", "hive");
    properties.put("authorization.ranger.service.name", "hiveDev");
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> RangerAuthorizationProperties.validate(properties));
  }

  @Test
  void testRangerPropertiesLoseUserName() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("authorization.ranger.auth.type", "simple");
    properties.put("authorization.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.ranger.password", "admin");
    properties.put("authorization.ranger.service.type", "hive");
    properties.put("authorization.ranger.service.name", "hiveDev");
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> RangerAuthorizationProperties.validate(properties));
  }

  @Test
  void testRangerPropertiesLosePassword() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("authorization.ranger.auth.type", "simple");
    properties.put("authorization.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.ranger.username", "admin");
    properties.put("authorization.ranger.service.type", "hive");
    properties.put("authorization.ranger.service.name", "hiveDev");
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> RangerAuthorizationProperties.validate(properties));
  }

  @Test
  void testRangerPropertiesLoseServiceType() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("authorization.ranger.auth.type", "simple");
    properties.put("authorization.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.ranger.username", "admin");
    properties.put("authorization.ranger.password", "admin");
    properties.put("authorization.ranger.service.name", "hiveDev");
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> RangerAuthorizationProperties.validate(properties));
  }

  @Test
  void testRangerPropertiesLoseServiceName() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("authorization.ranger.auth.type", "simple");
    properties.put("authorization.ranger.admin.url", "http://localhost:6080");
    properties.put("authorization.ranger.username", "admin");
    properties.put("authorization.ranger.password", "admin");
    properties.put("authorization.ranger.service.type", "hive");
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> RangerAuthorizationProperties.validate(properties));
  }
}
