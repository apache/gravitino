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
package com.apache.gravitino.integration.test.authorization.ranger;

import static org.apache.ranger.plugin.util.SearchFilter.SERVICE_NAME;
import static org.apache.ranger.plugin.util.SearchFilter.SERVICE_TYPE;

import com.apache.gravitino.integration.test.container.ContainerSuite;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ranger.RangerClient;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class RangerIT {
  private static final String trinoServiceName = "trinodev";
  private static final String trinoType = "trino";
  private static final String hiveServiceName = "hivedev";
  private static final String hiveType = "hive";
  private static RangerClient rangerClient;

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  @BeforeAll
  public static void setup() {
    containerSuite.startRangerContainer();

    rangerClient = containerSuite.getRangerContainer().rangerClient;
  }

  @AfterAll
  public static void cleanup() throws RangerServiceException {
    if (rangerClient != null) {
      rangerClient.deleteService(trinoServiceName);
      rangerClient.deleteService(hiveServiceName);
    }
  }

  @Test
  public void testCreateTrinoService() throws RangerServiceException {
    String usernameKey = "username";
    String usernameVal = "admin";
    String jdbcKey = "jdbc.driverClassName";
    String jdbcVal = "io.trino.jdbc.TrinoDriver";
    String jdbcUrlKey = "jdbc.url";
    String jdbcUrlVal = "http://localhost:8080";

    RangerService service = new RangerService();
    service.setType(trinoType);
    service.setName(trinoServiceName);
    service.setConfigs(
        ImmutableMap.<String, String>builder()
            .put(usernameKey, usernameVal)
            .put(jdbcKey, jdbcVal)
            .put(jdbcUrlKey, jdbcUrlVal)
            .build());

    RangerService createdService = rangerClient.createService(service);
    Assertions.assertNotNull(createdService);

    Map<String, String> filter = new HashMap<>();
    filter.put(SERVICE_TYPE, trinoType);
    filter.put(SERVICE_NAME, trinoServiceName);
    List<RangerService> services = rangerClient.findServices(filter);
    Assertions.assertEquals(services.get(0).getName(), trinoServiceName);
    Assertions.assertEquals(services.get(0).getType(), trinoType);
    Assertions.assertEquals(services.get(0).getConfigs().get(usernameKey), usernameVal);
    Assertions.assertEquals(services.get(0).getConfigs().get(jdbcKey), jdbcVal);
    Assertions.assertEquals(services.get(0).getConfigs().get(jdbcUrlKey), jdbcUrlVal);
  }

  @Test
  public void createHiveService() throws RangerServiceException {
    String usernameKey = "username";
    String usernameVal = "admin";
    String passwordKey = "password";
    String passwordVal = "admin";
    String jdbcKey = "jdbc.driverClassName";
    String jdbcVal = "org.apache.hive.jdbc.HiveDriver";
    String jdbcUrlKey = "jdbc.url";
    String jdbcUrlVal = "jdbc:hive2://172.17.0.2:10000";

    RangerService service = new RangerService();
    service.setType(hiveType);
    service.setName(hiveServiceName);
    service.setConfigs(
        ImmutableMap.<String, String>builder()
            .put(usernameKey, usernameVal)
            .put(passwordKey, passwordVal)
            .put(jdbcKey, jdbcVal)
            .put(jdbcUrlKey, jdbcUrlVal)
            .build());

    RangerService createdService = rangerClient.createService(service);
    Assertions.assertNotNull(createdService);

    Map<String, String> filter = new HashMap<>();
    filter.put(SERVICE_TYPE, hiveType);
    filter.put(SERVICE_NAME, hiveServiceName);
    List<RangerService> services = rangerClient.findServices(filter);
    Assertions.assertEquals(services.get(0).getName(), hiveServiceName);
    Assertions.assertEquals(services.get(0).getType(), hiveType);
    Assertions.assertEquals(services.get(0).getConfigs().get(usernameKey), usernameVal);
    Assertions.assertEquals(services.get(0).getConfigs().get(jdbcKey), jdbcVal);
    Assertions.assertEquals(services.get(0).getConfigs().get(jdbcUrlKey), jdbcUrlVal);
  }
}
