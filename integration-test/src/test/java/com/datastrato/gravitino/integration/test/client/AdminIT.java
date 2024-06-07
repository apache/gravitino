/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.client;

import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.authorization.User;
import com.datastrato.gravitino.exceptions.ForbiddenException;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AdminIT extends AbstractIT {

  @BeforeAll
  public static void startIntegrationTest() throws Exception {
    // We start and stop the server for every case
  }

  @AfterAll
  public static void stopIntegrationTest() throws IOException, InterruptedException {
    // We start and stop the server for every case
  }

  @AfterEach
  public void stop() throws Exception {
    AbstractIT.stopIntegrationTest();
  }

  @Test
  public void testAdminInterfacePass() throws Exception {
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), "true");
    configs.put(Configs.SERVICE_ADMINS.getKey(), AuthConstants.ANONYMOUS_USER);
    registerCustomConfigs(configs);
    AbstractIT.startIntegrationTest();

    User user = client.addMetalakeAdmin("user");
    Assertions.assertEquals("user", user.name());
    Assertions.assertEquals(1, user.roles().size());
    Assertions.assertTrue(user.roles().contains(Entity.METALAKE_CREATE_ROLE));
  }

  @Test
  public void testAdminIntefaceReject() throws Exception {
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), "true");
    configs.put(Configs.SERVICE_ADMINS.getKey(), "admin");
    registerCustomConfigs(configs);
    AbstractIT.startIntegrationTest();

    Assertions.assertThrows(ForbiddenException.class, () -> client.addMetalakeAdmin("user"));
  }
}
