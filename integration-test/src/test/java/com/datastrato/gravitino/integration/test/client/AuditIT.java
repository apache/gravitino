/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.client;

import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.MetalakeChange;
import com.datastrato.gravitino.auth.AuthenticatorType;
import com.datastrato.gravitino.client.GravitinoMetalake;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.utils.RandomNameUtils;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.awaitility.Awaitility;
import java.util.concurrent.TimeUnit;

public class AuditIT extends AbstractIT {

  private static final String expectUser = System.getProperty("user.name");

  @BeforeAll
  public static void startIntegrationTest() throws Exception {
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.AUTHENTICATOR.getKey(), AuthenticatorType.SIMPLE.name().toLowerCase());
    registerCustomConfigs(configs);
    AbstractIT.startIntegrationTest();
  }

  @Test
  public void testAuditMetalake() throws Exception {
    String metalakeAuditName = RandomNameUtils.genRandomName("metalakeAudit");
    String newName = RandomNameUtils.genRandomName("newmetaname");

    Awaitility.await().atMost(1, TimeUnit.MINUTES).until(() -> isGravitinoServerUp());

    GravitinoMetalake metaLake = client.createMetalake(metalakeAuditName, "metalake A comment", Collections.emptyMap());
    Assertions.assertEquals(expectUser, metaLake.auditInfo().creator());
    Assertions.assertNull(metaLake.auditInfo().lastModifier());
    MetalakeChange[] changes = new MetalakeChange[] {
        MetalakeChange.rename(newName), MetalakeChange.updateComment("new metalake comment")
    };
    metaLake = client.alterMetalake(metalakeAuditName, changes);
    Assertions.assertEquals(expectUser, metaLake.auditInfo().creator());
    Assertions.assertEquals(expectUser, metaLake.auditInfo().lastModifier());
    client.dropMetalake(newName);
  }

  // Method to check if the Gravitino server is up
  private boolean isGravitinoServerUp() {
    try {
      URL url = new URL("http://localhost:8090");
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      connection.setConnectTimeout(1000);
      connection.connect();
      int responseCode = connection.getResponseCode();
      return responseCode == 200;
      System.out.println("/n/n/n"+responseCode+"/n/n/n");
    } catch (IOException e) {
      return false;
    }
  }
}
