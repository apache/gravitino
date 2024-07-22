/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package org.apache.gravitino.catalog.hive.integration.test;

import org.apache.gravitino.integration.test.container.HiveContainer;
import org.junit.jupiter.api.BeforeAll;

public class MultipleHMSUserAuthenticationIT extends HiveUserAuthenticationIT {
  @BeforeAll
  static void setHiveURI() {
    String ip = kerberosHiveContainer.getContainerIpAddress();
    int port = HiveContainer.HIVE_METASTORE_PORT;
    // Multiple HMS URIs, I put the new one first to test the new HMS URI first.
    HIVE_METASTORE_URI = String.format("thrift://%s:1%d,thrift://%s:%d", ip, port, ip, port);
  }
}
