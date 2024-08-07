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

package org.apache.gravitino.catalog.hive.integration.test;

import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class MultipleHMSUserAuthenticationIT extends HiveUserAuthenticationIT {
  @BeforeAll
  static void setHiveURI() {
    String ip = kerberosHiveContainer.getContainerIpAddress();
    int oldHMSPort = HiveContainer.HIVE_METASTORE_PORT;

    // About the value of `newAddedHMSPort`, please see the value `hive.metastore.port` in the
    // dev/docker/kerberos-hive/hive-site1.xml
    int newAddedHMSPort = 19083;
    // Multiple HMS URIs, I put the new one first to test the new HMS URI first.
    HIVE_METASTORE_URI =
        String.format("thrift://%s:%d,thrift://%s:%d", ip, newAddedHMSPort, ip, oldHMSPort);
  }

  @Test
  public void testUserAuthentication() {
    METALAKE_NAME = GravitinoITUtils.genRandomName("test_metalake");
    CATALOG_NAME = GravitinoITUtils.genRandomName("test_catalog");
    SCHEMA_NAME = GravitinoITUtils.genRandomName("test_schema");
    TABLE_NAME = GravitinoITUtils.genRandomName("test_table");
    super.testUserAuthentication();
  }
}
