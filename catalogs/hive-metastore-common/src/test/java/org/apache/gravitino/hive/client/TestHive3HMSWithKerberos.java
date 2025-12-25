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

package org.apache.gravitino.hive.client;

import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;

/**
 * Kerberos-enabled Hive3 HMS tests. Reuses Kerberos setup from {@link TestHive2HMSWithKerberos} but
 * starts the Hive3 container and uses the default Hive3 catalog name "hive".
 */
@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestHive3HMSWithKerberos extends TestHive2HMSWithKerberos {

  @BeforeAll
  @Override
  public void startHiveContainer() {
    testPrefix = "hive3_kerberos";
    catalogName = "hive"; // Hive3 default catalog

    containerSuite.startKerberosHiveContainer(
        ImmutableMap.of(HiveContainer.HIVE_RUNTIME_VERSION, HiveContainer.HIVE3));
    hiveContainer = containerSuite.getKerberosHiveContainer();

    metastoreUri =
        String.format(
            "thrift://%s:%d",
            hiveContainer.getContainerIpAddress(), HiveContainer.HIVE_METASTORE_PORT);
    hdfsBasePath =
        String.format(
            "hdfs://%s:%d/tmp/gravitino_test",
            hiveContainer.getContainerIpAddress(), HiveContainer.HDFS_DEFAULTFS_PORT);

    // Prepare Kerberos config (keytab/krb5) for client connections.
    prepareKerberosConfig();

    hiveClient = new HiveClientFactory(createHiveProperties(), "testPrefix").createHiveClient();
  }
}
