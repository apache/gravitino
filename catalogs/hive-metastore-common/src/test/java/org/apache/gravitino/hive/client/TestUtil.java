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

import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestUtil {

  private static final String HIVE_METASTORE_URIS = "hive.metastore.uris";

  @Test
  void testMetastoreHostnameResolvedToIP() {
    Properties props = new Properties();
    props.setProperty(HIVE_METASTORE_URIS, "thrift://localhost:9083");
    Configuration config = new Configuration();

    Util.updateConfigurationFromProperties(props, config);

    String resolved = config.get(HIVE_METASTORE_URIS);
    Assertions.assertFalse(
        resolved.contains("localhost"), "hostname should be replaced with IP, got: " + resolved);
    Assertions.assertTrue(resolved.startsWith("thrift://") && resolved.endsWith(":9083"));
  }

  @Test
  void testMetastoreIPAddressPassthrough() {
    Properties props = new Properties();
    props.setProperty(HIVE_METASTORE_URIS, "thrift://192.168.1.1:9083");
    Configuration config = new Configuration();

    Util.updateConfigurationFromProperties(props, config);

    Assertions.assertEquals("thrift://192.168.1.1:9083", config.get(HIVE_METASTORE_URIS));
  }
}
