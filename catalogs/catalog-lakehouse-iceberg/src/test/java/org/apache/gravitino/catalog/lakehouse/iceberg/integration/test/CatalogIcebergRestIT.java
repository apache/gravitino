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
package org.apache.gravitino.catalog.lakehouse.iceberg.integration.test;

import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.server.web.JettyServerConfig;
import org.junit.jupiter.api.Tag;

@Tag("gravitino-docker-test")
public class CatalogIcebergRestIT extends CatalogIcebergBaseIT {

  @Override
  protected void initIcebergCatalogProperties() {
    Map<String, String> map =
        serverConfig.getConfigsWithPrefix(
            String.format("gravitino.%s.", IcebergConstants.GRAVITINO_ICEBERG_REST_SERVICE_NAME));
    IcebergConfig icebergConfig = new IcebergConfig(map);
    String host = icebergConfig.get(JettyServerConfig.WEBSERVER_HOST);
    int port = icebergConfig.get(JettyServerConfig.WEBSERVER_HTTP_PORT);
    URIS = String.format("http://%s:%d/iceberg/", host, port);
    TYPE = "rest";
    WAREHOUSE =
        String.format(
            "hdfs://%s:%d/user/hive/warehouse-catalog-iceberg/",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT);
  }
}
