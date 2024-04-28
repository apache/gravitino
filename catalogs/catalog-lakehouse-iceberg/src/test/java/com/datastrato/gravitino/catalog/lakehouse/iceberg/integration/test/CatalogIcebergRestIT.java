/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.integration.test;

import com.datastrato.gravitino.auxiliary.AuxiliaryServiceManager;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergConfig;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.IcebergRESTService;
import com.datastrato.gravitino.integration.test.container.HiveContainer;
import com.datastrato.gravitino.server.web.JettyServerConfig;
import com.datastrato.gravitino.utils.MapUtils;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;

@Tag("gravitino-docker-it")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CatalogIcebergRestIT extends CatalogIcebergBaseIT {

  @Override
  protected void initIcebergCatalogProperties() {
    Map<String, String> map =
        serverConfig.getConfigsWithPrefix(AuxiliaryServiceManager.GRAVITINO_AUX_SERVICE_PREFIX);
    map = MapUtils.getPrefixMap(map, IcebergRESTService.SERVICE_NAME + ".");
    IcebergConfig icebergConfig = new IcebergConfig(map);
    String host = icebergConfig.get(JettyServerConfig.WEBSERVER_HOST);
    int port = icebergConfig.get(JettyServerConfig.WEBSERVER_HTTP_PORT);
    URIS = String.format("http://%s:%d/iceberg/", host, port);
    TYPE = "rest";
    WAREHOUSE =
        String.format(
            "hdfs://%s:%d/user/hive/iceberg-rest-warehouse/",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT);
  }
}
