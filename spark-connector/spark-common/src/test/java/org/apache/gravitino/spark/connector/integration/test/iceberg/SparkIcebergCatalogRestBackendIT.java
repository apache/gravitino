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
package org.apache.gravitino.spark.connector.integration.test.iceberg;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.gravitino.spark.connector.iceberg.IcebergPropertiesConstants;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.condition.DisabledIf;

/**
 * This class use Apache Iceberg RESTCatalog for test, and the real backend catalog is HiveCatalog.
 */
@Tag("gravitino-docker-test")
// Spark connector use low Iceberg version, couldn't work with Iceberg REST server with high Iceberg
// version in embedded mode.
@DisabledIf("org.apache.gravitino.integration.test.util.ITUtils#isEmbedded")
public abstract class SparkIcebergCatalogRestBackendIT extends SparkIcebergCatalogIT {

  @Override
  protected Map<String, String> getCatalogConfigs() {
    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_REST);
    catalogProperties.put(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI, icebergRestServiceUri);

    return catalogProperties;
  }
}
