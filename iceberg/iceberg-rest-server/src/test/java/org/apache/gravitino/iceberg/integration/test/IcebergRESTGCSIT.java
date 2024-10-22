/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.integration.test;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.integration.test.util.DownloaderUtils;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.storage.GCSProperties;

// @Disabled
// export
// GOOGLE_APPLICATION_CREDENTIALS=/Users/fanng/deploy/gcs/tonal-land-426304-d3-a75b6878b6ce.json
public class IcebergRESTGCSIT extends IcebergRESTJdbcCatalogIT {
  private String gcsWarehouse = "gs://strato-iceberg/test";
  private String gcsCredentialPath =
      "/Users/fanng/deploy/gcs/tonal-land-426304-d3-a75b6878b6ce.json";
  private String gcsBundleUrl =
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-gcp-bundle/1.5.2/iceberg-gcp-bundle-1.5.2.jar";

  @Override
  void initEnv() {
    if (ITUtils.isEmbedded()) {
      return;
    }
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    Preconditions.checkArgument(gravitinoHome != null, "GRAVITINO_HOME should not be null");
    String targetDir = gravitinoHome + "/iceberg-rest-server/libs/";
    try {
      DownloaderUtils.downloadFile(gcsBundleUrl, targetDir);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<String, String> getCatalogConfig() {
    HashMap m = new HashMap<String, String>();
    m.putAll(getCatalogJdbcConfig());
    m.putAll(getGCSConfig());
    return m;
  }

  public boolean supportsCredentialVending() {
    return false;
  }

  private Map<String, String> getGCSConfig() {
    Map configMap = new HashMap<String, String>();

    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + CredentialConstants.CREDENTIAL_PROVIDER_TYPE,
        CredentialConstants.GCS_TOKEN_CREDENTIAL_TYPE);
    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + GCSProperties.GRAVITINO_GCS_CREDENTIAL_FILE_PATH,
        gcsCredentialPath);
    configMap.put(
        IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConstants.IO_IMPL,
        "org.apache.iceberg.gcp.gcs.GCSFileIO");
    configMap.put(IcebergConfig.ICEBERG_CONFIG_PREFIX + IcebergConstants.WAREHOUSE, gcsWarehouse);
    return configMap;
  }
}
