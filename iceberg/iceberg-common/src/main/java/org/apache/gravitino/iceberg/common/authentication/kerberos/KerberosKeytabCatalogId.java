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

package org.apache.gravitino.iceberg.common.authentication.kerberos;

import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;

/** Resolves a unique identifier for per-catalog Kerberos keytab local files. */
public final class KerberosKeytabCatalogId {

  /** Catalog UUID property set by Gravitino-managed Iceberg catalogs. */
  public static final String CATALOG_UUID_KEY = "catalog_uuid";

  private KerberosKeytabCatalogId() {}

  /**
   * Resolves the catalog identifier used for local Kerberos keytab file names.
   *
   * <p>Gravitino-managed catalogs use {@link #CATALOG_UUID_KEY}. Standalone Iceberg REST catalogs
   * fall back to {@link IcebergConstants#CATALOG_BACKEND_NAME} or the Iceberg catalog name.
   *
   * @param properties catalog properties
   * @param catalogName Iceberg catalog name from {@code Catalog#name()}
   * @return a non-blank catalog identifier for keytab file naming
   */
  public static String resolve(Map<String, String> properties, String catalogName) {
    String catalogUuid = properties.get(CATALOG_UUID_KEY);
    if (StringUtils.isNotBlank(catalogUuid)) {
      return catalogUuid;
    }

    String backendName = properties.get(IcebergConstants.CATALOG_BACKEND_NAME);
    if (StringUtils.isNotBlank(backendName)) {
      return backendName;
    }

    if (StringUtils.isNotBlank(catalogName)) {
      return catalogName;
    }

    throw new IllegalStateException(
        "Cannot resolve Kerberos keytab catalog id: catalog_uuid, catalog-backend-name and "
            + "catalog name are all blank");
  }
}
