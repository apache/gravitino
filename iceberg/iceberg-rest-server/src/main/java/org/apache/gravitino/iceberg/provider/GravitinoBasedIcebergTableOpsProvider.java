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
package org.apache.gravitino.iceberg.provider;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergPropertiesUtils;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.IcebergTableOps;
import org.apache.gravitino.iceberg.common.ops.IcebergTableOpsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This provider proxy Gravitino lakehouse-iceberg catalogs.
 *
 * <p>For example, there are one catalog named iceberg_catalog in metalake
 *
 * <p>The catalogName is iceberg_catalog
 */
public class GravitinoBasedIcebergTableOpsProvider implements IcebergTableOpsProvider {
  public static final Logger LOG =
      LoggerFactory.getLogger(GravitinoBasedIcebergTableOpsProvider.class);

  public static final String GRAVITINO_BASE_ICEBERG_TABLE_OPS_PROVIDER_NAME =
      "gravitino-based-provider";
  private final AtomicReference<GravitinoMetalake> metalakeWrapper = new AtomicReference<>();

  private String gravitinoUri;

  private String gravitinoMetalake;

  @Override
  public void initialize(Map<String, String> properties) {
    String uri = IcebergConstants.GRAVITINO_URI;
    String metalake = IcebergConstants.GRAVITINO_METALAKE;

    Preconditions.checkArgument(
        StringUtils.isNotBlank(uri), IcebergConstants.GRAVITINO_URI + " is blank");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metalake), IcebergConstants.GRAVITINO_METALAKE + " is blank");

    this.gravitinoUri = uri;
    this.gravitinoMetalake = metalake;
  }

  @Override
  public IcebergTableOps getIcebergTableOps(String catalogName) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalogName), "blank catalogName is illegal");
    Preconditions.checkArgument(
        !IcebergConstants.GRAVITINO_DEFAULT_CATALOG.equals(catalogName),
        IcebergConstants.GRAVITINO_DEFAULT_CATALOG + " is illegal in gravitino-based-provider");

    Catalog catalog = getMetalake().loadCatalog(catalogName);

    Preconditions.checkArgument(
        "lakehouse-iceberg".equals(catalog.provider()),
        String.format("%s.%s is not iceberg catalog", getMetalake(), catalogName));

    Map<String, String> properties =
        IcebergPropertiesUtils.toIcebergCatalogProperties(catalog.properties());
    return new IcebergTableOps(new IcebergConfig(properties));
  }

  public GravitinoMetalake getMetalake() {
    if (metalakeWrapper.get() == null) {
      metalakeWrapper.compareAndSet(
          null, GravitinoAdminClient.builder(gravitinoUri).build().loadMetalake(gravitinoMetalake));
    }
    return metalakeWrapper.get();
  }

  @VisibleForTesting
  void setGravitinoMetalake(GravitinoMetalake metalake) {
    this.metalakeWrapper.compareAndSet(null, metalake);
  }
}
