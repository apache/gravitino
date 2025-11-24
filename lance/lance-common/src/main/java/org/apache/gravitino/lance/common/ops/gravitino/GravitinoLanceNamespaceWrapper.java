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
package org.apache.gravitino.lance.common.ops.gravitino;

import static org.apache.gravitino.lance.common.config.LanceConfig.METALAKE_NAME;
import static org.apache.gravitino.lance.common.config.LanceConfig.NAMESPACE_BACKEND_URI;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.lancedb.lance.namespace.LanceNamespaceException;
import com.lancedb.lance.namespace.util.CommonUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.lance.common.config.LanceConfig;
import org.apache.gravitino.lance.common.ops.LanceNamespaceOperations;
import org.apache.gravitino.lance.common.ops.LanceTableOperations;
import org.apache.gravitino.lance.common.ops.NamespaceWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitinoLanceNamespaceWrapper extends NamespaceWrapper {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoLanceNamespaceWrapper.class);
  private GravitinoClient client;

  private LanceNamespaceOperations namespaceOperations;
  private LanceTableOperations tableOperations;

  @VisibleForTesting
  GravitinoLanceNamespaceWrapper() {
    super(null);
  }

  public GravitinoLanceNamespaceWrapper(LanceConfig config) {
    super(config);
  }

  public GravitinoClient getClient() {
    return client;
  }

  @Override
  protected void initialize() {
    String uri = config().get(NAMESPACE_BACKEND_URI);
    String metalakeName = config().get(METALAKE_NAME);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metalakeName),
        "Metalake name must be provided for Lance Gravitino namespace backend");

    this.client = GravitinoClient.builder(uri).withMetalake(metalakeName).build();
    this.namespaceOperations = new GravitinoLanceNameSpaceOperations(this);
    this.tableOperations = new GravitinoLanceTableOperations(this);
  }

  @Override
  public LanceNamespaceOperations newNamespaceOps() {
    return namespaceOperations;
  }

  @Override
  protected LanceTableOperations newTableOps() {
    return tableOperations;
  }

  @Override
  public void close() {
    if (client != null) {
      try {
        client.close();
      } catch (Exception e) {
        LOG.warn("Error closing Gravitino client", e);
      }
    }
  }

  public boolean isLakehouseCatalog(Catalog catalog) {
    return catalog.type().equals(Catalog.Type.RELATIONAL)
        && "generic-lakehouse".equals(catalog.provider());
  }

  public Catalog loadAndValidateLakehouseCatalog(String catalogName) {
    Catalog catalog;
    try {
      catalog = client.loadCatalog(catalogName);
    } catch (NoSuchCatalogException e) {
      throw LanceNamespaceException.notFound(
          "Catalog not found: " + catalogName,
          NoSuchCatalogException.class.getSimpleName(),
          catalogName,
          CommonUtil.formatCurrentStackTrace());
    }
    if (!isLakehouseCatalog(catalog)) {
      throw LanceNamespaceException.notFound(
          "Catalog is not a lakehouse catalog: " + catalogName,
          NoSuchCatalogException.class.getSimpleName(),
          catalogName,
          CommonUtil.formatCurrentStackTrace());
    }
    return catalog;
  }
}
