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
import static org.apache.gravitino.lance.common.config.LanceConfig.NAMESPACE_URI;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.lancedb.lance.namespace.LanceNamespaceException;
import com.lancedb.lance.namespace.ObjectIdentifier;
import com.lancedb.lance.namespace.model.CreateNamespaceRequest;
import com.lancedb.lance.namespace.model.CreateNamespaceResponse;
import com.lancedb.lance.namespace.model.DescribeNamespaceResponse;
import com.lancedb.lance.namespace.model.DropNamespaceRequest;
import com.lancedb.lance.namespace.model.DropNamespaceResponse;
import com.lancedb.lance.namespace.model.ListNamespacesResponse;
import com.lancedb.lance.namespace.model.ListTablesResponse;
import com.lancedb.lance.namespace.util.PageUtil;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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

public class GravitinoLanceNamespaceWrapper extends NamespaceWrapper
    implements LanceNamespaceOperations, LanceTableOperations {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoLanceNamespaceWrapper.class);
  private GravitinoClient client;

  public GravitinoLanceNamespaceWrapper(LanceConfig config) {
    super(config);
  }

  @Override
  protected void initialize() {
    String uri = config().get(NAMESPACE_URI);
    String metalakeName = config().get(METALAKE_NAME);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metalakeName),
        "Metalake name must be provided for Gravitino namespace backend");

    this.client = GravitinoClient.builder(uri).withMetalake(metalakeName).build();
  }

  @Override
  public LanceNamespaceOperations newNamespaceOps() {
    return this;
  }

  @Override
  protected LanceTableOperations newTableOps() {
    return this;
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

  @Override
  public ListNamespacesResponse listNamespaces(
      String namespaceId, String delimiter, String pageToken, Integer limit) {
    ObjectIdentifier nsId = ObjectIdentifier.of(namespaceId, delimiter);
    Preconditions.checkArgument(
        nsId.levels() <= 2, "Expected at most 2-level namespace but got: %s", namespaceId);

    List<String> namespaces;
    switch (nsId.levels()) {
      case 0:
        // List catalogs of type relational and provider generic-lakehouse
        namespaces =
            Arrays.stream(client.listCatalogsInfo())
                .filter(this::isLakehouseCatalog)
                .map(Catalog::name)
                .collect(Collectors.toList());
        break;

      case 1:
        // List schemas under the catalog
        String catalogName = nsId.levelAtListPos(0);
        Catalog catalog = client.loadCatalog(catalogName);
        if (!isLakehouseCatalog(catalog)) {
          throw new NoSuchCatalogException("Catalog not found: %s", catalogName);
        }

        namespaces = Lists.newArrayList(catalog.asSchemas().listSchemas());
        break;

      default:
        throw new IllegalArgumentException(
            "Expected at most 2-level namespace but got: " + namespaceId);
    }

    Collections.sort(namespaces);
    PageUtil.Page page =
        PageUtil.splitPage(namespaces, pageToken, PageUtil.normalizePageSize(limit));
    ListNamespacesResponse response = new ListNamespacesResponse();
    response.setNamespaces(Sets.newHashSet(page.items()));
    response.setPageToken(page.nextPageToken());
    return response;
  }

  @Override
  public DescribeNamespaceResponse describeNamespace(String id, String delimiter) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public CreateNamespaceResponse createNamespace(
      String id,
      String delimiter,
      CreateNamespaceRequest.ModeEnum mode,
      Map<String, String> properties) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public DropNamespaceResponse dropNamespace(
      String id,
      String delimiter,
      DropNamespaceRequest.ModeEnum mode,
      DropNamespaceRequest.BehaviorEnum behavior) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void namespaceExists(String id, String delimiter) throws LanceNamespaceException {}

  private boolean isLakehouseCatalog(Catalog catalog) {
    return catalog.type().equals(Catalog.Type.RELATIONAL)
        && "generic-lakehouse".equals(catalog.provider());
  }

  @Override
  public ListTablesResponse listTables(
      String id, String delimiter, String pageToken, Integer limit) {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
