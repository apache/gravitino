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
import com.google.common.collect.Maps;
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
import com.lancedb.lance.namespace.util.CommonUtil;
import com.lancedb.lance.namespace.util.PageUtil;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.CatalogAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NonEmptyCatalogException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
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
        namespaces =
            Arrays.stream(client.listCatalogsInfo())
                .filter(this::isLakehouseCatalog)
                .map(Catalog::name)
                .collect(Collectors.toList());
        break;
      case 1:
        Catalog catalog = loadAndValidateLakehouseCatalog(nsId.levelAtListPos(0));
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
  public DescribeNamespaceResponse describeNamespace(String namespaceId, String delimiter) {
    ObjectIdentifier nsId = ObjectIdentifier.of(namespaceId, delimiter);
    Preconditions.checkArgument(
        nsId.levels() <= 2, "Expected at most 2-level namespace but got: %s", namespaceId);

    Catalog catalog = loadAndValidateLakehouseCatalog(nsId.levelAtListPos(0));
    Map<String, String> properties = Maps.newHashMap();

    switch (nsId.levels()) {
      case 0:
        Optional.ofNullable(catalog.properties()).ifPresent(properties::putAll);
        break;
      case 1:
        String schemaName = nsId.levelAtListPos(1);
        Schema schema = catalog.asSchemas().loadSchema(schemaName);
        Optional.ofNullable(schema.properties()).ifPresent(properties::putAll);
        break;
      default:
        throw new IllegalArgumentException(
            "Expected at most 2-level namespace but got: " + namespaceId);
    }

    DescribeNamespaceResponse response = new DescribeNamespaceResponse();
    response.setProperties(properties);
    return response;
  }

  @Override
  public CreateNamespaceResponse createNamespace(
      String namespaceId,
      String delimiter,
      CreateNamespaceRequest.ModeEnum mode,
      Map<String, String> properties) {
    ObjectIdentifier nsId = ObjectIdentifier.of(namespaceId, delimiter);
    Preconditions.checkArgument(
        nsId.levels() <= 2, "Expected at most 2-level namespace but got: %s", namespaceId);

    switch (nsId.levels()) {
      case 0:
        return createOrUpdateCatalog(nsId.levelAtListPos(0), mode, properties);
      case 1:
        return createOrUpdateSchema(
            nsId.levelAtListPos(0), nsId.levelAtListPos(1), mode, properties);
      default:
        throw new IllegalArgumentException(
            "Expected at most 2-level namespace but got: " + namespaceId);
    }
  }

  @Override
  public DropNamespaceResponse dropNamespace(
      String namespaceId,
      String delimiter,
      DropNamespaceRequest.ModeEnum mode,
      DropNamespaceRequest.BehaviorEnum behavior) {
    ObjectIdentifier nsId = ObjectIdentifier.of(namespaceId, delimiter);
    Preconditions.checkArgument(
        nsId.levels() <= 2, "Expected at most 2-level namespace but got: %s", namespaceId);

    switch (nsId.levels()) {
      case 0:
        return dropCatalog(nsId.levelAtListPos(0), mode, behavior);
      case 1:
        return dropSchema(nsId.levelAtListPos(0), nsId.levelAtListPos(1), mode, behavior);
      default:
        throw new IllegalArgumentException(
            "Expected at most 2-level namespace but got: " + namespaceId);
    }
  }

  @Override
  public void namespaceExists(String id, String delimiter) throws LanceNamespaceException {
    ObjectIdentifier nsId = ObjectIdentifier.of(id, delimiter);
    Preconditions.checkArgument(
        nsId.levels() <= 2, "Expected at most 2-level namespace but got: %s", id);

    Catalog catalog = loadAndValidateLakehouseCatalog(nsId.levelAtListPos(0));
    if (nsId.levels() == 1) {
      String schemaName = nsId.levelAtListPos(1);
      if (!catalog.asSchemas().schemaExists(schemaName)) {
        throw LanceNamespaceException.notFound(
            "Schema not found: " + schemaName,
            NoSuchSchemaException.class.getSimpleName(),
            schemaName,
            CommonUtil.formatCurrentStackTrace());
      }
    }
  }

  @Override
  public ListTablesResponse listTables(
      String id, String delimiter, String pageToken, Integer limit) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  private boolean isLakehouseCatalog(Catalog catalog) {
    return catalog.type().equals(Catalog.Type.RELATIONAL)
        && "generic-lakehouse".equals(catalog.provider());
  }

  private Catalog loadAndValidateLakehouseCatalog(String catalogName) {
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
          "Catalog not found: " + catalogName,
          NoSuchCatalogException.class.getSimpleName(),
          catalogName,
          CommonUtil.formatCurrentStackTrace());
    }
    return catalog;
  }

  private CreateNamespaceResponse createOrUpdateCatalog(
      String catalogName, CreateNamespaceRequest.ModeEnum mode, Map<String, String> properties) {
    CreateNamespaceResponse response = new CreateNamespaceResponse();
    try {
      Catalog catalog = client.loadCatalog(catalogName);
      // Catalog exists, handle based on mode
      switch (mode) {
        case EXIST_OK:
          response.setProperties(Maps.newHashMap());
          return response;
        case CREATE:
          throw LanceNamespaceException.conflict(
              "Catalog already exists: " + catalogName,
              CatalogAlreadyExistsException.class.getSimpleName(),
              catalogName,
              CommonUtil.formatCurrentStackTrace());
        case OVERWRITE:
          CatalogChange[] changes =
              buildChanges(
                  properties,
                  catalog.properties(),
                  CatalogChange::setProperty,
                  CatalogChange::removeProperty,
                  CatalogChange[]::new);
          Catalog alteredCatalog = client.alterCatalog(catalogName, changes);
          Optional.ofNullable(alteredCatalog.properties()).ifPresent(response::setProperties);
          return response;
        default:
          throw new IllegalArgumentException("Unknown mode: " + mode);
      }
    } catch (NoSuchCatalogException e) {
      // Catalog does not exist, create it
      Catalog createdCatalog =
          client.createCatalog(
              catalogName, Catalog.Type.RELATIONAL, "generic-lakehouse", properties);
      response.setProperties(
          createdCatalog.properties() == null ? Maps.newHashMap() : createdCatalog.properties());
      return response;
    }
  }

  private CreateNamespaceResponse createOrUpdateSchema(
      String catalogName,
      String schemaName,
      CreateNamespaceRequest.ModeEnum mode,
      Map<String, String> properties) {
    CreateNamespaceResponse response = new CreateNamespaceResponse();
    Catalog loadedCatalog = loadAndValidateLakehouseCatalog(catalogName);

    try {
      Schema schema = loadedCatalog.asSchemas().loadSchema(schemaName);
      // Schema exists, handle based on mode
      switch (mode) {
        case EXIST_OK:
          response.setProperties(Maps.newHashMap());
          return response;
        case CREATE:
          throw LanceNamespaceException.conflict(
              "Schema already exists: " + schemaName,
              SchemaAlreadyExistsException.class.getSimpleName(),
              schemaName,
              CommonUtil.formatCurrentStackTrace());
        case OVERWRITE:
          SchemaChange[] changes =
              buildChanges(
                  properties,
                  schema.properties(),
                  SchemaChange::setProperty,
                  SchemaChange::removeProperty,
                  SchemaChange[]::new);
          Schema alteredSchema = loadedCatalog.asSchemas().alterSchema(schemaName, changes);
          Optional.ofNullable(alteredSchema.properties()).ifPresent(response::setProperties);
          return response;
        default:
          throw new IllegalArgumentException("Unknown mode: " + mode);
      }
    } catch (NoSuchSchemaException e) {
      // Schema does not exist, create it
      Schema createdSchema = loadedCatalog.asSchemas().createSchema(schemaName, null, properties);
      response.setProperties(
          createdSchema.properties() == null ? Maps.newHashMap() : createdSchema.properties());
      return response;
    }
  }

  private DropNamespaceResponse dropCatalog(
      String catalogName,
      DropNamespaceRequest.ModeEnum mode,
      DropNamespaceRequest.BehaviorEnum behavior) {
    try {
      boolean dropped =
          client.dropCatalog(catalogName, behavior == DropNamespaceRequest.BehaviorEnum.CASCADE);
      if (dropped) {
        return new DropNamespaceResponse();
      } else {
        // Catalog did not exist
        if (mode == DropNamespaceRequest.ModeEnum.FAIL) {
          throw LanceNamespaceException.notFound(
              "Catalog not found: " + catalogName,
              NoSuchCatalogException.class.getSimpleName(),
              catalogName,
              CommonUtil.formatCurrentStackTrace());
        }
        return new DropNamespaceResponse(); // SKIP mode
      }
    } catch (NonEmptyCatalogException e) {
      throw LanceNamespaceException.badRequest(
          String.format("Catalog %s is not empty.", catalogName),
          NonEmptyCatalogException.class.getSimpleName(),
          catalogName,
          CommonUtil.formatCurrentStackTrace());
    }
  }

  private DropNamespaceResponse dropSchema(
      String catalogName,
      String schemaName,
      DropNamespaceRequest.ModeEnum mode,
      DropNamespaceRequest.BehaviorEnum behavior) {
    try {
      boolean dropped =
          client
              .loadCatalog(catalogName)
              .asSchemas()
              .dropSchema(schemaName, behavior == DropNamespaceRequest.BehaviorEnum.CASCADE);
      if (dropped) {
        return new DropNamespaceResponse();
      } else {
        // Schema did not exist
        if (mode == DropNamespaceRequest.ModeEnum.FAIL) {
          throw LanceNamespaceException.notFound(
              "Schema not found: " + schemaName,
              NoSuchSchemaException.class.getSimpleName(),
              schemaName,
              CommonUtil.formatCurrentStackTrace());
        }
        return new DropNamespaceResponse(); // SKIP mode
      }
    } catch (NoSuchCatalogException e) {
      throw LanceNamespaceException.notFound(
          "Catalog not found: " + catalogName,
          NoSuchCatalogException.class.getSimpleName(),
          catalogName,
          CommonUtil.formatCurrentStackTrace());
    } catch (NonEmptySchemaException e) {
      throw LanceNamespaceException.badRequest(
          String.format("Schema %s is not empty.", schemaName),
          NonEmptySchemaException.class.getSimpleName(),
          schemaName,
          CommonUtil.formatCurrentStackTrace());
    }
  }

  private <T> T[] buildChanges(
      Map<String, String> newProps,
      Map<String, String> oldProps,
      BiFunction<String, String, T> setPropertyFunc,
      Function<String, T> removePropertyFunc,
      IntFunction<T[]> arrayCreator) {
    Stream<T> setPropertiesStream =
        newProps.entrySet().stream()
            .map(entry -> setPropertyFunc.apply(entry.getKey(), entry.getValue()));

    Stream<T> removePropertiesStream =
        oldProps == null
            ? Stream.empty()
            : oldProps.keySet().stream()
                .filter(key -> !newProps.containsKey(key))
                .map(removePropertyFunc);

    return Stream.concat(setPropertiesStream, removePropertiesStream).toArray(arrayCreator);
  }
}
