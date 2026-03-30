/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.lance.common.ops.gravitino;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.CatalogInUseException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NonEmptyCatalogException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.lance.common.ops.LanceNamespaceOperations;
import org.lance.namespace.errors.InvalidInputException;
import org.lance.namespace.errors.LanceNamespaceException;
import org.lance.namespace.errors.NamespaceAlreadyExistsException;
import org.lance.namespace.errors.NamespaceNotFoundException;
import org.lance.namespace.model.CreateNamespaceResponse;
import org.lance.namespace.model.DescribeNamespaceResponse;
import org.lance.namespace.model.DropNamespaceResponse;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesResponse;

public class GravitinoLanceNameSpaceOperations implements LanceNamespaceOperations {

  private final GravitinoLanceNamespaceWrapper namespaceWrapper;
  private final GravitinoClient client;

  private enum CreateMode {
    CREATE,
    EXIST_OK,
    OVERWRITE
  }

  private enum DropMode {
    FAIL,
    SKIP
  }

  private enum DropBehavior {
    RESTRICT,
    CASCADE
  }

  public GravitinoLanceNameSpaceOperations(GravitinoLanceNamespaceWrapper namespaceWrapper) {
    this.namespaceWrapper = namespaceWrapper;
    this.client = namespaceWrapper.getClient();
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
                .filter(namespaceWrapper::isLakehouseCatalog)
                .map(Catalog::name)
                .collect(Collectors.toList());
        break;

      case 1:
        Catalog catalog = namespaceWrapper.loadAndValidateLakehouseCatalog(nsId.levelAtListPos(0));
        namespaces = Lists.newArrayList(catalog.asSchemas().listSchemas());
        break;

      case 2:
        namespaces = Lists.newArrayList();
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
        nsId.levels() <= 2 && nsId.levels() > 0,
        "Expected at most 2-level and at least 1-level namespace but got: %s",
        namespaceId);

    Catalog catalog = namespaceWrapper.loadAndValidateLakehouseCatalog(nsId.levelAtListPos(0));
    Map<String, String> properties = Maps.newHashMap();

    switch (nsId.levels()) {
      case 1:
        Optional.ofNullable(catalog.properties()).ifPresent(properties::putAll);
        break;
      case 2:
        String schemaName = nsId.levelAtListPos(1);
        Schema schema = catalog.asSchemas().loadSchema(schemaName);
        Optional.ofNullable(schema.properties()).ifPresent(properties::putAll);
        break;
      default:
        throw new IllegalArgumentException(
            "Expected at most 2-level and at least 1-level namespace but got: " + namespaceId);
    }

    DescribeNamespaceResponse response = new DescribeNamespaceResponse();
    response.setProperties(properties);
    return response;
  }

  @Override
  public CreateNamespaceResponse createNamespace(
      String namespaceId, String delimiter, String mode, Map<String, String> properties) {
    ObjectIdentifier nsId = ObjectIdentifier.of(namespaceId, delimiter);
    Preconditions.checkArgument(
        nsId.levels() <= 2 && nsId.levels() > 0,
        "Expected at most 2-level and at least 1-level namespace but got: %s",
        namespaceId);

    switch (nsId.levels()) {
      case 1:
        return createOrUpdateCatalog(
            nsId.levelAtListPos(0), parseCreateMode(namespaceId, mode), properties);
      case 2:
        return createOrUpdateSchema(
            nsId.levelAtListPos(0),
            nsId.levelAtListPos(1),
            parseCreateMode(namespaceId, mode),
            properties);
      default:
        throw new IllegalArgumentException(
            "Expected at most 2-level and at least 1-level namespace but got: " + namespaceId);
    }
  }

  @Override
  public DropNamespaceResponse dropNamespace(
      String namespaceId, String delimiter, String mode, String behavior) {
    ObjectIdentifier nsId = ObjectIdentifier.of(namespaceId, delimiter);
    Preconditions.checkArgument(
        nsId.levels() <= 2 && nsId.levels() > 0,
        "Expected at most 2-level and at least 1-level namespace but got: %s",
        namespaceId);

    switch (nsId.levels()) {
      case 1:
        return dropCatalog(
            nsId.levelAtListPos(0),
            parseDropMode(namespaceId, mode),
            parseDropBehavior(namespaceId, behavior));
      case 2:
        return dropSchema(
            nsId.levelAtListPos(0),
            nsId.levelAtListPos(1),
            parseDropMode(namespaceId, mode),
            parseDropBehavior(namespaceId, behavior));
      default:
        throw new IllegalArgumentException(
            "Expected at most 2-level and at least 1-level namespace but got: " + namespaceId);
    }
  }

  @Override
  public void namespaceExists(String namespaceId, String delimiter) throws LanceNamespaceException {
    ObjectIdentifier nsId = ObjectIdentifier.of(namespaceId, delimiter);
    Preconditions.checkArgument(
        nsId.levels() <= 2 && nsId.levels() > 0,
        "Expected at most 2-level and at least 1-level namespace but got: %s",
        namespaceId);

    Catalog catalog = namespaceWrapper.loadAndValidateLakehouseCatalog(nsId.levelAtListPos(0));
    if (nsId.levels() == 2) {
      String schemaName = nsId.levelAtListPos(1);
      if (!catalog.asSchemas().schemaExists(schemaName)) {
        throw new NamespaceNotFoundException(
            "Schema not found: " + schemaName, CommonUtil.formatCurrentStackTrace(), schemaName);
      }
    }
  }

  private CreateNamespaceResponse createOrUpdateCatalog(
      String catalogName, CreateMode mode, Map<String, String> properties) {
    CreateNamespaceResponse response = new CreateNamespaceResponse();

    Catalog catalog;
    try {
      catalog = client.loadCatalog(catalogName);
    } catch (NoSuchCatalogException e) {
      // Catalog does not exist, create it
      Catalog createdCatalog =
          client.createCatalog(
              catalogName,
              Catalog.Type.RELATIONAL,
              "lakehouse-generic",
              "created by Lance REST server",
              properties);
      response.setProperties(
          createdCatalog.properties() == null ? Maps.newHashMap() : createdCatalog.properties());
      return response;
    }

    // Catalog exists, validate type
    if (!namespaceWrapper.isLakehouseCatalog(catalog)) {
      throw new NamespaceAlreadyExistsException(
          "Catalog already exists but is not a lakehouse catalog: " + catalogName,
          CommonUtil.formatCurrentStackTrace(),
          catalogName);
    }

    // Catalog exists, handle based on mode
    switch (mode) {
      case EXIST_OK:
        response.setProperties(
            Optional.ofNullable(catalog.properties()).orElse(Collections.emptyMap()));
        return response;
      case CREATE:
        throw new NamespaceAlreadyExistsException(
            "Catalog already exists: " + catalogName,
            CommonUtil.formatCurrentStackTrace(),
            catalogName);
      case OVERWRITE:
        CatalogChange[] changes =
            buildChanges(
                properties,
                removeInUseProperty(catalog.properties()),
                CatalogChange::setProperty,
                CatalogChange::removeProperty,
                CatalogChange[]::new);
        Catalog alteredCatalog = client.alterCatalog(catalogName, changes);
        Optional.ofNullable(alteredCatalog.properties()).ifPresent(response::setProperties);
        return response;
      default:
        throw new IllegalArgumentException("Unknown mode: " + mode);
    }
  }

  private Map<String, String> removeInUseProperty(Map<String, String> properties) {
    return properties.entrySet().stream()
        .filter(e -> !e.getKey().equalsIgnoreCase(Catalog.PROPERTY_IN_USE))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private CreateNamespaceResponse createOrUpdateSchema(
      String catalogName, String schemaName, CreateMode mode, Map<String, String> properties) {
    CreateNamespaceResponse response = new CreateNamespaceResponse();
    Catalog loadedCatalog = namespaceWrapper.loadAndValidateLakehouseCatalog(catalogName);

    Schema schema;
    try {
      schema = loadedCatalog.asSchemas().loadSchema(schemaName);
    } catch (NoSuchSchemaException e) {
      // Schema does not exist, create it
      Schema createdSchema = loadedCatalog.asSchemas().createSchema(schemaName, null, properties);
      response.setProperties(
          createdSchema.properties() == null ? Maps.newHashMap() : createdSchema.properties());
      return response;
    }

    // Schema exists, handle based on mode
    switch (mode) {
      case EXIST_OK:
        response.setProperties(
            Optional.ofNullable(schema.properties()).orElse(Collections.emptyMap()));
        return response;
      case CREATE:
        throw new NamespaceAlreadyExistsException(
            "Schema already exists: " + schemaName,
            CommonUtil.formatCurrentStackTrace(),
            schemaName);
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
  }

  private DropNamespaceResponse dropCatalog(
      String catalogName, DropMode mode, DropBehavior behavior) {
    try {
      boolean dropped = client.dropCatalog(catalogName, behavior == DropBehavior.CASCADE);
      if (dropped) {
        return new DropNamespaceResponse();
      } else {
        // Catalog did not exist
        if (mode == DropMode.FAIL) {
          throw new NamespaceNotFoundException(
              "Catalog not found: " + catalogName,
              CommonUtil.formatCurrentStackTrace(),
              catalogName);
        }
        return new DropNamespaceResponse(); // SKIP mode
      }
    } catch (NonEmptyCatalogException e) {
      throw new InvalidInputException(
          String.format("Catalog %s is not empty", catalogName),
          CommonUtil.formatCurrentStackTrace(),
          catalogName);
    } catch (CatalogInUseException e) {
      throw new InvalidInputException(
          String.format("Catalog %s is in use", catalogName),
          CommonUtil.formatCurrentStackTrace(),
          catalogName);
    }
  }

  private DropNamespaceResponse dropSchema(
      String catalogName, String schemaName, DropMode mode, DropBehavior behavior) {
    try {
      boolean dropped =
          client
              .loadCatalog(catalogName)
              .asSchemas()
              .dropSchema(schemaName, behavior == DropBehavior.CASCADE);
      if (dropped) {
        return new DropNamespaceResponse();
      } else {
        // Schema did not exist
        if (mode == DropMode.FAIL) {
          throw new NamespaceNotFoundException(
              "Schema not found: " + schemaName, CommonUtil.formatCurrentStackTrace(), schemaName);
        }
        return new DropNamespaceResponse(); // SKIP mode
      }
    } catch (NoSuchCatalogException e) {
      throw new NamespaceNotFoundException(
          "Catalog not found: " + catalogName, CommonUtil.formatCurrentStackTrace(), catalogName);
    } catch (NonEmptySchemaException e) {
      throw new InvalidInputException(
          String.format("Schema %s is not empty.", schemaName),
          CommonUtil.formatCurrentStackTrace(),
          schemaName);
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

  private static CreateMode parseCreateMode(String instance, String mode) {
    if (mode == null) {
      return CreateMode.CREATE;
    }
    String normalized = normalizeToken(mode);
    if ("CREATE".equals(normalized)) {
      return CreateMode.CREATE;
    }
    if ("EXISTOK".equals(normalized)) {
      return CreateMode.EXIST_OK;
    }
    if ("OVERWRITE".equals(normalized)) {
      return CreateMode.OVERWRITE;
    }
    throw new InvalidInputException(
        "Unknown create namespace mode: " + mode, CommonUtil.formatCurrentStackTrace(), instance);
  }

  private static DropMode parseDropMode(String instance, String mode) {
    if (mode == null) {
      return DropMode.FAIL;
    }
    String normalized = normalizeToken(mode);
    if ("FAIL".equals(normalized)) {
      return DropMode.FAIL;
    }
    if ("SKIP".equals(normalized)) {
      return DropMode.SKIP;
    }
    throw new InvalidInputException(
        "Unknown drop namespace mode: " + mode, CommonUtil.formatCurrentStackTrace(), instance);
  }

  private static DropBehavior parseDropBehavior(String instance, String behavior) {
    if (behavior == null) {
      return DropBehavior.RESTRICT;
    }
    String normalized = normalizeToken(behavior);
    if ("RESTRICT".equals(normalized)) {
      return DropBehavior.RESTRICT;
    }
    if ("CASCADE".equals(normalized)) {
      return DropBehavior.CASCADE;
    }
    throw new InvalidInputException(
        "Unknown drop namespace behavior: " + behavior,
        CommonUtil.formatCurrentStackTrace(),
        instance);
  }

  private static String normalizeToken(String value) {
    return value.replaceAll("[^A-Za-z0-9]", "").toUpperCase(Locale.ROOT);
  }

  @Override
  public ListTablesResponse listTables(
      String namespaceId, String delimiter, String pageToken, Integer limit) {
    ObjectIdentifier nsId = ObjectIdentifier.of(namespaceId, Pattern.quote(delimiter));
    Preconditions.checkArgument(
        nsId.levels() == 2, "Expected 2-level namespace but got: %s", nsId.levels());
    String catalogName = nsId.levelAtListPos(0);
    Catalog catalog = namespaceWrapper.loadAndValidateLakehouseCatalog(catalogName);
    String schemaName = nsId.levelAtListPos(1);
    List<String> tables =
        Arrays.stream(catalog.asTableCatalog().listTables(Namespace.of(schemaName)))
            .map(ident -> Joiner.on(delimiter).join(catalogName, schemaName, ident.name()))
            .sorted()
            .collect(Collectors.toList());

    PageUtil.Page page = PageUtil.splitPage(tables, pageToken, PageUtil.normalizePageSize(limit));
    ListNamespacesResponse response = new ListNamespacesResponse();
    response.setNamespaces(Sets.newHashSet(page.items()));
    response.setPageToken(page.nextPageToken());

    return new ListTablesResponse()
        .tables(response.getNamespaces())
        .pageToken(response.getPageToken());
  }
}
