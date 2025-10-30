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
import static org.apache.gravitino.lance.common.ops.gravitino.LanceDataTypeConverter.CONVERTER;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.lancedb.lance.namespace.LanceNamespaceException;
import com.lancedb.lance.namespace.ObjectIdentifier;
import com.lancedb.lance.namespace.model.CreateNamespaceRequest;
import com.lancedb.lance.namespace.model.CreateNamespaceRequest.ModeEnum;
import com.lancedb.lance.namespace.model.CreateNamespaceResponse;
import com.lancedb.lance.namespace.model.CreateTableResponse;
import com.lancedb.lance.namespace.model.DescribeNamespaceResponse;
import com.lancedb.lance.namespace.model.DescribeTableResponse;
import com.lancedb.lance.namespace.model.DropNamespaceRequest;
import com.lancedb.lance.namespace.model.DropNamespaceResponse;
import com.lancedb.lance.namespace.model.JsonArrowSchema;
import com.lancedb.lance.namespace.model.ListNamespacesResponse;
import com.lancedb.lance.namespace.model.ListTablesResponse;
import com.lancedb.lance.namespace.util.CommonUtil;
import com.lancedb.lance.namespace.util.JsonArrowSchemaConverter;
import com.lancedb.lance.namespace.util.PageUtil;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
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
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitinoLanceNamespaceWrapper extends NamespaceWrapper
    implements LanceNamespaceOperations, LanceTableOperations {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoLanceNamespaceWrapper.class);
  private GravitinoClient client;

  @VisibleForTesting
  GravitinoLanceNamespaceWrapper() {
    super(null);
  }

  public GravitinoLanceNamespaceWrapper(LanceConfig config) {
    super(config);
  }

  @Override
  protected void initialize() {
    String uri = config().get(NAMESPACE_BACKEND_URI);
    String metalakeName = config().get(METALAKE_NAME);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metalakeName),
        "Metalake name must be provided for Lance Gravitino namespace backend");

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

    Catalog catalog = loadAndValidateLakehouseCatalog(nsId.levelAtListPos(0));
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
      String namespaceId,
      String delimiter,
      CreateNamespaceRequest.ModeEnum mode,
      Map<String, String> properties) {
    ObjectIdentifier nsId = ObjectIdentifier.of(namespaceId, delimiter);
    Preconditions.checkArgument(
        nsId.levels() <= 2 && nsId.levels() > 0,
        "Expected at most 2-level and at least 1-level namespace but got: %s",
        namespaceId);

    switch (nsId.levels()) {
      case 1:
        return createOrUpdateCatalog(nsId.levelAtListPos(0), mode, properties);
      case 2:
        return createOrUpdateSchema(
            nsId.levelAtListPos(0), nsId.levelAtListPos(1), mode, properties);
      default:
        throw new IllegalArgumentException(
            "Expected at most 2-level and at least 1-level namespace but got: " + namespaceId);
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
        nsId.levels() <= 2 && nsId.levels() > 0,
        "Expected at most 2-level and at least 1-level namespace but got: %s",
        namespaceId);

    switch (nsId.levels()) {
      case 1:
        return dropCatalog(nsId.levelAtListPos(0), mode, behavior);
      case 2:
        return dropSchema(nsId.levelAtListPos(0), nsId.levelAtListPos(1), mode, behavior);
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

    Catalog catalog = loadAndValidateLakehouseCatalog(nsId.levelAtListPos(0));
    if (nsId.levels() == 2) {
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
          "Catalog is not a lakehouse catalog: " + catalogName,
          NoSuchCatalogException.class.getSimpleName(),
          catalogName,
          CommonUtil.formatCurrentStackTrace());
    }
    return catalog;
  }

  private CreateNamespaceResponse createOrUpdateCatalog(
      String catalogName, CreateNamespaceRequest.ModeEnum mode, Map<String, String> properties) {
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
              "generic-lakehouse",
              "created by Lance REST server",
              properties);
      response.setProperties(
          createdCatalog.properties() == null ? Maps.newHashMap() : createdCatalog.properties());
      return response;
    }

    // Catalog exists, validate type
    if (!isLakehouseCatalog(catalog)) {
      throw LanceNamespaceException.conflict(
          "Catalog already exists but is not a lakehouse catalog: " + catalogName,
          CatalogAlreadyExistsException.class.getSimpleName(),
          catalogName,
          CommonUtil.formatCurrentStackTrace());
    }

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
  }

  private CreateNamespaceResponse createOrUpdateSchema(
      String catalogName,
      String schemaName,
      CreateNamespaceRequest.ModeEnum mode,
      Map<String, String> properties) {
    CreateNamespaceResponse response = new CreateNamespaceResponse();
    Catalog loadedCatalog = loadAndValidateLakehouseCatalog(catalogName);

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

  @Override
  public ListTablesResponse listTables(
      String namespaceId, String delimiter, String pageToken, Integer limit) {
    ObjectIdentifier nsId = ObjectIdentifier.of(namespaceId, Pattern.quote(delimiter));
    Preconditions.checkArgument(
        nsId.levels() <= 2, "Expected at most 2-level namespace but got: %s", nsId.levels());
    String catalogName = nsId.levelAtListPos(0);
    Catalog catalog = loadAndValidateLakehouseCatalog(catalogName);
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

  @Override
  public DescribeTableResponse describeTable(String tableId, String delimiter) {
    ObjectIdentifier nsId = ObjectIdentifier.of(tableId, Pattern.quote(delimiter));
    Preconditions.checkArgument(
        nsId.levels() <= 3, "Expected at most 3-level namespace but got: %s", nsId.levels());

    String catalogName = nsId.levelAtListPos(0);
    Catalog catalog = loadAndValidateLakehouseCatalog(catalogName);
    NameIdentifier tableIdentifier =
        NameIdentifier.of(nsId.levelAtListPos(1), nsId.levelAtListPos(2));

    Table table = catalog.asTableCatalog().loadTable(tableIdentifier);
    DescribeTableResponse response = new DescribeTableResponse();
    response.setProperties(table.properties());
    response.setLocation(table.properties().get("location"));
    response.setSchema(toJsonArrowSchema(table.columns()));
    return response;
  }

  @Override
  public CreateTableResponse createTable(
      String tableId,
      String mode,
      String delimiter,
      String tableLocation,
      Map<String, String> tableProperties,
      String rootCatalog,
      byte[] arrowStreamBody) {
    ObjectIdentifier nsId = ObjectIdentifier.of(tableId, Pattern.quote(delimiter));
    Preconditions.checkArgument(
        nsId.levels() <= 3, "Expected at most 3-level namespace but got: %s", nsId.levels());
    if (rootCatalog != null) {
      List<String> levels = nsId.listStyleId();
      List<String> newLevels = Lists.newArrayList(rootCatalog);
      newLevels.addAll(levels);
      nsId = ObjectIdentifier.of(newLevels);
    }

    // Parser column information.
    List<Column> columns = Lists.newArrayList();
    if (arrowStreamBody != null) {
      org.apache.arrow.vector.types.pojo.Schema schema = parseArrowIpcStream(arrowStreamBody);
      columns = extractColumns(schema);
    }

    String catalogName = nsId.levelAtListPos(0);
    Catalog catalog = loadAndValidateLakehouseCatalog(catalogName);

    NameIdentifier tableIdentifier =
        NameIdentifier.of(nsId.levelAtListPos(1), nsId.levelAtListPos(2));

    Map<String, String> createTableProperties = Maps.newHashMap(tableProperties);
    createTableProperties.put("location", tableLocation);
    createTableProperties.put("mode", mode);
    // TODO considering the mode (create, exist_ok, overwrite)

    ModeEnum createMode = ModeEnum.fromValue(mode.toLowerCase());
    switch (createMode) {
      case EXIST_OK:
        if (catalog.asTableCatalog().tableExists(tableIdentifier)) {
          CreateTableResponse response = new CreateTableResponse();
          Table existingTable = catalog.asTableCatalog().loadTable(tableIdentifier);
          response.setProperties(existingTable.properties());
          response.setLocation(existingTable.properties().get("location"));
          response.setVersion(0L);
          return response;
        }
        break;
      case CREATE:
        if (catalog.asTableCatalog().tableExists(tableIdentifier)) {
          throw LanceNamespaceException.conflict(
              "Table already exists: " + tableId,
              SchemaAlreadyExistsException.class.getSimpleName(),
              tableId,
              CommonUtil.formatCurrentStackTrace());
        }
        break;
      case OVERWRITE:
        if (catalog.asTableCatalog().tableExists(tableIdentifier)) {
          catalog.asTableCatalog().dropTable(tableIdentifier);
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown mode: " + mode);
    }

    Table t =
        catalog
            .asTableCatalog()
            .createTable(
                tableIdentifier,
                columns.toArray(new Column[0]),
                tableLocation,
                createTableProperties);

    CreateTableResponse response = new CreateTableResponse();
    response.setProperties(t.properties());
    response.setLocation(tableLocation);
    response.setVersion(0L);
    return response;
  }

  private JsonArrowSchema toJsonArrowSchema(Column[] columns) {
    List<Field> fields =
        Arrays.stream(columns)
            .map(col -> CONVERTER.toArrowField(col.name(), col.dataType(), col.nullable()))
            .collect(Collectors.toList());

    return JsonArrowSchemaConverter.convertToJsonArrowSchema(
        new org.apache.arrow.vector.types.pojo.Schema(fields));
  }

  @VisibleForTesting
  org.apache.arrow.vector.types.pojo.Schema parseArrowIpcStream(byte[] stream) {
    org.apache.arrow.vector.types.pojo.Schema schema;

    try (BufferAllocator allocator = new RootAllocator();
        ByteArrayInputStream bais = new ByteArrayInputStream(stream);
        ArrowStreamReader reader = new ArrowStreamReader(bais, allocator)) {
      schema = reader.getVectorSchemaRoot().getSchema();
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse Arrow IPC stream", e);
    }

    Preconditions.checkArgument(schema != null, "No schema found in Arrow IPC stream");
    return schema;
  }

  private List<Column> extractColumns(org.apache.arrow.vector.types.pojo.Schema arrowSchema) {
    List<Column> columns = new ArrayList<>();

    for (org.apache.arrow.vector.types.pojo.Field field : arrowSchema.getFields()) {
      columns.add(
          Column.of(
              field.getName(),
              CONVERTER.toGravitino(field),
              null,
              field.isNullable(),
              false,
              DEFAULT_VALUE_NOT_SET));
    }
    return columns;
  }
}
