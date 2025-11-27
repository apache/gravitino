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

import static org.apache.gravitino.lance.common.ops.gravitino.LanceDataTypeConverter.CONVERTER;
import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_LOCATION;
import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.lancedb.lance.namespace.LanceNamespaceException;
import com.lancedb.lance.namespace.ObjectIdentifier;
import com.lancedb.lance.namespace.model.CreateEmptyTableResponse;
import com.lancedb.lance.namespace.model.CreateTableRequest;
import com.lancedb.lance.namespace.model.CreateTableRequest.ModeEnum;
import com.lancedb.lance.namespace.model.CreateTableResponse;
import com.lancedb.lance.namespace.model.DeregisterTableResponse;
import com.lancedb.lance.namespace.model.DescribeTableResponse;
import com.lancedb.lance.namespace.model.DropTableResponse;
import com.lancedb.lance.namespace.model.JsonArrowSchema;
import com.lancedb.lance.namespace.model.RegisterTableRequest;
import com.lancedb.lance.namespace.model.RegisterTableResponse;
import com.lancedb.lance.namespace.util.CommonUtil;
import com.lancedb.lance.namespace.util.JsonArrowSchemaConverter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.lance.common.ops.LanceTableOperations;
import org.apache.gravitino.lance.common.utils.ArrowUtils;
import org.apache.gravitino.lance.common.utils.LancePropertiesUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitinoLanceTableOperations implements LanceTableOperations {
  private static final Logger LOG = LoggerFactory.getLogger(GravitinoLanceTableOperations.class);

  private final GravitinoLanceNamespaceWrapper namespaceWrapper;

  public GravitinoLanceTableOperations(GravitinoLanceNamespaceWrapper namespaceWrapper) {
    this.namespaceWrapper = namespaceWrapper;
  }

  @Override
  public DescribeTableResponse describeTable(
      String tableId, String delimiter, Optional<Long> version) {
    if (!version.isEmpty()) {
      throw new UnsupportedOperationException(
          "Describing specific table version is not supported. It should be null to indicate the"
              + " latest version.");
    }

    ObjectIdentifier nsId = ObjectIdentifier.of(tableId, Pattern.quote(delimiter));
    Preconditions.checkArgument(
        nsId.levels() == 3, "Expected at 3-level namespace but got: %s", nsId.levels());

    String catalogName = nsId.levelAtListPos(0);
    Catalog catalog = namespaceWrapper.loadAndValidateLakehouseCatalog(catalogName);
    NameIdentifier tableIdentifier =
        NameIdentifier.of(nsId.levelAtListPos(1), nsId.levelAtListPos(2));

    Table table = catalog.asTableCatalog().loadTable(tableIdentifier);
    DescribeTableResponse response = new DescribeTableResponse();
    response.setProperties(table.properties());
    response.setLocation(table.properties().get(LANCE_LOCATION));
    response.setSchema(toJsonArrowSchema(table.columns()));
    response.setVersion(null);
    response.setStorageOptions(LancePropertiesUtils.getLanceStorageOptions(table.properties()));
    return response;
  }

  @Override
  public CreateTableResponse createTable(
      String tableId,
      CreateTableRequest.ModeEnum mode,
      String delimiter,
      String tableLocation,
      Map<String, String> tableProperties,
      byte[] arrowStreamBody) {
    ObjectIdentifier nsId = ObjectIdentifier.of(tableId, Pattern.quote(delimiter));
    Preconditions.checkArgument(
        nsId.levels() == 3, "Expected at 3-level namespace but got: %s", nsId.levels());

    // Parser column information.
    List<Column> columns = Lists.newArrayList();
    if (arrowStreamBody != null) {
      org.apache.arrow.vector.types.pojo.Schema schema =
          ArrowUtils.parseArrowIpcStream(arrowStreamBody);
      columns = extractColumns(schema);
    }

    String catalogName = nsId.levelAtListPos(0);
    Catalog catalog = namespaceWrapper.loadAndValidateLakehouseCatalog(catalogName);

    NameIdentifier tableIdentifier =
        NameIdentifier.of(nsId.levelAtListPos(1), nsId.levelAtListPos(2));

    Map<String, String> createTableProperties = Maps.newHashMap(tableProperties);
    if (tableLocation != null) {
      createTableProperties.put(LANCE_LOCATION, tableLocation);
    }
    // The format is defined in GenericLakehouseCatalog
    createTableProperties.put(Table.PROPERTY_TABLE_FORMAT, "lance");
    createTableProperties.put(Table.PROPERTY_EXTERNAL, "true");

    Table t;
    try {
      t =
          catalog
              .asTableCatalog()
              .createTable(
                  tableIdentifier, columns.toArray(new Column[0]), null, createTableProperties);
    } catch (TableAlreadyExistsException exception) {
      if (mode == CreateTableRequest.ModeEnum.CREATE) {
        throw LanceNamespaceException.conflict(
            "Table already exists: " + tableId,
            TableAlreadyExistsException.class.getSimpleName(),
            tableId,
            CommonUtil.formatCurrentStackTrace());
      } else if (mode == CreateTableRequest.ModeEnum.OVERWRITE) {
        LOG.info("Overwriting existing table: {}", tableId);
        catalog.asTableCatalog().purgeTable(tableIdentifier);

        t =
            catalog
                .asTableCatalog()
                .createTable(
                    tableIdentifier, columns.toArray(new Column[0]), null, createTableProperties);
      } else { // EXIST_OK
        CreateTableResponse response = new CreateTableResponse();
        Table existingTable = catalog.asTableCatalog().loadTable(tableIdentifier);
        response.setProperties(existingTable.properties());
        response.setLocation(existingTable.properties().get(LANCE_LOCATION));
        response.setVersion(null);
        response.setStorageOptions(
            LancePropertiesUtils.getLanceStorageOptions(existingTable.properties()));
        return response;
      }
    }

    CreateTableResponse response = new CreateTableResponse();
    response.setProperties(t.properties());
    response.setLocation(tableLocation);
    // Extract storage options from table properties. All storage options stores in table
    // properties.
    response.setStorageOptions(LancePropertiesUtils.getLanceStorageOptions(t.properties()));
    response.setVersion(null);
    response.setLocation(t.properties().get(LANCE_LOCATION));
    response.setProperties(t.properties());
    return response;
  }

  @Override
  public CreateEmptyTableResponse createEmptyTable(
      String tableId, String delimiter, String tableLocation, Map<String, String> tableProperties) {
    CreateTableResponse response =
        createTable(tableId, ModeEnum.CREATE, delimiter, tableLocation, tableProperties, null);
    CreateEmptyTableResponse emptyTableResponse = new CreateEmptyTableResponse();
    emptyTableResponse.setProperties(response.getProperties());
    emptyTableResponse.setLocation(response.getLocation());
    emptyTableResponse.setStorageOptions(response.getStorageOptions());
    return emptyTableResponse;
  }

  @Override
  public RegisterTableResponse registerTable(
      String tableId,
      RegisterTableRequest.ModeEnum mode,
      String delimiter,
      Map<String, String> tableProperties) {
    ObjectIdentifier nsId = ObjectIdentifier.of(tableId, Pattern.quote(delimiter));
    Preconditions.checkArgument(
        nsId.levels() == 3, "Expected at 3-level namespace but got: %s", nsId.levels());

    String catalogName = nsId.levelAtListPos(0);
    Catalog catalog = namespaceWrapper.loadAndValidateLakehouseCatalog(catalogName);
    NameIdentifier tableIdentifier =
        NameIdentifier.of(nsId.levelAtListPos(1), nsId.levelAtListPos(2));

    Map<String, String> copiedTableProperties = Maps.newHashMap(tableProperties);
    copiedTableProperties.put("format", "lance");
    Table t = null;
    try {
      t =
          catalog
              .asTableCatalog()
              .createTable(tableIdentifier, new Column[] {}, null, copiedTableProperties);
    } catch (TableAlreadyExistsException exception) {
      if (mode == RegisterTableRequest.ModeEnum.CREATE) {
        throw LanceNamespaceException.conflict(
            "Table already exists: " + tableId,
            TableAlreadyExistsException.class.getSimpleName(),
            tableId,
            CommonUtil.formatCurrentStackTrace());
      } else if (mode == RegisterTableRequest.ModeEnum.OVERWRITE) {
        LOG.info("Overwriting existing table: {}", tableId);
        catalog.asTableCatalog().dropTable(tableIdentifier);

        t =
            catalog
                .asTableCatalog()
                .createTable(tableIdentifier, new Column[] {}, null, copiedTableProperties);
      }
    }

    RegisterTableResponse response = new RegisterTableResponse();
    response.setProperties(t.properties());
    response.setLocation(t.properties().get(LANCE_LOCATION));
    return response;
  }

  @Override
  public DeregisterTableResponse deregisterTable(String tableId, String delimiter) {
    ObjectIdentifier nsId = ObjectIdentifier.of(tableId, Pattern.quote(delimiter));
    Preconditions.checkArgument(
        nsId.levels() == 3, "Expected at 3-level namespace but got: %s", nsId.levels());

    String catalogName = nsId.levelAtListPos(0);
    Catalog catalog = namespaceWrapper.loadAndValidateLakehouseCatalog(catalogName);

    NameIdentifier tableIdentifier =
        NameIdentifier.of(nsId.levelAtListPos(1), nsId.levelAtListPos(2));
    Table t = catalog.asTableCatalog().loadTable(tableIdentifier);
    Map<String, String> properties = t.properties();
    // TODO Support real deregister API.
    boolean result = catalog.asTableCatalog().dropTable(tableIdentifier);
    if (!result) {
      throw LanceNamespaceException.notFound(
          "Table not found: " + tableId,
          NoSuchTableException.class.getSimpleName(),
          tableId,
          CommonUtil.formatCurrentStackTrace());
    }

    DeregisterTableResponse response = new DeregisterTableResponse();
    response.setProperties(properties);
    response.setLocation(properties.get(LANCE_LOCATION));
    response.setId(nsId.listStyleId());
    return response;
  }

  @Override
  public boolean tableExists(String tableId, String delimiter) {
    ObjectIdentifier nsId = ObjectIdentifier.of(tableId, Pattern.quote(delimiter));
    Preconditions.checkArgument(
        nsId.levels() == 3, "Expected at 3-level namespace but got: %s", nsId.levels());

    String catalogName = nsId.levelAtListPos(0);
    Catalog catalog = namespaceWrapper.loadAndValidateLakehouseCatalog(catalogName);

    NameIdentifier tableIdentifier =
        NameIdentifier.of(nsId.levelAtListPos(1), nsId.levelAtListPos(2));

    return catalog.asTableCatalog().tableExists(tableIdentifier);
  }

  @Override
  public DropTableResponse dropTable(String tableId, String delimiter) {
    ObjectIdentifier nsId = ObjectIdentifier.of(tableId, Pattern.quote(delimiter));
    Preconditions.checkArgument(
        nsId.levels() == 3, "Expected at 3-level namespace but got: %s", nsId.levels());

    String catalogName = nsId.levelAtListPos(0);
    Catalog catalog = namespaceWrapper.loadAndValidateLakehouseCatalog(catalogName);

    NameIdentifier tableIdentifier =
        NameIdentifier.of(nsId.levelAtListPos(1), nsId.levelAtListPos(2));

    Table table;
    try {
      table = catalog.asTableCatalog().loadTable(tableIdentifier);
    } catch (NoSuchTableException e) {
      throw LanceNamespaceException.notFound(
          "Table not found: " + tableId,
          NoSuchTableException.class.getSimpleName(),
          tableId,
          CommonUtil.formatCurrentStackTrace());
    }

    boolean deleted = catalog.asTableCatalog().purgeTable(tableIdentifier);
    if (!deleted) {
      throw LanceNamespaceException.notFound(
          "Table not found: " + tableId,
          NoSuchTableException.class.getSimpleName(),
          tableId,
          CommonUtil.formatCurrentStackTrace());
    }

    DropTableResponse response = new DropTableResponse();
    response.setId(nsId.listStyleId());
    response.setLocation(table.properties().get(LANCE_LOCATION));
    response.setProperties(table.properties());
    // TODO Support transaction ids later
    response.setTransactionId(List.of());

    return response;
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

  private JsonArrowSchema toJsonArrowSchema(Column[] columns) {
    List<Field> fields =
        Arrays.stream(columns)
            .map(col -> CONVERTER.toArrowField(col.name(), col.dataType(), col.nullable()))
            .collect(Collectors.toList());

    return JsonArrowSchemaConverter.convertToJsonArrowSchema(
        new org.apache.arrow.vector.types.pojo.Schema(fields));
  }
}
