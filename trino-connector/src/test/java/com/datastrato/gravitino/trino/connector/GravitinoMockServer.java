/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import static java.util.Collections.emptyMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.client.GravitinoClient;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SupportsSchemas;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableCatalog;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorManager;
import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoColumn;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoSchema;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoTable;
import com.datastrato.gravitino.trino.connector.util.DataTypeTransformer;
import com.google.common.base.Preconditions;
import io.trino.plugin.memory.MemoryConnector;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.ResourcePresence;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class GravitinoMockServer implements AutoCloseable {
  private final String testMetalake = "test";
  private final String testCatalog = "memory";
  private final String testCatalogPrivate = "memory";

  private boolean start = true;
  private CatalogConnectorManager catalogConnectorManager;

  public void setCatalogConnectorManager(CatalogConnectorManager catalogConnectorManager) {
    this.catalogConnectorManager = catalogConnectorManager;
  }

  public GravitinoClient createGravitinoClient() {
    GravitinoClient client = mock(GravitinoClient.class);

    when(client.loadMetalake(any(NameIdentifier.class)))
        .thenAnswer(
            new Answer<GravitinoMetaLake>() {
              @Override
              public GravitinoMetaLake answer(InvocationOnMock invocation) throws Throwable {
                NameIdentifier metalakeName = invocation.getArgument(0);
                if (!metalakeName.name().equals(testMetalake)) {
                  throw new NoSuchMetalakeException("metalake does not be found");
                }
                return createGravitinoMetalake(metalakeName);
              }
            });

    return client;
  }

  private GravitinoMetaLake createGravitinoMetalake(NameIdentifier metalakeName) {
    GravitinoMetaLake metaLake = mock(GravitinoMetaLake.class);
    when(metaLake.name()).thenReturn(metalakeName.name());
    when(metaLake.listCatalogs(any()))
        .thenReturn(new NameIdentifier[] {NameIdentifier.ofCatalog(testMetalake, testCatalog)});

    when(metaLake.loadCatalog(any(NameIdentifier.class)))
        .thenAnswer(
            new Answer<Catalog>() {
              @Override
              public Catalog answer(InvocationOnMock invocation) throws Throwable {
                NameIdentifier catalogName = invocation.getArgument(0);
                if (!catalogName.name().equals(testCatalog)) {
                  throw new NoSuchCatalogException("catalog does not be found");
                }

                return createGravitinoCatalog(catalogName);
              }
            });
    return metaLake;
  }

  private Catalog createGravitinoCatalog(NameIdentifier catalogName) {
    Catalog catalog = mock(Catalog.class);
    when(catalog.name()).thenReturn(catalogName.name());
    when(catalog.provider()).thenReturn(testCatalogPrivate);

    when(catalog.asTableCatalog()).thenAnswer(answer -> createTableCatalog(catalogName));

    when(catalog.asSchemas()).thenAnswer(answer -> createSchemas(catalogName));
    return catalog;
  }

  private SupportsSchemas createSchemas(NameIdentifier catalogName) {
    SupportsSchemas schemas = mock(SupportsSchemas.class);
    when(schemas.createSchema(any(NameIdentifier.class), anyString(), anyMap()))
        .thenAnswer(
            new Answer<Schema>() {
              @Override
              public Schema answer(InvocationOnMock invocation) throws Throwable {
                NameIdentifier schemaName = invocation.getArgument(0);
                Map<String, String> properties = invocation.getArgument(2);

                // create schema
                MemoryConnector memoryConnector =
                    (MemoryConnector)
                        catalogConnectorManager
                            .getCatalogConnector(catalogName.toString())
                            .getInternalConnector();
                ConnectorMetadata metadata = memoryConnector.getMetadata(null, null);

                CatalogConnectorMetadataAdapter metadataAdapter =
                    catalogConnectorManager
                        .getCatalogConnector(catalogName.toString())
                        .getMetadataAdapter();
                GravitinoSchema schema = new GravitinoSchema(schemaName.name(), properties, "");
                metadata.createSchema(null, schemaName.name(), emptyMap(), null);
                return schema.getSchemaDTO();
              }
            });

    when(schemas.dropSchema(any(NameIdentifier.class), anyBoolean()))
        .thenAnswer(
            new Answer<Boolean>() {
              @Override
              public Boolean answer(InvocationOnMock invocation) throws Throwable {
                NameIdentifier nameIdentifier = invocation.getArgument(0);
                boolean cascade = invocation.getArgument(1);

                // drop schema,
                MemoryConnector memoryConnector =
                    (MemoryConnector)
                        catalogConnectorManager
                            .getCatalogConnector(catalogName.toString())
                            .getInternalConnector();
                ConnectorMetadata metadata = memoryConnector.getMetadata(null, null);
                metadata.dropSchema(null, nameIdentifier.name(), cascade);
                return true;
              }
            });

    when(schemas.listSchemas(any(Namespace.class)))
        .thenAnswer(
            new Answer<NameIdentifier[]>() {
              @Override
              public NameIdentifier[] answer(InvocationOnMock invocation) throws Throwable {
                Namespace namespace = invocation.getArgument(0);
                MemoryConnector memoryConnector =
                    (MemoryConnector)
                        catalogConnectorManager
                            .getCatalogConnector(catalogName.toString())
                            .getInternalConnector();
                ConnectorMetadata metadata = memoryConnector.getMetadata(null, null);
                return metadata.listSchemaNames(null).stream()
                    .map(
                        schemaName ->
                            NameIdentifier.ofSchema(
                                namespace.level(0), namespace.level(1), schemaName))
                    .toArray(NameIdentifier[]::new);
              }
            });

    when(schemas.loadSchema(any(NameIdentifier.class)))
        .thenAnswer(
            new Answer<Schema>() {
              @Override
              public Schema answer(InvocationOnMock invocation) throws Throwable {
                NameIdentifier schemaName = invocation.getArgument(0);
                MemoryConnector memoryConnector =
                    (MemoryConnector)
                        catalogConnectorManager
                            .getCatalogConnector(catalogName.toString())
                            .getInternalConnector();
                memoryConnector.getMetadata(null, null);
                ConnectorMetadata metadata = memoryConnector.getMetadata(null, null);
                Map<String, Object> schemaProperties =
                    metadata.getSchemaProperties(null, schemaName.name());

                CatalogConnectorMetadataAdapter metadataAdapter =
                    catalogConnectorManager
                        .getCatalogConnector(catalogName.toString())
                        .getMetadataAdapter();

                GravitinoSchema gravitinoSchema =
                    new GravitinoSchema(
                        schemaName.name(),
                        metadataAdapter.toGravitinoSchemaProperties(schemaProperties),
                        "");
                return gravitinoSchema.getSchemaDTO();
              }
            });
    return schemas;
  }

  private TableCatalog createTableCatalog(NameIdentifier catalogName) {
    TableCatalog tableCatalog = mock(TableCatalog.class);
    when(tableCatalog.createTable(
            any(NameIdentifier.class), any(Column[].class), anyString(), anyMap()))
        .thenAnswer(
            new Answer<Table>() {
              @Override
              public Table answer(InvocationOnMock invocation) throws Throwable {
                NameIdentifier nameIdentifier = invocation.getArgument(0);
                TableName tableName = new TableName(nameIdentifier);
                Column[] columns = invocation.getArgument(1);
                String comment = invocation.getArgument(2);
                Map<String, String> properties = invocation.getArgument(3);

                GravitinoTable gravitinoTable =
                    new GravitinoTable(
                        tableName.schema(), tableName.table(), columns, comment, properties);
                CatalogConnectorMetadataAdapter metadataAdapter =
                    catalogConnectorManager
                        .getCatalogConnector(catalogName.toString())
                        .getMetadataAdapter();
                ConnectorTableMetadata tableMetadata =
                    metadataAdapter.getTableMetadata(gravitinoTable);

                MemoryConnector memoryConnector =
                    (MemoryConnector)
                        catalogConnectorManager
                            .getCatalogConnector(catalogName.toString())
                            .getInternalConnector();
                ConnectorMetadata metadata = memoryConnector.getMetadata(null, null);
                metadata.createTable(null, tableMetadata, false);
                return null;
              }
            });

    when(tableCatalog.dropTable(any(NameIdentifier.class)))
        .thenAnswer(
            new Answer<Boolean>() {
              @Override
              public Boolean answer(InvocationOnMock invocation) throws Throwable {
                NameIdentifier nameIdentifier = invocation.getArgument(0);
                TableName tableName = new TableName(nameIdentifier);
                // todo
                MemoryConnector memoryConnector =
                    (MemoryConnector)
                        catalogConnectorManager
                            .getCatalogConnector(catalogName.toString())
                            .getInternalConnector();
                memoryConnector.getMetadata(null, null);
                ConnectorMetadata metadata = memoryConnector.getMetadata(null, null);
                ConnectorTableHandle tableHandle =
                    metadata.getTableHandle(
                        null,
                        new SchemaTableName(tableName.schema(), tableName.table()),
                        Optional.empty(),
                        Optional.empty());
                metadata.dropTable(null, tableHandle);
                return true;
              }
            });

    when(tableCatalog.listTables(any(Namespace.class)))
        .thenAnswer(
            new Answer<NameIdentifier[]>() {
              @Override
              public NameIdentifier[] answer(InvocationOnMock invocation) throws Throwable {
                Namespace schemaName = invocation.getArgument(0);
                MemoryConnector memoryConnector =
                    (MemoryConnector)
                        catalogConnectorManager
                            .getCatalogConnector(catalogName.toString())
                            .getInternalConnector();
                ConnectorMetadata metadata = memoryConnector.getMetadata(null, null);
                ArrayList<NameIdentifier> tableNames = new ArrayList<>();
                for (SchemaTableName tableName : metadata.listTables(null, Optional.empty())) {
                  tableNames.add(
                      NameIdentifier.ofTable(
                          schemaName.level(0),
                          schemaName.level(1),
                          schemaName.level(2),
                          tableName.getTableName()));
                }
                return tableNames.toArray(new NameIdentifier[tableNames.size()]);
              }
            });

    when(tableCatalog.tableExists(any()))
        .thenAnswer(
            new Answer<Boolean>() {
              @Override
              public Boolean answer(InvocationOnMock invocation) throws Throwable {
                NameIdentifier nameIdentifier = invocation.getArgument(0);
                TableName tableName = new TableName(nameIdentifier);
                MemoryConnector memoryConnector =
                    (MemoryConnector)
                        catalogConnectorManager
                            .getCatalogConnector(catalogName.toString())
                            .getInternalConnector();
                ConnectorMetadata metadata = memoryConnector.getMetadata(null, null);
                return metadata.getTableHandle(
                        null,
                        new SchemaTableName(tableName.schema(), tableName.table()),
                        Optional.empty(),
                        Optional.empty())
                    != null;
              }
            });

    when(tableCatalog.loadTable(any()))
        .thenAnswer(
            new Answer<Table>() {
              @Override
              public Table answer(InvocationOnMock invocation) throws Throwable {
                NameIdentifier nameIdentifier = invocation.getArgument(0);
                TableName tableName = new TableName(nameIdentifier);
                MemoryConnector memoryConnector =
                    (MemoryConnector)
                        catalogConnectorManager
                            .getCatalogConnector(catalogName.toString())
                            .getInternalConnector();

                ConnectorMetadata metadata = memoryConnector.getMetadata(null, null);
                ConnectorTableHandle tableHandle =
                    metadata.getTableHandle(
                        null,
                        new SchemaTableName(tableName.schema(), tableName.table()),
                        Optional.empty(),
                        Optional.empty());
                ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(null, tableHandle);

                CatalogConnectorMetadataAdapter metadataAdapter =
                    catalogConnectorManager
                        .getCatalogConnector(catalogName.toString())
                        .getMetadataAdapter();
                GravitinoTable gravitinoTable = metadataAdapter.createTable(tableMetadata);
                return gravitinoTable.getTableDTO();
              }
            });

    when(tableCatalog.alterTable(any(NameIdentifier.class), any(TableChange[].class)))
        .thenAnswer(
            new Answer<Table>() {
              @Override
              public Table answer(InvocationOnMock invocation) throws Throwable {
                NameIdentifier nameIdentifier = invocation.getArgument(0);
                TableName tableName = new TableName(nameIdentifier);

                MemoryConnector memoryConnector =
                    (MemoryConnector)
                        catalogConnectorManager
                            .getCatalogConnector(catalogName.toString())
                            .getInternalConnector();
                ConnectorMetadata metadata = memoryConnector.getMetadata(null, null);
                ConnectorTableHandle tableHandle =
                    metadata.getTableHandle(
                        null,
                        new SchemaTableName(tableName.schema(), tableName.table()),
                        Optional.empty(),
                        Optional.empty());

                for (int i = 1; i < invocation.getArguments().length; i++) {
                  TableChange tableChange = invocation.getArgument(i);
                  doAlterTable(tableChange, tableHandle, tableName, metadata, catalogName);
                }
                return null;
              }
            });
    return tableCatalog;
  }

  void doAlterTable(
      TableChange tableChange,
      ConnectorTableHandle tableHandle,
      TableName tableName,
      ConnectorMetadata metadata,
      NameIdentifier catalogName) {
    if (tableChange instanceof TableChange.RenameTable) {
      TableChange.RenameTable renameTable = (TableChange.RenameTable) tableChange;
      metadata.renameTable(
          null, tableHandle, new SchemaTableName(tableName.schema(), renameTable.getNewName()));

    } else if (tableChange instanceof TableChange.AddColumn) {
      TableChange.AddColumn addColumn = (TableChange.AddColumn) tableChange;
      String fieldName = addColumn.fieldNames()[0];
      GravitinoColumn column =
          new GravitinoColumn(fieldName, addColumn.getDataType(), -1, "", true);
      CatalogConnectorMetadataAdapter metadataAdapter =
          catalogConnectorManager.getCatalogConnector(catalogName.toString()).getMetadataAdapter();
      metadata.addColumn(null, tableHandle, metadataAdapter.getColumnMetadata(column));

    } else if (tableChange instanceof TableChange.DeleteColumn) {
      TableChange.DeleteColumn deleteColumn = (TableChange.DeleteColumn) tableChange;
      String fieldName = deleteColumn.fieldNames()[0];
      ColumnHandle columnHandle = metadata.getColumnHandles(null, tableHandle).get(fieldName);
      metadata.dropColumn(null, tableHandle, columnHandle);

    } else if (tableChange instanceof TableChange.RenameColumn) {
      TableChange.RenameColumn renameColumn = (TableChange.RenameColumn) tableChange;
      String fieldName = renameColumn.fieldNames()[0];
      ColumnHandle columnHandle = metadata.getColumnHandles(null, tableHandle).get(fieldName);
      metadata.renameColumn(null, tableHandle, columnHandle, renameColumn.getNewName());

    } else if (tableChange instanceof TableChange.UpdateColumnType) {
      TableChange.UpdateColumnType updateColumnType = (TableChange.UpdateColumnType) tableChange;
      String fieldName = updateColumnType.fieldNames()[0];
      ColumnHandle columnHandle = metadata.getColumnHandles(null, tableHandle).get(fieldName);
      metadata.setColumnType(
          null,
          tableHandle,
          columnHandle,
          DataTypeTransformer.getTrinoType(updateColumnType.getNewDataType()));

    } else if (tableChange instanceof TableChange.UpdateComment) {
      TableChange.UpdateComment updateComment = (TableChange.UpdateComment) tableChange;
      metadata.setTableComment(null, tableHandle, Optional.of(updateComment.getNewComment()));

    } else if (tableChange instanceof TableChange.UpdateColumnComment) {
      TableChange.UpdateColumnComment updateColumnComment =
          (TableChange.UpdateColumnComment) tableChange;
      ColumnHandle columnHandle =
          metadata.getColumnHandles(null, tableHandle).get(updateColumnComment.fieldNames()[0]);
      metadata.setColumnComment(
          null, tableHandle, columnHandle, Optional.of(updateColumnComment.getNewComment()));

    } else if (tableChange instanceof TableChange.SetProperty) {
      TableChange.SetProperty setProperty = (TableChange.SetProperty) tableChange;
      metadata.setTableProperties(
          null,
          tableHandle,
          Map.of(setProperty.getProperty(), Optional.of(setProperty.getValue())));
    }
  }

  @ResourcePresence
  public boolean isRunning() {
    return start;
  }

  @Override
  public void close() {
    start = false;
  }

  static class TableName {
    NameIdentifier nameIdentifier;

    TableName(NameIdentifier nameIdentifier) {
      Preconditions.checkArgument(
          nameIdentifier.namespace().length() == 3,
          "Not a table nameIdentifier: " + nameIdentifier);
      this.nameIdentifier = nameIdentifier;
    }

    String schema() {
      return nameIdentifier.namespace().level(2);
    }

    String table() {
      return nameIdentifier.name();
    }
  }
}
