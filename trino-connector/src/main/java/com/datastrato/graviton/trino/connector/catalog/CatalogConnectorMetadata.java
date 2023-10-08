/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.catalog;

import com.datastrato.graviton.Catalog;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.client.GravitonMetaLake;
import com.datastrato.graviton.rel.Schema;
import com.datastrato.graviton.rel.SupportsSchemas;
import com.datastrato.graviton.rel.Table;
import com.datastrato.graviton.rel.TableCatalog;
import com.datastrato.graviton.trino.connector.metadata.GravitonSchema;
import com.datastrato.graviton.trino.connector.metadata.GravitonTable;
import io.airlift.log.Logger;
import java.util.Arrays;
import java.util.List;

/** This class implements graviton metadata operators. */
public class CatalogConnectorMetadata {

  private static final Logger LOG = Logger.get(CatalogConnectorMetadata.class);

  private final GravitonMetaLake metalake;
  private final Catalog catalog;
  private final String catalogName;

  public CatalogConnectorMetadata(GravitonMetaLake metalake, NameIdentifier catalogIdentifier) {
    this.catalogName = catalogIdentifier.name();
    this.metalake = metalake;
    catalog = metalake.loadCatalog(catalogIdentifier);
  }

  public List<String> listSchemaNames() {
    SupportsSchemas schemas = catalog.asSchemas();
    return Arrays.stream(schemas.listSchemas(Namespace.ofSchema(metalake.name(), catalogName)))
        .map(NameIdentifier::name)
        .toList();
  }

  public GravitonSchema getSchema(String schemaName) {
    SupportsSchemas schemaCatalog = catalog.asSchemas();
    Schema schema =
        schemaCatalog.loadSchema(NameIdentifier.ofSchema(metalake.name(), catalogName, schemaName));
    return new GravitonSchema(schema);
  }

  public GravitonTable getTable(String schemaName, String tableName) {
    TableCatalog tableCatalog = catalog.asTableCatalog();
    Table table =
        tableCatalog.loadTable(
            NameIdentifier.ofTable(metalake.name(), catalogName, schemaName, tableName));
    return new GravitonTable(schemaName, table);
  }

  public List<String> listTables(String schemaName) {
    TableCatalog tableCatalog = catalog.asTableCatalog();
    NameIdentifier[] tables =
        tableCatalog.listTables(Namespace.ofTable(metalake.name(), catalogName, schemaName));
    return Arrays.stream(tables).map(NameIdentifier::name).toList();
  }

  public boolean tableExists(String schemaName, String tableName) {
    return catalog
        .asTableCatalog()
        .tableExists(NameIdentifier.ofTable(metalake.name(), catalogName, schemaName, tableName));
  }
}
