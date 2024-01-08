/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.catalog;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.spark.TypeConverter;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.NotSupportedException;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public abstract class GravitinoCatalog implements TableCatalog, SupportsNamespaces {
  private String metalakeName;
  private String catalogName;
  private GravitinoCatalogManager gravitinoCatalogManager;
  protected TableCatalog sparkCatalog;
  private Catalog gravitinoCatalog;

  public GravitinoCatalog() {
    gravitinoCatalogManager = GravitinoCatalogManager.getGravitinoCatalogManager();
    metalakeName = gravitinoCatalogManager.getMetalakeName();
  }

  abstract Table createGravitinoTable(
      Identifier identifier, com.datastrato.gravitino.rel.Table gravitinoTable);

  abstract TableCatalog createSparkCatalog();

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    return new Identifier[0];
  }

  public Table createTable(
      Identifier ident, Column[] columns, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    NameIdentifier nameIdentifier =
        NameIdentifier.of(metalakeName, catalogName, getDatabase(ident), ident.name());
    ColumnDTO[] gravitinoColumns =
        Arrays.stream(columns)
            .map(column -> createGravitinoColumn(column))
            .toArray(ColumnDTO[]::new);

    Map<String, String> gravitinoProperties = new HashMap<>();
    gravitinoProperties.putAll(properties);
    String comment = gravitinoProperties.remove("comment");
    if (comment == null) {
      comment = "";
    }

    try {
      com.datastrato.gravitino.rel.Table table =
          gravitinoCatalog
              .asTableCatalog()
              .createTable(nameIdentifier, gravitinoColumns, comment, gravitinoProperties);
      return createGravitinoTable(ident, table);
    } catch (NoSuchSchemaException e) {
      throw new NoSuchNamespaceException(ident.namespace());
    } catch (com.datastrato.gravitino.exceptions.TableAlreadyExistsException e) {
      throw new TableAlreadyExistsException(ident);
    }
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    try {
      com.datastrato.gravitino.rel.Table table =
          gravitinoCatalog
              .asTableCatalog()
              .loadTable(
                  NameIdentifier.of(metalakeName, catalogName, getDatabase(ident), ident.name()));
      return createGravitinoTable(ident, table);
    } catch (com.datastrato.gravitino.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }
  }

  @SuppressWarnings("deprecation")
  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    throw new NotSupportedException("Deprecated create table method");
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    throw new NotSupportedException("Not support Alter table");
  }

  @Override
  public boolean dropTable(Identifier ident) {
    return gravitinoCatalog
        .asTableCatalog()
        .dropTable(NameIdentifier.of(metalakeName, catalogName, getDatabase(ident), ident.name()));
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException {
    throw new NotSupportedException("Not support Alter table");
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;
    gravitinoCatalog = gravitinoCatalogManager.getGravitinoCatalogInfo(name);
    sparkCatalog = createSparkCatalog();
    sparkCatalog.initialize(name, options);
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public String[][] listNamespaces() throws NoSuchNamespaceException {
    NameIdentifier[] schemas =
        gravitinoCatalog.asSchemas().listSchemas(Namespace.of(metalakeName, catalogName));
    return Arrays.stream(schemas)
        .map(schema -> new String[] {schema.name()})
        .toArray(String[][]::new);
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    if (namespace.length == 0) {
      return listNamespaces();
    }
    throw new NotSupportedException(
        "Not support list namespaces with " + String.join(".", namespace));
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    valiateNamespace(namespace);
    try {
      Schema schema =
          gravitinoCatalog
              .asSchemas()
              .loadSchema(NameIdentifier.of(metalakeName, catalogName, namespace[0]));
      return schema.properties();
    } catch (NoSuchSchemaException e) {
      throw new NoSuchNamespaceException(namespace);
    }
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata)
      throws NamespaceAlreadyExistsException {
    valiateNamespace(namespace);
    try {
      gravitinoCatalog
          .asSchemas()
          .createSchema(NameIdentifier.of(metalakeName, catalogName, namespace[0]), null, metadata);
    } catch (SchemaAlreadyExistsException e) {
      throw new NamespaceAlreadyExistsException(namespace);
    }
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes)
      throws NoSuchNamespaceException {}

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade)
      throws NoSuchNamespaceException, NonEmptyNamespaceException {
    return false;
  }

  private String getDatabase(Identifier ident) {
    String database = "default";
    if (ident.namespace().length > 0) {
      database = ident.namespace()[0];
    }
    return database;
  }

  private void valiateNamespace(String[] namespace) {
    Preconditions.checkArgument(
        namespace.length == 1,
        "Unsupported multi level " + "namespaces:" + String.join(".", namespace));
  }

  private ColumnDTO createGravitinoColumn(Column sparkColumn) {
    return ColumnDTO.builder()
        .withName(sparkColumn.name())
        .withDataType(TypeConverter.convert(sparkColumn.dataType()))
        .withNullable(sparkColumn.nullable())
        .withComment(sparkColumn.comment())
        .build();
  }
}
