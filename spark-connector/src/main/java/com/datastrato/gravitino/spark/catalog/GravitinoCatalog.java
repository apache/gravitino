/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.catalog;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.spark.GravitinoSparkConfig;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import javax.ws.rs.NotSupportedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog;
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

/**
 * GravitinoCatalog is the class registered to Spark CatalogManager, it's lazy load means Spark
 * connector loads GravitinoCatalog when it's used. It will create different Spark Table for
 * different Gravitino catalog.
 */
public class GravitinoCatalog implements TableCatalog, SupportsNamespaces {
  protected TableCatalog sparkCatalog;
  protected Catalog gravitinoCatalog;

  private String metalakeName;
  private String catalogName;
  private GravitinoCatalogManager gravitinoCatalogManager;

  public GravitinoCatalog() {
    gravitinoCatalogManager = GravitinoCatalogManager.get();
    metalakeName = gravitinoCatalogManager.getMetalakeName();
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    throw new NotSupportedException("Doesn't support listing table");
  }

  @Override
  public Table createTable(
      Identifier ident, Column[] columns, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    throw new NotSupportedException("Doesn't support creating table");
  }

  // Will create a catalog specific table by invoking createSparkTable()
  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    throw new NotSupportedException("Doesn't support loading table");
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
    throw new NotSupportedException("Doesn't support altering table");
  }

  @Override
  public boolean dropTable(Identifier ident) {
    throw new NotSupportedException("Doesn't support drop table");
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException {
    throw new NotSupportedException("Doesn't support renaming table");
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;
    this.gravitinoCatalog = gravitinoCatalogManager.getGravitinoCatalogInfo(name);
    String provider = gravitinoCatalog.provider();
    Preconditions.checkArgument(provider != null, name + " catalog provider is null");
    sparkCatalog = createAndInitSparkCatalog(provider, name, options);
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
        "Doesn't support listing namespaces with " + String.join(".", namespace));
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
      String comment = schema.comment();
      Map<String, String> properties = schema.properties();
      if (comment != null) {
        properties = new HashMap<>(schema.properties());
        properties.put(SupportsNamespaces.PROP_COMMENT, comment);
      }
      return properties;
    } catch (NoSuchSchemaException e) {
      throw new NoSuchNamespaceException(namespace);
    }
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata)
      throws NamespaceAlreadyExistsException {
    valiateNamespace(namespace);
    Map<String, String> properties = new HashMap<>(metadata);
    String comment = properties.remove(SupportsNamespaces.PROP_COMMENT);
    try {
      gravitinoCatalog
          .asSchemas()
          .createSchema(
              NameIdentifier.of(metalakeName, catalogName, namespace[0]), comment, properties);
    } catch (SchemaAlreadyExistsException e) {
      throw new NamespaceAlreadyExistsException(namespace);
    }
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes)
      throws NoSuchNamespaceException {
    throw new NotSupportedException("Doesn't support altering namespace");
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade)
      throws NoSuchNamespaceException, NonEmptyNamespaceException {
    valiateNamespace(namespace);
    try {
      return gravitinoCatalog
          .asSchemas()
          .dropSchema(NameIdentifier.of(metalakeName, catalogName, namespace[0]), cascade);
    } catch (NonEmptySchemaException e) {
      throw new NonEmptyNamespaceException(namespace);
    }
  }

  // Create a internal catalog to do IO operations.
  private TableCatalog createAndInitSparkCatalog(
      String provider, String name, CaseInsensitiveStringMap options) {
    switch (provider.toLowerCase(Locale.ROOT)) {
      case "hive":
        return createAndInitHiveCatalog(name, options);
      default:
        throw new NotSupportedException("Not support catalog: " + name + ", provider: " + provider);
    }
  }

  private TableCatalog createAndInitHiveCatalog(String name, CaseInsensitiveStringMap options) {
    Preconditions.checkArgument(
        gravitinoCatalog.properties() != null, "Hive Catalog properties should not be null");
    String metastoreUri =
        gravitinoCatalog.properties().get(GravitinoSparkConfig.GRAVITINO_HIVE_METASTORE_URI);
    Preconditions.checkArgument(
        StringUtils.isNoneBlank(metastoreUri),
        "Couldn't get "
            + GravitinoSparkConfig.GRAVITINO_HIVE_METASTORE_URI
            + " from hive catalog properties");

    TableCatalog hiveCatalog = new HiveTableCatalog();
    HashMap all = new HashMap(options);
    all.put(GravitinoSparkConfig.SPARK_HIVE_METASTORE_URI, metastoreUri);
    hiveCatalog.initialize(name, new CaseInsensitiveStringMap(all));

    return hiveCatalog;
  }

  private void valiateNamespace(String[] namespace) {
    Preconditions.checkArgument(
        namespace.length == 1,
        "Doesn't support multi level namespaces: " + String.join(".", namespace));
  }
}
