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
import java.util.Optional;
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
 * GravitinoCatalog is the class registered to Spark CatalogManager, it's lazy loaded which means
 * Spark connector loads GravitinoCatalog when it's used. It will create different Spark Tables from
 * different Gravitino catalogs.
 */
public class GravitinoCatalog implements TableCatalog, SupportsNamespaces {
  // The specific Spark catalog to do IO operations, different catalogs have different spark catalog
  // implementations, like HiveTableCatalog for Hive, JDBCTableCatalog for JDBC, SparkCatalog for
  // Iceberg.
  protected TableCatalog sparkCatalog;
  // The Gravitino catalog client to do schema operations.
  protected Catalog gravitinoCatalogClient;

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
    this.gravitinoCatalogClient = gravitinoCatalogManager.getGravitinoCatalogInfo(name);
    String provider = gravitinoCatalogClient.provider();
    Preconditions.checkArgument(provider != null, name + " catalog provider is null");
    this.sparkCatalog = createAndInitSparkCatalog(provider, name, options);
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public String[][] listNamespaces() throws NoSuchNamespaceException {
    NameIdentifier[] schemas =
        gravitinoCatalogClient.asSchemas().listSchemas(Namespace.of(metalakeName, catalogName));
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
          gravitinoCatalogClient
              .asSchemas()
              .loadSchema(NameIdentifier.of(metalakeName, catalogName, namespace[0]));
      String comment = schema.comment();
      Map<String, String> properties = schema.properties();
      if (comment != null) {
        Map<String, String> propertiesWithComment =
            new HashMap<>(Optional.ofNullable(properties).orElse(new HashMap<>()));
        propertiesWithComment.put(SupportsNamespaces.PROP_COMMENT, comment);
        return propertiesWithComment;
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
      gravitinoCatalogClient
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
      return gravitinoCatalogClient
          .asSchemas()
          .dropSchema(NameIdentifier.of(metalakeName, catalogName, namespace[0]), cascade);
    } catch (NonEmptySchemaException e) {
      throw new NonEmptyNamespaceException(namespace);
    }
  }

  // Create specific Spark catalogs for different Gravitino catalog providers, mainly used to do IO
  // operations.
  private TableCatalog createAndInitSparkCatalog(
      String provider, String name, CaseInsensitiveStringMap options) {
    switch (provider.toLowerCase(Locale.ROOT)) {
      case "hive":
        return createAndInitSparkHiveCatalog(name, options);
      default:
        throw new NotSupportedException("Not support catalog: " + name + ", provider: " + provider);
    }
  }

  private TableCatalog createAndInitSparkHiveCatalog(
      String name, CaseInsensitiveStringMap options) {
    Preconditions.checkArgument(
        gravitinoCatalogClient.properties() != null, "Hive Catalog properties should not be null");
    String metastoreUri =
        gravitinoCatalogClient.properties().get(GravitinoSparkConfig.GRAVITINO_HIVE_METASTORE_URI);
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
