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
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.spark.GravitinoSparkConfig;
import com.datastrato.gravitino.spark.ConnectorConstants;
import com.datastrato.gravitino.spark.GravitinoCatalogAdaptor;
import com.datastrato.gravitino.spark.GravitinoCatalogAdaptorFactory;
import com.datastrato.gravitino.spark.PropertiesConverter;
import com.datastrato.gravitino.spark.SparkTypeConverter;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.ws.rs.NotSupportedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.NamespaceChange.SetProperty;
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
  protected PropertiesConverter propertiesConverter;

  private final String metalakeName;
  private String catalogName;
  private final GravitinoCatalogManager gravitinoCatalogManager;
  // Different catalog use GravitinoCatalogAdaptor to adapt to GravitinoCatalog
  private GravitinoCatalogAdaptor gravitinoAdaptor;

  public GravitinoCatalog() {
    gravitinoCatalogManager = GravitinoCatalogManager.get();
    metalakeName = gravitinoCatalogManager.getMetalakeName();
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;
    this.gravitinoCatalogClient = gravitinoCatalogManager.getGravitinoCatalogInfo(name);
    String provider = gravitinoCatalogClient.provider();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(provider), name + " catalog provider is empty");
    this.gravitinoAdaptor = GravitinoCatalogAdaptorFactory.createGravitinoAdaptor(provider);
    this.sparkCatalog =
        gravitinoAdaptor.createAndInitSparkCatalog(
            name, options, gravitinoCatalogClient.properties());
    this.propertiesConverter = gravitinoAdaptor.getPropertiesConverter();
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    String gravitinoNamespace;
    if (namespace.length == 0) {
      gravitinoNamespace = getCatalogDefaultNamespace();
    } else {
      validateNamespace(namespace);
      gravitinoNamespace = namespace[0];
    }
    try {
      NameIdentifier[] identifiers =
          gravitinoCatalogClient
              .asTableCatalog()
              .listTables(Namespace.of(metalakeName, catalogName, gravitinoNamespace));
      return Arrays.stream(identifiers)
          .map(
              identifier ->
                  Identifier.of(new String[] {getDatabase(identifier)}, identifier.name()))
          .toArray(Identifier[]::new);
    } catch (NoSuchSchemaException e) {
      throw new NoSuchNamespaceException(namespace);
    }
  }

  @Override
  public Table createTable(
      Identifier ident, Column[] columns, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    NameIdentifier gravitinoIdentifier =
        NameIdentifier.of(metalakeName, catalogName, getDatabase(ident), ident.name());
    com.datastrato.gravitino.rel.Column[] gravitinoColumns =
        Arrays.stream(columns)
            .map(column -> createGravitinoColumn(column))
            .toArray(com.datastrato.gravitino.rel.Column[]::new);

    Map<String, String> gravitinoProperties =
        propertiesConverter.toGravitinoTableProperties(properties);
    // Spark store comment in properties, we should retrieve it and pass to Gravitino explicitly.
    String comment = gravitinoProperties.remove(ConnectorConstants.COMMENT);

    try {
      com.datastrato.gravitino.rel.Table table =
          gravitinoCatalogClient
              .asTableCatalog()
              .createTable(gravitinoIdentifier, gravitinoColumns, comment, gravitinoProperties);
      return gravitinoAdaptor.createSparkTable(ident, table, sparkCatalog, propertiesConverter);
    } catch (NoSuchSchemaException e) {
      throw new NoSuchNamespaceException(ident.namespace());
    } catch (com.datastrato.gravitino.exceptions.TableAlreadyExistsException e) {
      throw new TableAlreadyExistsException(ident);
    }
  }

  // Will create a catalog specific table
  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    try {
      String database = getDatabase(ident);
      com.datastrato.gravitino.rel.Table table =
          gravitinoCatalogClient
              .asTableCatalog()
              .loadTable(NameIdentifier.of(metalakeName, catalogName, database, ident.name()));
      return gravitinoAdaptor.createSparkTable(ident, table, sparkCatalog, propertiesConverter);
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
    throw new NotSupportedException("Doesn't support altering table for now");
  }

  @Override
  public boolean dropTable(Identifier ident) {
    return gravitinoCatalogClient
        .asTableCatalog()
        .dropTable(NameIdentifier.of(metalakeName, catalogName, getDatabase(ident), ident.name()));
  }

  @Override
  public boolean purgeTable(Identifier ident) {
    return gravitinoCatalogClient
        .asTableCatalog()
        .purgeTable(NameIdentifier.of(metalakeName, catalogName, getDatabase(ident), ident.name()));
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException {
    String oldDatabase = getDatabase(oldIdent);
    String newDatabase = getDatabase(newIdent);
    Preconditions.checkArgument(
        newDatabase.equals(oldDatabase), "Doesn't support rename table to different database");
    com.datastrato.gravitino.rel.TableChange rename =
        com.datastrato.gravitino.rel.TableChange.rename(newIdent.name());
    try {
      gravitinoCatalogClient
          .asTableCatalog()
          .alterTable(
              NameIdentifier.of(metalakeName, catalogName, getDatabase(oldIdent), oldIdent.name()),
              rename);
    } catch (com.datastrato.gravitino.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(oldIdent);
    }
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
    Preconditions.checkArgument(
        namespace.length == 0,
        "Doesn't support listing namespaces with " + String.join(".", namespace));
    return listNamespaces();
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    validateNamespace(namespace);
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
    validateNamespace(namespace);
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
    validateNamespace(namespace);
    SchemaChange[] schemaChanges =
        Arrays.stream(changes)
            .map(
                change -> {
                  if (change instanceof SetProperty) {
                    SetProperty setProperty = ((SetProperty) change);
                    return SchemaChange.setProperty(setProperty.property(), setProperty.value());
                  } else {
                    throw new UnsupportedOperationException(
                        String.format(
                            "Unsupported namespace change %s", change.getClass().getName()));
                  }
                })
            .toArray(SchemaChange[]::new);
    try {
      gravitinoCatalogClient
          .asSchemas()
          .alterSchema(NameIdentifier.of(metalakeName, catalogName, namespace[0]), schemaChanges);
    } catch (NoSuchSchemaException e) {
      throw new NoSuchNamespaceException(namespace);
    }
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade)
      throws NoSuchNamespaceException, NonEmptyNamespaceException {
    validateNamespace(namespace);
    try {
      return gravitinoCatalogClient
          .asSchemas()
          .dropSchema(NameIdentifier.of(metalakeName, catalogName, namespace[0]), cascade);
    } catch (NonEmptySchemaException e) {
      throw new NonEmptyNamespaceException(namespace);
    }
  }

  private void validateNamespace(String[] namespace) {
    Preconditions.checkArgument(
        namespace.length == 1,
        "Doesn't support multi level namespaces: " + String.join(".", namespace));
  }

  private String getCatalogDefaultNamespace() {
    String[] catalogDefaultNamespace = sparkCatalog.defaultNamespace();
    Preconditions.checkArgument(
        catalogDefaultNamespace != null && catalogDefaultNamespace.length == 1,
        "Catalog default namespace is not valid");
    return catalogDefaultNamespace[0];
  }

  private com.datastrato.gravitino.rel.Column createGravitinoColumn(Column sparkColumn) {
    return com.datastrato.gravitino.rel.Column.of(
        sparkColumn.name(),
        SparkTypeConverter.toGravitinoType(sparkColumn.dataType()),
        sparkColumn.comment(),
        sparkColumn.nullable(),
        // Spark doesn't support autoIncrement
        false,
        // todo: support default value
        com.datastrato.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET);
  }

  private String getDatabase(Identifier sparkIdentifier) {
    if (sparkIdentifier.namespace().length > 0) {
      return sparkIdentifier.namespace()[0];
    }
    return getCatalogDefaultNamespace();
  }

  private String getDatabase(NameIdentifier gravitinoIdentifier) {
    Preconditions.checkArgument(
        gravitinoIdentifier.namespace().length() == 3,
        "Only support 3 level namespace," + gravitinoIdentifier.namespace());
    return gravitinoIdentifier.namespace().level(2);
  }
}
