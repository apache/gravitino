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

package org.apache.gravitino.spark.connector.catalog;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.spark.connector.ConnectorConstants;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTableChangeConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter.DistributionAndSortOrdersInfo;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.NamespaceChange.SetProperty;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * BaseCatalog acts as the foundational class for Apache Spark CatalogManager registration, enabling
 * seamless integration of various data source catalogs within Spark's ecosystem. This class is
 * pivotal in bridging Spark with diverse data sources, ensuring a unified approach to data
 * management and manipulation across the platform.
 *
 * <p>This class implements essential interfaces for the table and namespace management. Subclasses
 * can extend BaseCatalog to implement more specific interfaces tailored to the needs of different
 * data sources. Its lazy loading design ensures that instances of BaseCatalog are created only when
 * needed, optimizing resource utilization and minimizing the overhead associated with
 * initialization.
 */
public abstract class BaseCatalog implements TableCatalog, SupportsNamespaces {

  // The specific Spark catalog to do IO operations, different catalogs have different spark catalog
  // implementations, like HiveTableCatalog for Hive, JDBCTableCatalog for JDBC, SparkCatalog for
  // Iceberg.
  protected TableCatalog sparkCatalog;
  protected PropertiesConverter propertiesConverter;
  protected SparkTransformConverter sparkTransformConverter;
  // The Gravitino catalog client to do schema operations.
  protected Catalog gravitinoCatalogClient;
  private SparkTypeConverter sparkTypeConverter;
  private SparkTableChangeConverter sparkTableChangeConverter;

  private String catalogName;
  private final GravitinoCatalogManager gravitinoCatalogManager;

  protected BaseCatalog() {
    gravitinoCatalogManager = GravitinoCatalogManager.get();
  }

  /**
   * Create a specific Spark catalog, mainly used to create Spark table.
   *
   * @param name catalog name
   * @param options catalog options from configuration
   * @param properties catalog properties from Gravitino
   * @return a specific Spark catalog
   */
  protected abstract TableCatalog createAndInitSparkCatalog(
      String name, CaseInsensitiveStringMap options, Map<String, String> properties);

  /**
   * Create a specific Spark table, combined with gravitinoTable to do DML operations and
   * sparkCatalog to do IO operations.
   *
   * @param identifier Spark's table identifier
   * @param gravitinoTable Gravitino table to do DDL operations
   * @param sparkTable Spark internal table to do IO operations
   * @param sparkCatalog specific Spark catalog to do IO operations
   * @param propertiesConverter transform properties between Gravitino and Spark
   * @param sparkTransformConverter sparkTransformConverter convert transforms between Gravitino and
   *     Spark
   * @param sparkTypeConverter sparkTypeConverter convert types between Gravitino and Spark
   * @return a specific Spark table
   */
  protected abstract Table createSparkTable(
      Identifier identifier,
      org.apache.gravitino.rel.Table gravitinoTable,
      Table sparkTable,
      TableCatalog sparkCatalog,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter,
      SparkTypeConverter sparkTypeConverter);

  /**
   * Get a PropertiesConverter to transform properties between Gravitino and Spark.
   *
   * @return an PropertiesConverter
   */
  protected abstract PropertiesConverter getPropertiesConverter();

  /**
   * Get a SparkTransformConverter to convert transforms between Gravitino and Spark.
   *
   * @return an SparkTransformConverter
   */
  protected abstract SparkTransformConverter getSparkTransformConverter();

  protected SparkTypeConverter getSparkTypeConverter() {
    return new SparkTypeConverter();
  }

  protected SparkTableChangeConverter getSparkTableChangeConverter(
      SparkTypeConverter sparkTypeConverter) {
    return new SparkTableChangeConverter(sparkTypeConverter);
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;
    this.gravitinoCatalogClient = gravitinoCatalogManager.getGravitinoCatalogInfo(name);
    String provider = gravitinoCatalogClient.provider();
    Preconditions.checkArgument(
        StringUtils.isNotBlank(provider), name + " catalog provider is empty");
    this.sparkCatalog =
        createAndInitSparkCatalog(name, options, gravitinoCatalogClient.properties());
    this.propertiesConverter = getPropertiesConverter();
    this.sparkTransformConverter = getSparkTransformConverter();
    this.sparkTypeConverter = getSparkTypeConverter();
    this.sparkTableChangeConverter = getSparkTableChangeConverter(sparkTypeConverter);
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
          gravitinoCatalogClient.asTableCatalog().listTables(Namespace.of(gravitinoNamespace));
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
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    NameIdentifier gravitinoIdentifier = NameIdentifier.of(getDatabase(ident), ident.name());
    org.apache.gravitino.rel.Column[] gravitinoColumns =
        Arrays.stream(schema.fields())
            .map(structField -> createGravitinoColumn(structField))
            .toArray(org.apache.gravitino.rel.Column[]::new);

    Map<String, String> gravitinoProperties =
        propertiesConverter.toGravitinoTableProperties(properties);
    // Spark store comment in properties, we should retrieve it and pass to Gravitino explicitly.
    String comment = gravitinoProperties.remove(ConnectorConstants.COMMENT);

    DistributionAndSortOrdersInfo distributionAndSortOrdersInfo =
        sparkTransformConverter.toGravitinoDistributionAndSortOrders(transforms);
    org.apache.gravitino.rel.expressions.transforms.Transform[] partitionings =
        sparkTransformConverter.toGravitinoPartitionings(transforms);

    try {
      org.apache.gravitino.rel.Table gravitinoTable =
          gravitinoCatalogClient
              .asTableCatalog()
              .createTable(
                  gravitinoIdentifier,
                  gravitinoColumns,
                  comment,
                  gravitinoProperties,
                  partitionings,
                  distributionAndSortOrdersInfo.getDistribution(),
                  distributionAndSortOrdersInfo.getSortOrders());
      org.apache.spark.sql.connector.catalog.Table sparkTable = loadSparkTable(ident);
      return createSparkTable(
          ident,
          gravitinoTable,
          sparkTable,
          sparkCatalog,
          propertiesConverter,
          sparkTransformConverter,
          sparkTypeConverter);
    } catch (NoSuchSchemaException e) {
      throw new NoSuchNamespaceException(ident.namespace());
    } catch (org.apache.gravitino.exceptions.TableAlreadyExistsException e) {
      throw new TableAlreadyExistsException(ident);
    }
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    try {
      org.apache.gravitino.rel.Table gravitinoTable = loadGravitinoTable(ident);
      org.apache.spark.sql.connector.catalog.Table sparkTable = loadSparkTable(ident);
      // Will create a catalog specific table
      return createSparkTable(
          ident,
          gravitinoTable,
          sparkTable,
          sparkCatalog,
          propertiesConverter,
          sparkTransformConverter,
          sparkTypeConverter);
    } catch (org.apache.gravitino.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    org.apache.gravitino.rel.TableChange[] gravitinoTableChanges =
        Arrays.stream(changes)
            .map(sparkTableChangeConverter::toGravitinoTableChange)
            .toArray(org.apache.gravitino.rel.TableChange[]::new);
    try {
      sparkCatalog.invalidateTable(ident);
      org.apache.gravitino.rel.Table gravitinoTable =
          gravitinoCatalogClient
              .asTableCatalog()
              .alterTable(
                  NameIdentifier.of(getDatabase(ident), ident.name()), gravitinoTableChanges);
      org.apache.spark.sql.connector.catalog.Table sparkTable = loadSparkTable(ident);
      return createSparkTable(
          ident,
          gravitinoTable,
          sparkTable,
          sparkCatalog,
          propertiesConverter,
          sparkTransformConverter,
          sparkTypeConverter);
    } catch (org.apache.gravitino.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }
  }

  @Override
  public boolean dropTable(Identifier ident) {
    sparkCatalog.invalidateTable(ident);
    return gravitinoCatalogClient
        .asTableCatalog()
        .dropTable(NameIdentifier.of(getDatabase(ident), ident.name()));
  }

  @Override
  public boolean purgeTable(Identifier ident) {
    sparkCatalog.invalidateTable(ident);
    return gravitinoCatalogClient
        .asTableCatalog()
        .purgeTable(NameIdentifier.of(getDatabase(ident), ident.name()));
  }

  @Override
  public boolean tableExists(Identifier ident) {
    // Gravitino uses loadTable() to verify table existence, which requires LOAD_TABLE privilege.
    // For CREATE TABLE IF NOT EXISTS operations, users may only have CREATE_TABLE privilege.
    // When ForbiddenException is thrown (lacking LOAD_TABLE privilege), we return false to allow
    // the CREATE TABLE operation to proceed.
    // See: https://github.com/apache/gravitino/issues/9180
    try {
      loadGravitinoTable(ident);
      return true;
    } catch (NoSuchTableException e) {
      return false;
    } catch (ForbiddenException e) {
      // User lacks LOAD_TABLE privilege, return false to allow CREATE TABLE IF NOT EXISTS
      return false;
    }
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException {
    String oldDatabase = getDatabase(oldIdent);
    String newDatabase = getDatabase(newIdent);
    Preconditions.checkArgument(
        newDatabase.equals(oldDatabase), "Doesn't support rename table to different database");
    org.apache.gravitino.rel.TableChange rename =
        org.apache.gravitino.rel.TableChange.rename(newIdent.name());
    try {
      sparkCatalog.invalidateTable(oldIdent);
      gravitinoCatalogClient
          .asTableCatalog()
          .alterTable(NameIdentifier.of(getDatabase(oldIdent), oldIdent.name()), rename);
    } catch (org.apache.gravitino.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(oldIdent);
    }
  }

  @Override
  public String[][] listNamespaces() throws NoSuchNamespaceException {
    String[] schemas = gravitinoCatalogClient.asSchemas().listSchemas();
    return Arrays.stream(schemas).map(schema -> new String[] {schema}).toArray(String[][]::new);
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
      Schema schema = gravitinoCatalogClient.asSchemas().loadSchema(namespace[0]);
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
      gravitinoCatalogClient.asSchemas().createSchema(namespace[0], comment, properties);
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
      gravitinoCatalogClient.asSchemas().alterSchema(namespace[0], schemaChanges);
    } catch (NoSuchSchemaException e) {
      throw new NoSuchNamespaceException(namespace);
    }
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade)
      throws NoSuchNamespaceException, NonEmptyNamespaceException {
    validateNamespace(namespace);
    try {
      return gravitinoCatalogClient.asSchemas().dropSchema(namespace[0], cascade);
    } catch (NonEmptySchemaException e) {
      throw new NonEmptyNamespaceException(namespace);
    }
  }

  protected org.apache.gravitino.rel.Table loadGravitinoTable(Identifier ident)
      throws NoSuchTableException {
    try {
      String database = getDatabase(ident);
      return gravitinoCatalogClient
          .asTableCatalog()
          .loadTable(NameIdentifier.of(database, ident.name()));
    } catch (org.apache.gravitino.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }
  }

  protected String getDatabase(Identifier sparkIdentifier) {
    if (sparkIdentifier.namespace().length > 0) {
      return sparkIdentifier.namespace()[0];
    }
    return getCatalogDefaultNamespace();
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

  private org.apache.gravitino.rel.Column createGravitinoColumn(StructField structField) {
    return org.apache.gravitino.rel.Column.of(
        structField.name(),
        sparkTypeConverter.toGravitinoType(structField.dataType()),
        structField.getComment().isEmpty() ? null : structField.getComment().get(),
        structField.nullable(),
        // Spark doesn't support autoIncrement
        false,
        // todo: support default value
        org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET);
  }

  private String getDatabase(NameIdentifier gravitinoIdentifier) {
    Preconditions.checkArgument(
        gravitinoIdentifier.namespace().length() == 1,
        "Only support 1 level namespace," + gravitinoIdentifier.namespace());
    return gravitinoIdentifier.namespace().level(0);
  }

  private Table loadSparkTable(Identifier ident) {
    try {
      return sparkCatalog.loadTable(ident);
    } catch (NoSuchTableException e) {
      throw new RuntimeException(
          String.format(
              "Failed to load the real sparkTable: %s",
              String.join(".", getDatabase(ident), ident.name())),
          e);
    }
  }
}
