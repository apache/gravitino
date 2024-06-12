/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.catalog;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.Schema;
import com.datastrato.gravitino.SchemaChange;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.spark.connector.ConnectorConstants;
import com.datastrato.gravitino.spark.connector.PropertiesConverter;
import com.datastrato.gravitino.spark.connector.SparkTableChangeConverter;
import com.datastrato.gravitino.spark.connector.SparkTransformConverter;
import com.datastrato.gravitino.spark.connector.SparkTransformConverter.DistributionAndSortOrdersInfo;
import com.datastrato.gravitino.spark.connector.SparkTypeConverter;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
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
 * BaseCatalog acts as the foundational class for Spark CatalogManager registration, enabling
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
  private SparkTypeConverter sparkTypeConverter;
  private SparkTableChangeConverter sparkTableChangeConverter;

  // The Gravitino catalog client to do schema operations.
  private Catalog gravitinoCatalogClient;
  private final String metalakeName;
  private String catalogName;
  private final GravitinoCatalogManager gravitinoCatalogManager;

  protected BaseCatalog() {
    gravitinoCatalogManager = GravitinoCatalogManager.get();
    metalakeName = gravitinoCatalogManager.getMetalakeName();
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
      com.datastrato.gravitino.rel.Table gravitinoTable,
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
      gravitinoNamespace = getCatalogDefaultNamespace()[0];
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
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    NameIdentifier gravitinoIdentifier = createGravitinoNameIdentifier(ident);
    com.datastrato.gravitino.rel.Column[] gravitinoColumns =
        Arrays.stream(schema.fields())
            .map(structField -> createGravitinoColumn(structField))
            .toArray(com.datastrato.gravitino.rel.Column[]::new);

    Map<String, String> gravitinoProperties =
        propertiesConverter.toGravitinoTableProperties(properties);
    // Spark store comment in properties, we should retrieve it and pass to Gravitino explicitly.
    String comment = gravitinoProperties.remove(ConnectorConstants.COMMENT);

    DistributionAndSortOrdersInfo distributionAndSortOrdersInfo =
        sparkTransformConverter.toGravitinoDistributionAndSortOrders(transforms);
    com.datastrato.gravitino.rel.expressions.transforms.Transform[] partitionings =
        sparkTransformConverter.toGravitinoPartitionings(transforms);

    try {
      com.datastrato.gravitino.rel.Table gravitinoTable =
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
    } catch (com.datastrato.gravitino.exceptions.TableAlreadyExistsException e) {
      throw new TableAlreadyExistsException(ident);
    }
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    try {
      com.datastrato.gravitino.rel.Table gravitinoTable = loadGravitinoTable(ident);
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
    } catch (com.datastrato.gravitino.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    com.datastrato.gravitino.rel.TableChange[] gravitinoTableChanges =
        Arrays.stream(changes)
            .map(sparkTableChangeConverter::toGravitinoTableChange)
            .toArray(com.datastrato.gravitino.rel.TableChange[]::new);
    try {
      com.datastrato.gravitino.rel.Table gravitinoTable =
          gravitinoCatalogClient
              .asTableCatalog()
              .alterTable(createGravitinoNameIdentifier(ident), gravitinoTableChanges);
      org.apache.spark.sql.connector.catalog.Table sparkTable = loadSparkTable(ident);
      return createSparkTable(
          ident,
          gravitinoTable,
          sparkTable,
          sparkCatalog,
          propertiesConverter,
          sparkTransformConverter,
          sparkTypeConverter);
    } catch (com.datastrato.gravitino.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }
  }

  @Override
  public boolean dropTable(Identifier ident) {
    return gravitinoCatalogClient.asTableCatalog().dropTable(createGravitinoNameIdentifier(ident));
  }

  @Override
  public boolean purgeTable(Identifier ident) {
    return gravitinoCatalogClient.asTableCatalog().purgeTable(createGravitinoNameIdentifier(ident));
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException {
    String[] oldDatabase = getDatabase(oldIdent);
    String[] newDatabase = getDatabase(newIdent);
    Preconditions.checkArgument(
        Arrays.equals(newDatabase, oldDatabase),
        "Doesn't support rename table to different database");
    com.datastrato.gravitino.rel.TableChange rename =
        com.datastrato.gravitino.rel.TableChange.rename(newIdent.name());
    try {
      gravitinoCatalogClient
          .asTableCatalog()
          .alterTable(createGravitinoNameIdentifier(oldIdent), rename);
    } catch (com.datastrato.gravitino.exceptions.NoSuchTableException e) {
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

  protected com.datastrato.gravitino.rel.Table loadGravitinoTable(Identifier ident)
      throws NoSuchTableException {
    try {
      return gravitinoCatalogClient
          .asTableCatalog()
          .loadTable(createGravitinoNameIdentifier(ident));
    } catch (com.datastrato.gravitino.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(ident);
    }
  }

  protected String[] getCatalogDefaultNamespace() {
    String[] catalogDefaultNamespace = sparkCatalog.defaultNamespace();
    Preconditions.checkArgument(
        catalogDefaultNamespace != null && catalogDefaultNamespace.length == 1,
        "Catalog default namespace is not valid");
    return new String[] {catalogDefaultNamespace[0]};
  }

  protected String[] getDatabase(Identifier sparkIdentifier) {
    if (sparkIdentifier.namespace().length > 0) {
      return new String[] {sparkIdentifier.namespace()[0]};
    }
    return getCatalogDefaultNamespace();
  }

  private NameIdentifier createGravitinoNameIdentifier(Identifier ident) {
    List<String> gravitinoNameIdentifier = new ArrayList<>();
    gravitinoNameIdentifier.addAll(Arrays.asList(metalakeName, catalogName));
    gravitinoNameIdentifier.addAll(Arrays.asList(getDatabase(ident)));
    gravitinoNameIdentifier.add(ident.name());
    return NameIdentifier.of(gravitinoNameIdentifier.toArray(new String[0]));
  }

  private void validateNamespace(String[] namespace) {
    Preconditions.checkArgument(
        namespace.length == 1,
        "Doesn't support multi level namespaces: " + String.join(".", namespace));
  }

  private com.datastrato.gravitino.rel.Column createGravitinoColumn(StructField structField) {
    return com.datastrato.gravitino.rel.Column.of(
        structField.name(),
        sparkTypeConverter.toGravitinoType(structField.dataType()),
        structField.getComment().isEmpty() ? null : structField.getComment().get(),
        structField.nullable(),
        // Spark doesn't support autoIncrement
        false,
        // todo: support default value
        com.datastrato.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET);
  }

  private String getDatabase(NameIdentifier gravitinoIdentifier) {
    Preconditions.checkArgument(
        gravitinoIdentifier.namespace().length() == 3,
        "Only support 3 level namespace," + gravitinoIdentifier.namespace());
    return gravitinoIdentifier.namespace().level(2);
  }

  private Table loadSparkTable(Identifier ident) {
    try {
      return sparkCatalog.loadTable(ident);
    } catch (NoSuchTableException e) {
      throw new RuntimeException(
          String.format("Failed to load the real sparkTable: %s", ident.toString()), e);
    }
  }
}
