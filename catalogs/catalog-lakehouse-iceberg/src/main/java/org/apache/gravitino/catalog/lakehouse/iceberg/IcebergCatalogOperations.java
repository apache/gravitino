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
package org.apache.gravitino.catalog.lakehouse.iceberg;

import static org.apache.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.catalog.lakehouse.iceberg.ops.IcebergCatalogWrapperHelper;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.authentication.AuthenticationConfig;
import org.apache.gravitino.iceberg.common.authentication.SupportsKerberos;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper.IcebergTableChange;
import org.apache.gravitino.iceberg.common.ops.KerberosAwareIcebergCatalogProxy;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewCatalog;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.utils.ClassLoaderResourceCleanerUtils;
import org.apache.gravitino.utils.MapUtils;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Operations for interacting with an Apache Iceberg catalog in Apache Gravitino. */
public class IcebergCatalogOperations
    implements CatalogOperations, SupportsSchemas, TableCatalog, ViewCatalog {

  private static final String ICEBERG_TABLE_DOES_NOT_EXIST_MSG = "Iceberg table does not exist: %s";
  private static final String PHYSICAL_SCHEMA_SEPARATOR = ".";

  public static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogOperations.class);

  @VisibleForTesting IcebergCatalogWrapper icebergCatalogWrapper;
  @VisibleForTesting String schemaNameSeparator = ".";

  private IcebergCatalogWrapperHelper icebergCatalogWrapperHelper;

  /**
   * Initializes the Iceberg catalog operations with the provided configuration.
   *
   * @param conf The configuration map for the Iceberg catalog operations.
   * @param info The catalog info associated with this operations instance.
   * @throws RuntimeException if initialization fails.
   */
  @Override
  public void initialize(
      Map<String, String> conf, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {
    // Key format like gravitino.bypass.a.b
    Map<String, String> prefixMap = MapUtils.getPrefixMap(conf, CATALOG_BYPASS_PREFIX);

    // Hold keys that lie in GRAVITINO_CONFIG_TO_ICEBERG
    Map<String, String> gravitinoConfig =
        ((IcebergCatalogPropertiesMetadata) propertiesMetadata.catalogPropertiesMetadata())
            .transformProperties(conf);

    Map<String, String> resultConf = Maps.newHashMap(prefixMap);
    resultConf.putAll(gravitinoConfig);
    resultConf.put("catalog_uuid", info.id().toString());
    IcebergConfig icebergConfig = new IcebergConfig(resultConf);

    IcebergCatalogWrapper rawWrapper = new IcebergCatalogWrapper(icebergConfig);

    AuthenticationConfig authenticationConfig = new AuthenticationConfig(resultConf);
    this.icebergCatalogWrapper =
        authenticationConfig.isKerberosAuth() && rawWrapper.getCatalog() instanceof SupportsKerberos
            ? new KerberosAwareIcebergCatalogProxy(rawWrapper).getProxy(icebergConfig)
            : rawWrapper;
    if (GravitinoEnv.getInstance().config() != null) {
      String configuredSeparator =
          GravitinoEnv.getInstance()
              .config()
              .getRawString(
                  Configs.NAMESPACE_SCHEMA_NAME_SEPARATOR.key(),
                  Configs.NAMESPACE_SCHEMA_NAME_SEPARATOR.defaultValue());
      this.schemaNameSeparator =
          StringUtils.isBlank(configuredSeparator)
              ? Configs.NAMESPACE_SCHEMA_NAME_SEPARATOR.defaultValue()
              : configuredSeparator;
    }
    this.icebergCatalogWrapperHelper =
        new IcebergCatalogWrapperHelper(icebergCatalogWrapper.getCatalog());
  }

  /** Closes the Iceberg catalog and releases the associated client pool. */
  @Override
  public void close() {
    if (null != icebergCatalogWrapper) {
      try {
        icebergCatalogWrapper.close();
        ClassLoaderResourceCleanerUtils.closeClassLoaderResource(this.getClass().getClassLoader());
      } catch (Exception e) {
        LOG.warn("Failed to close Iceberg catalog", e);
      }
    }
  }

  /**
   * Lists the schemas under the given namespace.
   *
   * @param namespace The namespace to list the schemas for.
   * @return An array of {@link NameIdentifier} representing the schemas.
   * @throws NoSuchCatalogException If the provided namespace is invalid or does not exist.
   */
  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    try {
      List<org.apache.iceberg.catalog.Namespace> namespaces =
          icebergCatalogWrapper
              .listNamespace(IcebergCatalogWrapperHelper.getIcebergNamespace())
              .namespaces();
      List<String> schemaNames =
          namespaces.stream()
              .map(org.apache.iceberg.catalog.Namespace::toString)
              .collect(Collectors.toList());
      String parentSchema = namespace.length() > 2 ? toPhysicalSchemaName(namespace.level(namespace.length() - 1)) : null;

      List<String> hierarchicalSchemas =
          StringUtils.isBlank(parentSchema)
              ? topLevelSchemas(schemaNames, PHYSICAL_SCHEMA_SEPARATOR)
              : directChildSchemas(schemaNames, parentSchema, PHYSICAL_SCHEMA_SEPARATOR);
      return hierarchicalSchemas.stream()
          .map(
              schema ->
                  NameIdentifier.of(
                      namespace.level(0), namespace.level(1), toExternalSchemaName(schema)))
          .toArray(NameIdentifier[]::new);
    } catch (NoSuchNamespaceException e) {
      throw new NoSuchSchemaException(
          e, "Failed to list all schemas (database) under namespace : %s in Iceberg", namespace);
    }
  }

  /**
   * Creates a new schema with the provided identifier, comment, and metadata.
   *
   * @param ident The identifier of the schema to create.
   * @param comment The comment for the schema.
   * @param properties The properties for the schema.
   * @return The created {@link IcebergSchema}.
   * @throws NoSuchCatalogException If the provided namespace is invalid or does not exist.
   * @throws SchemaAlreadyExistsException If a schema with the same name already exists.
   */
  @Override
  public IcebergSchema createSchema(
      NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    try {
      for (String parent : parentSchemas(ident.name(), schemaNameSeparator)) {
        String parentPhysicalName = toPhysicalSchemaName(parent);
        try {
          icebergCatalogWrapper.createNamespace(
              IcebergSchema.builder()
                  .withName(parentPhysicalName)
                  .withComment(comment)
                  .withProperties(properties)
                  .withAuditInfo(AuditInfo.EMPTY)
                  .build()
                  .toCreateRequest(IcebergCatalogWrapperHelper.getIcebergNamespace(parentPhysicalName)));
        } catch (org.apache.iceberg.exceptions.AlreadyExistsException ignored) {
          // Parent schema already exists, continue to create the target schema.
        }
      }
      String currentUser = currentUser();
      IcebergSchema createdSchema =
          IcebergSchema.builder()
              .withName(toExternalSchemaName(toPhysicalSchemaName(ident.name())))
              .withComment(comment)
              .withProperties(properties)
              .withAuditInfo(
                  AuditInfo.builder()
                      .withCreator(currentUser)
                      .withCreateTime(Instant.now())
                      .build())
              .build();
      icebergCatalogWrapper.createNamespace(
          createdSchema.toCreateRequest(
              IcebergCatalogWrapperHelper.getIcebergNamespace(toPhysicalSchemaName(ident.name()))));
      LOG.info(
          "Created Iceberg schema (database) {} in Iceberg\ncurrentUser:{} \ncomment: {} \nmetadata: {}",
          ident.name(),
          currentUser,
          comment,
          properties);
      return createdSchema;
    } catch (org.apache.iceberg.exceptions.AlreadyExistsException e) {
      throw new SchemaAlreadyExistsException(
          e, "Iceberg schema (database) '%s' already exists", ident.name());
    } catch (NoSuchNamespaceException e) {
      throw new NoSuchSchemaException(
          e,
          "Iceberg schema (database) does not exist: %s in Gravitino store, This scenario occurs after the creation is completed and reloaded",
          ident.name());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Loads the schema with the provided identifier.
   *
   * @param ident The identifier of the schema to load.
   * @return The loaded {@link IcebergSchema}.
   * @throws NoSuchSchemaException If the schema with the provided identifier does not exist.
   */
  @Override
  public IcebergSchema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    try {
      GetNamespaceResponse response =
          icebergCatalogWrapper.loadNamespace(
              IcebergCatalogWrapperHelper.getIcebergNamespace(toPhysicalSchemaName(ident.name())));
      IcebergSchema icebergSchema =
          IcebergSchema.builder()
              .withName(toExternalSchemaName(toPhysicalSchemaName(ident.name())))
              .withComment(
                  Optional.of(response)
                      .map(GetNamespaceResponse::properties)
                      .map(map -> map.get(IcebergSchemaPropertiesMetadata.COMMENT))
                      .orElse(null))
              .withProperties(response.properties())
              .withAuditInfo(AuditInfo.EMPTY)
              .build();
      LOG.info("Loaded Iceberg schema (database) {} from Iceberg ", ident.name());
      return icebergSchema;
    } catch (NoSuchNamespaceException e) {
      throw new NoSuchSchemaException(
          e, "Iceberg schema (database) does not exist: %s in Gravitino store", ident.name());
    }
  }

  /**
   * Alters the schema with the provided identifier according to the specified changes.
   *
   * @param ident The identifier of the schema to alter.
   * @param changes The changes to apply to the schema.
   * @return The altered {@link IcebergSchema}.
   * @throws NoSuchSchemaException If the schema with the provided identifier does not exist.
   */
  @Override
  public IcebergSchema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    try {
      GetNamespaceResponse response =
          icebergCatalogWrapper.loadNamespace(
              IcebergCatalogWrapperHelper.getIcebergNamespace(toPhysicalSchemaName(ident.name())));
      Map<String, String> metadata = response.properties();
      List<String> removals = new ArrayList<>();
      Map<String, String> updates = new HashMap<>();
      Map<String, String> resultProperties = new HashMap<>(metadata);
      for (SchemaChange change : changes) {
        if (change instanceof SchemaChange.SetProperty) {
          String key = ((SchemaChange.SetProperty) change).getProperty();
          String val = ((SchemaChange.SetProperty) change).getValue();
          updates.put(key, val);
          resultProperties.put(key, val);
        } else if (change instanceof SchemaChange.RemoveProperty) {
          removals.add(((SchemaChange.RemoveProperty) change).getProperty());
          resultProperties.remove(((SchemaChange.RemoveProperty) change).getProperty());
        } else {
          throw new IllegalArgumentException(
              "Unsupported schema change type: " + change.getClass().getSimpleName());
        }
      }

      String comment =
          Optional.of(response.properties())
              .map(map -> map.get(IcebergSchemaPropertiesMetadata.COMMENT))
              .orElse(null);
      IcebergSchema icebergSchema =
          IcebergSchema.builder()
              .withName(toExternalSchemaName(toPhysicalSchemaName(ident.name())))
              .withComment(comment)
              .withAuditInfo(AuditInfo.EMPTY)
              .withProperties(resultProperties)
              .build();
      UpdateNamespacePropertiesRequest updateNamespacePropertiesRequest =
          UpdateNamespacePropertiesRequest.builder().updateAll(updates).removeAll(removals).build();
      UpdateNamespacePropertiesResponse updateNamespacePropertiesResponse =
          icebergCatalogWrapper.updateNamespaceProperties(
              IcebergCatalogWrapperHelper.getIcebergNamespace(toPhysicalSchemaName(ident.name())),
              updateNamespacePropertiesRequest);
      LOG.info(
          "Altered Iceberg schema (database) {}. UpdateResponse:\n{}",
          ident.name(),
          updateNamespacePropertiesResponse);
      return icebergSchema;
    } catch (NoSuchNamespaceException e) {
      throw new NoSuchSchemaException(
          e, "Iceberg schema (database) %s does not exist", ident.name());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Drops the schema with the provided identifier.
   *
   * @param ident The identifier of the schema to drop.
   * @param cascade If set to true, drops all the tables in the schema as well.
   * @return true if the schema was dropped successfully, false if the schema does not exist.
   * @throws NonEmptySchemaException If the schema is not empty and 'cascade' is set to false.
   */
  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    try {
      if (!schemaExists(ident)) {
        LOG.warn("Iceberg schema (database) {} does not exist", ident.name());
        return false;
      }
      List<String> allSchemas =
          icebergCatalogWrapper.listNamespace(IcebergCatalogWrapperHelper.getIcebergNamespace()).namespaces().stream()
              .map(org.apache.iceberg.catalog.Namespace::toString)
              .collect(Collectors.toList());
      String physicalSchemaName = toPhysicalSchemaName(ident.name());
      List<String> descendants =
          descendantSchemas(allSchemas, physicalSchemaName, PHYSICAL_SCHEMA_SEPARATOR);

      if (!cascade && !descendants.isEmpty()) {
        throw new NonEmptySchemaException(
            "Iceberg schema (database) %s is not empty. Sub-schemas exist: %s",
            ident.name(),
            descendants);
      }

      if (cascade) {
        List<String> schemasToDrop = new ArrayList<>(descendants);
        schemasToDrop.sort(Comparator.comparingInt(String::length).reversed());
        schemasToDrop.add(physicalSchemaName);
        for (String schemaName : schemasToDrop) {
          NameIdentifier schemaIdent =
              NameIdentifier.of(ident.namespace().level(0), ident.namespace().level(1), schemaName);
          Namespace schemaNamespace =
              Namespace.of(ArrayUtils.add(schemaIdent.namespace().levels(), schemaIdent.name()));
          for (NameIdentifier table : listTables(schemaNamespace)) {
            dropTable(table);
          }
          icebergCatalogWrapper.dropNamespace(IcebergCatalogWrapperHelper.getIcebergNamespace(schemaName));
        }
        LOG.info("Dropped Iceberg schema (database) {} with cascade", ident.name());
        return true;
      }

      icebergCatalogWrapper.dropNamespace(
          IcebergCatalogWrapperHelper.getIcebergNamespace(physicalSchemaName));
      LOG.info("Dropped Iceberg schema (database) {}", ident.name());
      return true;
    } catch (NamespaceNotEmptyException e) {
      throw new NonEmptySchemaException(
          e, "Iceberg schema (database) %s is not empty. One or more tables exist.", ident.name());
    } catch (NoSuchNamespaceException e) {
      LOG.warn("Iceberg schema (database) {} does not exist", ident.name());
      return false;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  static List<String> topLevelSchemas(List<String> allSchemas, String separator) {
    Set<String> result = new LinkedHashSet<>();
    for (String schema : allSchemas) {
      int idx = schema.indexOf(separator);
      result.add(idx < 0 ? schema : schema.substring(0, idx));
    }
    return new ArrayList<>(result);
  }

  @VisibleForTesting
  static List<String> directChildSchemas(List<String> allSchemas, String parent, String separator) {
    Set<String> result = new LinkedHashSet<>();
    int parentDepth = splitPath(parent, separator).length;
    String prefix = parent + separator;
    for (String schema : allSchemas) {
      if (!schema.startsWith(prefix)) {
        continue;
      }
      int depth = splitPath(schema, separator).length;
      if (depth == parentDepth + 1) {
        result.add(schema);
      }
    }
    return new ArrayList<>(result);
  }

  @VisibleForTesting
  static List<String> descendantSchemas(List<String> allSchemas, String parent, String separator) {
    String prefix = parent + separator;
    return allSchemas.stream().filter(schema -> schema.startsWith(prefix)).collect(Collectors.toList());
  }

  @VisibleForTesting
  static List<String> parentSchemas(String schema, String separator) {
    String[] levels = splitPath(schema, separator);
    List<String> result = new ArrayList<>();
    for (int i = 1; i < levels.length; i++) {
      result.add(String.join(separator, Arrays.copyOfRange(levels, 0, i)));
    }
    return result;
  }

  private static String[] splitPath(String path, String separator) {
    return path.split(java.util.regex.Pattern.quote(separator), -1);
  }

  /**
   * Lists all the tables under the specified namespace.
   *
   * @param namespace The namespace to list tables for.
   * @return An array of {@link NameIdentifier} representing the tables in the namespace.
   * @throws NoSuchSchemaException If the schema with the provided namespace does not exist.
   */
  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    NameIdentifier schemaIdent = NameIdentifier.of(namespace.levels());
    if (!schemaExists(schemaIdent)) {
      throw new NoSuchSchemaException("Schema (database) does not exist %s", namespace);
    }

    try {
      ListTablesResponse listTablesResponse =
          icebergCatalogWrapper.listTable(
              IcebergCatalogWrapperHelper.getIcebergNamespace(namespace));
      return listTablesResponse.identifiers().stream()
          .map(
              tableIdentifier ->
                  NameIdentifier.of(ArrayUtils.add(namespace.levels(), tableIdentifier.name())))
          .toArray(NameIdentifier[]::new);
    } catch (NoSuchNamespaceException e) {
      throw new NoSuchSchemaException("Schema (database) does not exist %s in Iceberg", namespace);
    }
  }

  /**
   * Loads a table from the Iceberg.
   *
   * @param tableIdent The identifier of the table to load.
   * @return The loaded IcebergTable instance representing the table.
   * @throws NoSuchTableException If the specified table does not exist in the Iceberg.
   */
  @Override
  public Table loadTable(NameIdentifier tableIdent) throws NoSuchTableException {
    try {

      LoadTableResponse tableResponse =
          icebergCatalogWrapper.loadTable(
              IcebergCatalogWrapperHelper.buildIcebergTableIdentifier(tableIdent));
      IcebergTable icebergTable =
          IcebergTable.fromIcebergTable(tableResponse.tableMetadata(), tableIdent.name());

      LOG.info("Loaded Iceberg table {}", tableIdent.name());
      return icebergTable;
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(e, ICEBERG_TABLE_DOES_NOT_EXIST_MSG, tableIdent.name());
    }
  }

  /**
   * Apply the {@link TableChange change} to an existing Iceberg table.
   *
   * @param tableIdent The identifier of the table to alter.
   * @param changes The changes to apply to the table.
   * @return This method always throws UnsupportedOperationException.
   * @throws NoSuchTableException This exception will not be thrown in this method.
   * @throws IllegalArgumentException This exception will not be thrown in this method.
   */
  @Override
  public Table alterTable(NameIdentifier tableIdent, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    Optional<TableChange> renameTableOptional =
        Arrays.stream(changes)
            .filter(tableChange -> tableChange instanceof TableChange.RenameTable)
            .reduce((a, b) -> b);
    if (renameTableOptional.isPresent()) {
      String otherChange =
          Arrays.stream(changes)
              .filter(tableChange -> !(tableChange instanceof TableChange.RenameTable))
              .map(String::valueOf)
              .collect(Collectors.joining("\n"));
      Preconditions.checkArgument(
          StringUtils.isEmpty(otherChange),
          String.format(
              "The operation to change the table name cannot be performed together with other operations."
                  + "The list of operations that you cannot perform includes: \n %s",
              otherChange));
      return renameTable(tableIdent, (TableChange.RenameTable) renameTableOptional.get());
    }
    return internalUpdateTable(tableIdent, changes);
  }

  private Table internalUpdateTable(NameIdentifier tableIdent, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    try {
      String[] levels = tableIdent.namespace().levels();
      IcebergTableChange icebergTableChange =
          icebergCatalogWrapperHelper.buildIcebergTableChanges(
              NameIdentifier.of(levels[levels.length - 1], tableIdent.name()), changes);
      LoadTableResponse loadTableResponse = icebergCatalogWrapper.updateTable(icebergTableChange);
      loadTableResponse.validate();
      return IcebergTable.fromIcebergTable(loadTableResponse.tableMetadata(), tableIdent.name());
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(e, ICEBERG_TABLE_DOES_NOT_EXIST_MSG, tableIdent.name());
    }
  }

  /**
   * Perform name change operations on the Iceberg.
   *
   * @param tableIdent tableIdent of this table.
   * @param renameTable Table Change to modify the table name.
   * @return Returns the table for Iceberg.
   * @throws NoSuchTableException
   * @throws IllegalArgumentException
   */
  private Table renameTable(NameIdentifier tableIdent, TableChange.RenameTable renameTable)
      throws NoSuchTableException, IllegalArgumentException {
    try {
      Namespace destNamespace = tableIdent.namespace();
      if (renameTable.getNewSchemaName().isPresent()) {
        String[] namespaceLevels = tableIdent.namespace().levels();
        String[] destLevels = Arrays.copyOf(namespaceLevels, namespaceLevels.length);
        destLevels[destLevels.length - 1] = renameTable.getNewSchemaName().get();
        NameIdentifier destSchemaIdent = NameIdentifier.of(destLevels);
        if (!schemaExists(destSchemaIdent)) {
          throw new NoSuchSchemaException("Iceberg schema does not exist %s", destSchemaIdent);
        }
        destNamespace = Namespace.of(destLevels);
      }

      RenameTableRequest renameTableRequest =
          RenameTableRequest.builder()
              .withSource(IcebergCatalogWrapperHelper.buildIcebergTableIdentifier(tableIdent))
              .withDestination(
                  IcebergCatalogWrapperHelper.buildIcebergTableIdentifier(
                      destNamespace, renameTable.getNewName()))
              .build();
      icebergCatalogWrapper.renameTable(renameTableRequest);
      return loadTable(
          NameIdentifier.of(ArrayUtils.add(destNamespace.levels(), renameTable.getNewName())));
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new NoSuchTableException(e, ICEBERG_TABLE_DOES_NOT_EXIST_MSG, tableIdent.name());
    }
  }

  /**
   * Drops a table from the Iceberg.
   *
   * @param tableIdent The identifier of the table to drop.
   * @return true if the table is successfully dropped; false if the table does not exist.
   */
  @Override
  public boolean dropTable(NameIdentifier tableIdent) {
    try {
      icebergCatalogWrapper.dropTable(
          IcebergCatalogWrapperHelper.buildIcebergTableIdentifier(tableIdent));
      LOG.info("Dropped Iceberg table {}", tableIdent.name());
      return true;
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      LOG.warn("Iceberg table {} does not exist", tableIdent.name());
      return false;
    }
  }

  /**
   * Creates a new table in the Iceberg.
   *
   * @param tableIdent The identifier of the table to create.
   * @param columns The array of columns for the new table.
   * @param comment The comment for the new table.
   * @param properties The properties for the new table.
   * @param partitioning The partitioning for the new table.
   * @param indexes The indexes for the new table.
   * @return The newly created IcebergTable instance.
   * @throws NoSuchSchemaException If the schema for the table does not exist.
   * @throws TableAlreadyExistsException If the table with the same name already exists.
   */
  @Override
  public Table createTable(
      NameIdentifier tableIdent,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    Preconditions.checkArgument(indexes.length == 0, "Iceberg-catalog does not support indexes");
    try {
      NameIdentifier schemaIdent = NameIdentifier.of(tableIdent.namespace().levels());
      if (!schemaExists(schemaIdent)) {
        LOG.warn("Iceberg schema (database) does not exist: {}", schemaIdent);
        throw new NoSuchSchemaException("Iceberg Schema (database) does not exist %s", schemaIdent);
      }
      IcebergColumn[] icebergColumns =
          Arrays.stream(columns)
              .map(
                  column ->
                      IcebergColumn.builder()
                          .withName(column.name())
                          .withType(column.dataType())
                          .withComment(column.comment())
                          .withNullable(column.nullable())
                          .build())
              .toArray(IcebergColumn[]::new);

      // Gravitino NONE distribution means the client side doesn't specify distribution, which is
      // not the same as none distribution in Iceberg.
      if (Distributions.NONE.equals(distribution)) {
        distribution =
            getIcebergDefaultDistribution(sortOrders.length > 0, partitioning.length > 0);
      }

      IcebergTable createdTable =
          IcebergTable.builder()
              .withName(tableIdent.name())
              .withColumns(icebergColumns)
              .withComment(comment)
              .withPartitioning(partitioning)
              .withSortOrders(sortOrders)
              .withProperties(properties)
              .withDistribution(distribution)
              .withAuditInfo(
                  AuditInfo.builder()
                      .withCreator(currentUser())
                      .withCreateTime(Instant.now())
                      .build())
              .build();

      LoadTableResponse loadTableResponse =
          icebergCatalogWrapper.createTable(
              IcebergCatalogWrapperHelper.getIcebergNamespace(toPhysicalSchemaName(schemaIdent.name())),
              createdTable.toCreateTableRequest());
      loadTableResponse.validate();

      LOG.info("Created Iceberg table {}", tableIdent.name());
      return createdTable;
    } catch (AlreadyExistsException e) {
      throw new TableAlreadyExistsException(e, "Table already exists: %s", tableIdent.name());
    }
  }

  /**
   * Purges a table from the Iceberg.
   *
   * @param tableIdent The identifier of the table to purge.
   * @return true if the table is successfully purged; false if the table does not exist.
   * @throws UnsupportedOperationException If the table type is EXTERNAL_TABLE, it cannot be purged.
   */
  @Override
  public boolean purgeTable(NameIdentifier tableIdent) throws UnsupportedOperationException {
    try {
      String schema = NameIdentifier.of(tableIdent.namespace().levels()).name();
      icebergCatalogWrapper.purgeTable(TableIdentifier.of(schema, tableIdent.name()));
      LOG.info("Purge Iceberg table {}", tableIdent.name());
      return true;
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      LOG.warn("Iceberg table {} does not exist", tableIdent.name());
      return false;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Performs `listNamespaces` operation on the Iceberg catalog to test the connection.
   *
   * @param catalogIdent the name of the catalog.
   * @param type the type of the catalog.
   * @param provider the provider of the catalog.
   * @param comment the comment of the catalog.
   * @param properties the properties of the catalog.
   */
  @Override
  public void testConnection(
      NameIdentifier catalogIdent,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties) {
    try {
      icebergCatalogWrapper.listNamespace(IcebergCatalogWrapperHelper.getIcebergNamespace());
    } catch (Exception e) {
      throw new ConnectionFailedException(
          e, "Failed to run listNamespace on Iceberg catalog: %s", e.getMessage());
    }
  }

  /**
   * Load view metadata from the Iceberg catalog.
   *
   * <p>Delegates to the underlying Iceberg REST catalog to load view metadata.
   *
   * @param ident The identifier of the view to load.
   * @return The loaded view metadata.
   * @throws NoSuchViewException If the view does not exist.
   */
  @Override
  public View loadView(NameIdentifier ident) throws NoSuchViewException {
    try {
      LoadViewResponse response =
          icebergCatalogWrapper.loadView(
              IcebergCatalogWrapperHelper.buildIcebergTableIdentifier(ident));

      return IcebergView.fromLoadViewResponse(response, ident.name());
    } catch (Exception e) {
      throw new NoSuchViewException(
          e, "Failed to load view %s from Iceberg catalog: %s", ident, e.getMessage());
    }
  }

  private static Distribution getIcebergDefaultDistribution(
      boolean isSorted, boolean isPartitioned) {
    if (isSorted) {
      return Distributions.RANGE;
    } else if (isPartitioned) {
      return Distributions.HASH;
    }
    return Distributions.NONE;
  }

  private static String currentUser() {
    return PrincipalUtils.getCurrentUserName();
  }

  private String toPhysicalSchemaName(String schemaName) {
    if (StringUtils.isBlank(schemaName)) {
      return schemaName;
    }
    if (PHYSICAL_SCHEMA_SEPARATOR.equals(schemaNameSeparator)) {
      return schemaName;
    }
    return schemaName.replace(schemaNameSeparator, PHYSICAL_SCHEMA_SEPARATOR);
  }

  private String toExternalSchemaName(String physicalSchemaName) {
    if (StringUtils.isBlank(physicalSchemaName)) {
      return physicalSchemaName;
    }
    if (PHYSICAL_SCHEMA_SEPARATOR.equals(schemaNameSeparator)) {
      return physicalSchemaName;
    }
    return physicalSchemaName.replace(PHYSICAL_SCHEMA_SEPARATOR, schemaNameSeparator);
  }
}
