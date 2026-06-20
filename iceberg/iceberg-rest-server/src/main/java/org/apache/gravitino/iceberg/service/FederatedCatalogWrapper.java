/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.gravitino.credential.CredentialPrivilege;
import org.apache.gravitino.credential.CredentialPropertyUtils;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.utils.MapUtils;
import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthManagers;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;

/**
 * A {@link CatalogWrapperForREST} for a federated Iceberg REST catalog (the underlying catalog is a
 * {@link RESTCatalog}).
 *
 * <p>Federation-specific behavior is expressed through polymorphic overrides instead of {@code
 * instanceof RESTCatalog} checks scattered across the base class. Table operations are routed to
 * federation-aware {@code *Internal} methods so client-facing FileIO and credential properties are
 * extracted from the remote catalog's {@code table.io()}. Credentials are vended by the remote
 * catalog, so this wrapper never injects Gravitino-generated credentials.
 *
 * <p>Portions of the table create and update handling are derived from Apache Iceberg's {@code
 * org.apache.iceberg.rest.CatalogHandlers}:
 * https://github.com/apache/iceberg/blob/2abac79fcae94b5ad039bd09f7235be191b0761e/core/src/main/java/org/apache/iceberg/rest/CatalogHandlers.java
 */
public class FederatedCatalogWrapper extends CatalogWrapperForREST {

  private static final String FORMAT_VERSION = "format-version";
  private static final Schema EMPTY_SCHEMA = new Schema();

  /**
   * Creates a federated wrapper.
   *
   * @param catalogName the catalog name.
   * @param config the Iceberg catalog configuration (backend {@code rest}).
   */
  public FederatedCatalogWrapper(String catalogName, IcebergConfig config) {
    super(catalogName, config);
  }

  @Override
  public LoadTableResponse createTable(
      Namespace namespace, CreateTableRequest request, boolean requestCredential) {
    // The remote REST catalog vends its own credentials, so the requestCredential flag is not used
    // here; FileIO-derived client config is extracted by createTableInternal.
    return createTableInternal(namespace, request);
  }

  @Override
  public LoadTableResponse loadTable(
      TableIdentifier identifier, boolean requestCredential, CredentialPrivilege privilege) {
    return loadTableInternal(identifier);
  }

  @Override
  public LoadTableResponse registerTable(
      Namespace namespace, RegisterTableRequest request, boolean requestCredential) {
    return registerTableInternal(namespace, request);
  }

  @Override
  public LoadTableResponse updateTable(
      TableIdentifier tableIdentifier, UpdateTableRequest updateTableRequest) {
    return tableUpdateInternal(tableIdentifier, updateTableRequest);
  }

  @Override
  Map<String, String> buildCatalogConfigToClients() {
    Map<String, String> merged = ((RESTCatalog) getCatalog()).properties();
    return filterCatalogConfigForClients(merged != null ? merged : Collections.emptyMap());
  }

  /**
   * Fetches table credentials from the remote REST catalog instead of vending Gravitino-managed
   * credentials. The {@code privilege} is ignored because the remote catalog decides what to vend.
   *
   * <p>The upstream credentials are rewritten via {@link IcebergRESTUtils#rewriteTableCredentials}
   * so their {@code refresh-credentials-endpoint} points at this IRC catalog rather than the remote
   * catalog, consistent with the {@link #loadTable}/{@code createTable} federation paths.
   *
   * @param identifier the table identifier.
   * @param privilege ignored; the remote REST catalog vends its own credentials.
   * @return the remote credentials with IRC-local refresh endpoints.
   */
  @Override
  public LoadCredentialsResponse getTableCredentials(
      TableIdentifier identifier, CredentialPrivilege privilege) {
    LoadCredentialsResponse upstream =
        getRESTTableCredentials((RESTCatalog) getCatalog(), identifier);
    return IcebergRESTUtils.rewriteTableCredentials(
        catalogCredentialManager.catalogName(), identifier, upstream);
  }

  private static LoadCredentialsResponse getRESTTableCredentials(
      RESTCatalog restCatalog, TableIdentifier identifier) {
    Map<String, String> properties = Maps.newHashMap(restCatalog.properties());
    String credentialsPath =
        ResourcePaths.forCatalogProperties(properties).table(identifier) + "/credentials";

    AuthManager authManager = null;
    RESTClient client = null;
    AuthSession authSession = null;
    try {
      authManager = AuthManagers.loadAuthManager(restCatalog.name(), properties);
      client =
          HTTPClient.builder(properties)
              .uri(properties.get(CatalogProperties.URI))
              .withHeaders(RESTUtil.configHeaders(properties))
              .build();
      authSession = authManager.catalogSession(client, properties);
      return client
          .withAuthSession(authSession)
          .get(
              credentialsPath,
              LoadCredentialsResponse.class,
              Collections.emptyMap(),
              ErrorHandlers.tableErrorHandler());
    } finally {
      if (authSession != null) {
        try {
          authSession.close();
        } catch (Exception e) {
          LOG.warn(
              "Failed to close auth session when loading credentials for table: {}", identifier, e);
        }
      }

      if (client != null) {
        try {
          client.close();
        } catch (Exception e) {
          LOG.warn(
              "Failed to close REST client when loading credentials for table: {}", identifier, e);
        }
      }

      if (authManager != null) {
        try {
          authManager.close();
        } catch (Exception e) {
          LOG.warn(
              "Failed to close auth manager when loading credentials for table: {}", identifier, e);
        }
      }
    }
  }

  /**
   * Federation-aware {@code createTable}: creates the table on the underlying (remote) catalog and
   * extracts client-facing FileIO/credential properties from {@code table.io()}.
   */
  private LoadTableResponse createTableInternal(Namespace namespace, CreateTableRequest request) {
    Catalog loadedCatalog = getCatalog();

    request.validate();

    if (request.stageCreate()) {
      return stageTableCreateInternal(namespace, request);
    }

    TableIdentifier ident = TableIdentifier.of(namespace, request.name());
    Table table =
        loadedCatalog
            .buildTable(ident, request.schema())
            .withLocation(request.location())
            .withPartitionSpec(request.spec())
            .withSortOrder(request.writeOrder())
            .withProperties(request.properties())
            .create();

    if (table instanceof BaseTable) {
      return buildLoadTableResponseFromFileIo(ident, (BaseTable) table);
    }

    throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
  }

  private LoadTableResponse stageTableCreateInternal(
      Namespace namespace, CreateTableRequest request) {
    Catalog loadedCatalog = getCatalog();
    TableIdentifier ident = TableIdentifier.of(namespace, request.name());
    if (loadedCatalog.tableExists(ident)) {
      throw new AlreadyExistsException("Table already exists: %s", ident);
    }

    Map<String, String> properties = Maps.newHashMap();
    properties.put("created-at", OffsetDateTime.now(ZoneOffset.UTC).toString());
    properties.putAll(request.properties());

    Map<String, String> config = Maps.newHashMap();
    Catalog.TableBuilder tableBuilder =
        loadedCatalog
            .buildTable(ident, request.schema())
            .withPartitionSpec(request.spec())
            .withSortOrder(request.writeOrder())
            .withProperties(properties);

    Table table;
    if (request.location() != null) {
      table = tableBuilder.withLocation(request.location()).createTransaction().table();
    } else {
      table = tableBuilder.createTransaction().table();
    }

    Map<String, String> tableProperties = retrieveFileIOProperties(table.io());
    Map<String, String> filteredCredentialProperties =
        CredentialPropertyUtils.filterCredentialProperties(tableProperties);
    config.putAll(
        MapUtils.getFilteredMap(
            tableProperties, key -> catalogPropertiesToClientKeys.contains(key)));
    config.putAll(filteredCredentialProperties);
    config.putAll(
        IcebergRESTUtils.buildRefreshProps(
            catalogCredentialManager.catalogName(), ident, filteredCredentialProperties));

    List<Credential> credentials =
        IcebergRESTUtils.buildStorageCreds(
            catalogCredentialManager.catalogName(), ident, table.io());

    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            request.schema(),
            request.spec() != null ? request.spec() : PartitionSpec.unpartitioned(),
            request.writeOrder() != null ? request.writeOrder() : SortOrder.unsorted(),
            table.location(),
            properties);

    return LoadTableResponse.builder()
        .withTableMetadata(metadata)
        .addAllConfig(config)
        .addAllCredentials(credentials)
        .build();
  }

  /**
   * Federation-aware {@code registerTable}: registers the existing table metadata on the underlying
   * (remote) catalog and extracts client-facing FileIO/credential properties from {@code
   * table.io()}, mirroring {@link #loadTableInternal(TableIdentifier)}.
   */
  private LoadTableResponse registerTableInternal(
      Namespace namespace, RegisterTableRequest request) {
    TableIdentifier ident = TableIdentifier.of(namespace, request.name());
    Table table = getCatalog().registerTable(ident, request.metadataLocation());

    if (table instanceof BaseTable) {
      return buildLoadTableResponseFromFileIo(ident, (BaseTable) table);
    }

    throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
  }

  /**
   * Federation-aware {@code updateTable}: applies the update against the underlying (remote)
   * catalog, including the staged-create path used by federated table creation.
   */
  private LoadTableResponse tableUpdateInternal(TableIdentifier ident, UpdateTableRequest request) {
    if (isCreate(request)) {
      // this is a hacky way to get TableOperations for an uncommitted table
      Optional<Integer> formatVersion =
          request.updates().stream()
              .filter(update -> update instanceof MetadataUpdate.UpgradeFormatVersion)
              .map(update -> ((MetadataUpdate.UpgradeFormatVersion) update).formatVersion())
              .findFirst();

      Schema schema =
          request.updates().stream()
              .filter(update -> update instanceof MetadataUpdate.AddSchema)
              .map(update -> ((MetadataUpdate.AddSchema) update).schema())
              .findFirst()
              .orElse(EMPTY_SCHEMA);

      Catalog.TableBuilder tableBuilder = getCatalog().buildTable(ident, schema);

      TableMetadata.Builder changedMetadata =
          formatVersion.map(TableMetadata::buildFromEmpty).orElse(TableMetadata.buildFromEmpty());
      request.updates().forEach(update -> update.applyTo(changedMetadata));

      TableMetadata changedTableMeta = changedMetadata.build();
      tableBuilder.withPartitionSpec(changedTableMeta.spec());
      tableBuilder.withSortOrder(changedTableMeta.sortOrder());
      tableBuilder.withLocation(changedTableMeta.location());
      tableBuilder.withProperty(FORMAT_VERSION, String.valueOf(changedTableMeta.formatVersion()));
      tableBuilder.withProperties(changedTableMeta.properties());

      Transaction transaction = tableBuilder.createOrReplaceTransaction();
      if (transaction instanceof BaseTransaction) {
        BaseTransaction baseTransaction = (BaseTransaction) transaction;

        return LoadTableResponse.builder()
            .withTableMetadata(create(baseTransaction, request))
            .build();
      } else {
        throw new IllegalStateException(
            "Cannot wrap catalog that does not produce BaseTransaction");
      }

    } else {
      return CatalogHandlers.updateTable(getCatalog(), ident, request);
    }
  }

  /**
   * Federation-aware {@code loadTable}: loads the table from the underlying (remote) catalog and
   * extracts client-facing FileIO/credential properties from {@code table.io()}.
   */
  private LoadTableResponse loadTableInternal(TableIdentifier ident) {
    Table table = getCatalog().loadTable(ident);

    if (table instanceof BaseTable) {
      return buildLoadTableResponseFromFileIo(ident, (BaseTable) table);
    } else if (table instanceof BaseMetadataTable) {
      // metadata tables are loaded on the client side, return NoSuchTableException for now
      throw new NoSuchTableException("Table does not exist: %s", ident.toString());
    }

    throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
  }

  /**
   * Builds a {@link LoadTableResponse} from a remote {@link BaseTable}, exposing the client-facing
   * FileIO and credential properties extracted from {@code table.io()}, including the refreshable
   * vended credentials and refresh properties for the remote storage.
   *
   * @param ident the table identifier, used to build the credential-refresh endpoint.
   * @param table the remote base table whose {@code io()} carries the storage credentials.
   * @return the load-table response including FileIO-derived client config and vended credentials.
   */
  private LoadTableResponse buildLoadTableResponseFromFileIo(
      TableIdentifier ident, BaseTable table) {
    Map<String, String> properties = retrieveFileIOProperties(table.io());
    Map<String, String> filteredCredentialProperties =
        CredentialPropertyUtils.filterCredentialProperties(properties);
    return LoadTableResponse.builder()
        .withTableMetadata(table.operations().current())
        .addAllConfig(
            MapUtils.getFilteredMap(properties, key -> catalogPropertiesToClientKeys.contains(key)))
        // Keep only credential fields from FileIO properties before returning them to the client.
        .addAllConfig(filteredCredentialProperties)
        .addAllConfig(
            IcebergRESTUtils.buildRefreshProps(
                catalogCredentialManager.catalogName(), ident, filteredCredentialProperties))
        .addAllCredentials(
            IcebergRESTUtils.buildStorageCreds(
                catalogCredentialManager.catalogName(), ident, table.io()))
        .build();
  }

  private static boolean isCreate(UpdateTableRequest request) {
    boolean isCreate =
        request.requirements().stream()
            .anyMatch(UpdateRequirement.AssertTableDoesNotExist.class::isInstance);

    if (isCreate) {
      List<UpdateRequirement> invalidRequirements =
          request.requirements().stream()
              .filter(req -> !(req instanceof UpdateRequirement.AssertTableDoesNotExist))
              .collect(Collectors.toList());
      Preconditions.checkArgument(
          invalidRequirements.isEmpty(), "Invalid create requirements: %s", invalidRequirements);
    }

    return isCreate;
  }

  private static TableMetadata create(BaseTransaction baseTransaction, UpdateTableRequest request) {
    // the only valid requirement is that the table will be created
    TableOperations ops = baseTransaction.underlyingOps();
    request.requirements().forEach(requirement -> requirement.validate(ops.current()));

    TableMetadata.Builder builder = TableMetadata.buildFrom(baseTransaction.currentMetadata());
    request
        .updates()
        .forEach(
            update -> {
              if (shouldApplyMetadataUpdateAfterBuilder(update)) {
                update.applyTo(builder);
              }
            });

    // create transactions do not retry. if the table exists, retrying is not a solution
    ops.commit(null, builder.build());

    return ops.current();
  }

  /**
   * Returns {@code false} for updates already reflected through {@link Catalog.TableBuilder} during
   * staged create; those must not be applied again on {@link TableMetadata.Builder}.
   */
  @VisibleForTesting
  static boolean shouldApplyMetadataUpdateAfterBuilder(MetadataUpdate update) {
    if (update instanceof MetadataUpdate.UpgradeFormatVersion) {
      return false;
    }

    if (update instanceof MetadataUpdate.AddSchema) {
      return false;
    }

    if (update instanceof MetadataUpdate.SetCurrentSchema) {
      return false;
    }

    if (update instanceof MetadataUpdate.RemoveSchemas) {
      return false;
    }

    if (update instanceof MetadataUpdate.SetLocation) {
      return false;
    }

    if (update instanceof MetadataUpdate.SetProperties) {
      return false;
    }

    if (update instanceof MetadataUpdate.RemoveProperties) {
      return false;
    }

    if (update instanceof MetadataUpdate.AddSortOrder) {
      return false;
    }

    if (update instanceof MetadataUpdate.SetDefaultSortOrder) {
      return false;
    }

    if (update instanceof MetadataUpdate.AddPartitionSpec) {
      return false;
    }

    if (update instanceof MetadataUpdate.SetDefaultPartitionSpec) {
      return false;
    }

    if (update instanceof MetadataUpdate.RemovePartitionSpecs) {
      return false;
    }

    return true;
  }

  private static Map<String, String> retrieveFileIOProperties(FileIO fileIO) {
    return fileIO instanceof InMemoryFileIO
        ? Maps.newHashMap()
        : new HashMap<>(fileIO.properties());
  }
}
