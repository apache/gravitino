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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.URI;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.CatalogCredentialManager;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.CredentialPrivilege;
import org.apache.gravitino.credential.CredentialPropertyUtils;
import org.apache.gravitino.credential.PathBasedCredentialContext;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.iceberg.service.cache.ScanPlanCache;
import org.apache.gravitino.iceberg.service.cache.ScanPlanCacheKey;
import org.apache.gravitino.storage.GCSProperties;
import org.apache.gravitino.utils.ClassUtils;
import org.apache.gravitino.utils.MapUtils;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Scan;
import org.apache.iceberg.ScanTaskParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.PlanStatus;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ImmutableLoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;

/** Process Iceberg REST specific operations, like credential vending. */
public class CatalogWrapperForREST extends IcebergCatalogWrapper {

  private final CatalogCredentialManager catalogCredentialManager;

  private final Map<String, String> catalogConfigToClients;

  private final ScanPlanCache scanPlanCache;

  private static final String DATA_ACCESS_VENDED_CREDENTIALS = "vended-credentials";
  private static final String DATA_ACCESS_REMOTE_SIGNING = "remote-signing";

  private static final Set<String> catalogPropertiesToClientKeys =
      ImmutableSet.of(
          IcebergConstants.IO_IMPL,
          IcebergConstants.AWS_S3_REGION,
          IcebergConstants.ICEBERG_S3_ENDPOINT,
          IcebergConstants.ICEBERG_OSS_ENDPOINT,
          IcebergConstants.ICEBERG_S3_PATH_STYLE_ACCESS,
          IcebergConstants.ICEBERG_GCS_PROJECT_ID,
          IcebergConstants.ICEBERG_GCS_SERVICE_HOST,
          IcebergConstants.ICEBERG_ACCESS_DELEGATION);

  @SuppressWarnings("deprecation")
  private static Map<String, String> deprecatedProperties =
      ImmutableMap.of(
          CredentialConstants.CREDENTIAL_PROVIDER_TYPE,
          CredentialConstants.CREDENTIAL_PROVIDERS,
          "gcs-credential-file-path",
          GCSProperties.GRAVITINO_GCS_SERVICE_ACCOUNT_FILE);

  public CatalogWrapperForREST(String catalogName, IcebergConfig config) {
    super(config);
    this.catalogConfigToClients = buildCatalogConfigToClients(config, getCatalog());
    // To be compatible with old properties
    Map<String, String> catalogProperties =
        checkForCompatibility(config.getAllConfig(), deprecatedProperties);
    this.catalogCredentialManager = new CatalogCredentialManager(catalogName, catalogProperties);
    this.scanPlanCache = loadScanPlanCache(config);
  }

  public LoadTableResponse createTable(
      Namespace namespace, CreateTableRequest request, boolean requestCredential) {
    if (requestCredential && StringUtils.isNotBlank(request.location())) {
      validateCreateLocation(request.location(), catalogCredentialManager.getCredentialTypes());
    }
    LoadTableResponse loadTableResponse;
    if (catalog instanceof RESTCatalog) {
      loadTableResponse = createTableInternal(namespace, request);
    } else {
      loadTableResponse = super.createTable(namespace, request);
    }
    if (shouldGenerateCredential(loadTableResponse, requestCredential)) {
      return injectCredentialConfig(
          TableIdentifier.of(namespace, request.name()),
          loadTableResponse,
          CredentialPrivilege.WRITE);
    }
    return loadTableResponse;
  }

  public LoadTableResponse loadTable(
      TableIdentifier identifier, boolean requestCredential, CredentialPrivilege privilege) {
    LoadTableResponse loadTableResponse;
    if (catalog instanceof RESTCatalog) {
      loadTableResponse = loadTableInternal(identifier);
    } else {
      loadTableResponse = super.loadTable(identifier);
    }
    if (shouldGenerateCredential(loadTableResponse, requestCredential)) {
      return injectCredentialConfig(identifier, loadTableResponse, privilege);
    }
    return loadTableResponse;
  }

  @Override
  public LoadTableResponse updateTable(
      TableIdentifier tableIdentifier, UpdateTableRequest updateTableRequest) {
    if (catalog instanceof RESTCatalog) {
      return CatalogHandlers.updateTable(catalog, tableIdentifier, updateTableRequest);
    } else {
      return super.updateTable(tableIdentifier, updateTableRequest);
    }
  }

  /**
   * Get table credentials.
   *
   * @param identifier The table identifier for which to load credentials
   * @return A {@link org.apache.iceberg.rest.responses.LoadCredentialsResponse} object containing
   *     the credentials.
   */
  public LoadCredentialsResponse getTableCredentials(
      TableIdentifier identifier, CredentialPrivilege privilege) {
    try {
      LoadTableResponse loadTableResponse = super.loadTable(identifier);
      List<Credential> credentials = getCredentials(loadTableResponse, privilege);
      ImmutableLoadCredentialsResponse.Builder responseBuilder =
          ImmutableLoadCredentialsResponse.builder();
      for (Credential credential : credentials) {
        org.apache.iceberg.rest.credentials.Credential icebergCredential =
            new org.apache.iceberg.rest.credentials.Credential() {
              @Override
              public String prefix() {
                return "";
              }

              @Override
              public Map<String, String> config() {
                // Convert Gravitino credentials to the Iceberg REST credential payload format.
                return CredentialPropertyUtils.toIcebergProperties(credential);
              }

              @Override
              public void validate() {}
            };
        responseBuilder.addCredentials(icebergCredential);
      }
      return responseBuilder.build();
    } catch (ServiceUnavailableException e) {
      LOG.warn("Service unavailable when loading table credentials for table: {}", identifier, e);
      return ImmutableLoadCredentialsResponse.builder().build();
    }
  }

  @Override
  public void close() throws Exception {
    try {
      if (catalogCredentialManager != null) {
        catalogCredentialManager.close();
      }
      if (scanPlanCache != null) {
        scanPlanCache.close();
      }
    } finally {
      // Call super.close() to release parent class resources including:
      // 1. Close underlying catalog (JdbcCatalog, WrappedHiveCatalog, etc.)
      // 2. Close metadata cache
      // 3. Cleanup JDBC drivers and threads (MySQL AbandonedConnectionCleanupThread, etc.)
      super.close();
    }
  }

  public Map<String, String> getCatalogConfigToClient() {
    return catalogConfigToClients;
  }

  /**
   * Builds properties exposed to Iceberg clients via the IRC {@code /v1/config} defaults.
   *
   * <p>For {@link RESTCatalog}, uses {@link RESTCatalog#properties()} so defaults reflect the
   * remote catalog's config response merged with client properties (after REST handshake), not only
   * static Gravitino catalog configuration.
   */
  @VisibleForTesting
  static Map<String, String> buildCatalogConfigToClients(IcebergConfig config, Catalog catalog) {
    Map<String, String> sourceProps;
    if (catalog instanceof RESTCatalog) {
      Map<String, String> merged = ((RESTCatalog) catalog).properties();
      sourceProps = merged != null ? new HashMap<>(merged) : new HashMap<>();
    } else {
      sourceProps = new HashMap<>(config.getIcebergCatalogProperties());
    }

    Map<String, String> filtered =
        MapUtils.getFilteredMap(sourceProps, key -> catalogPropertiesToClientKeys.contains(key));
    filtered = new HashMap<>(filtered);
    validateAndNormalizeDataAccessProperty(filtered);

    return Collections.unmodifiableMap(filtered);
  }

  @VisibleForTesting
  static void validateAndNormalizeDataAccessProperty(Map<String, String> properties) {
    String dataAccess = properties.get(IcebergConstants.ICEBERG_ACCESS_DELEGATION);
    if (StringUtils.isBlank(dataAccess)) {
      return;
    }

    String normalizedDataAccess = dataAccess.toLowerCase(Locale.ROOT);
    if (!DATA_ACCESS_VENDED_CREDENTIALS.equals(normalizedDataAccess)
        && !DATA_ACCESS_REMOTE_SIGNING.equals(normalizedDataAccess)) {
      throw new IllegalArgumentException(
          "Invalid catalog property '"
              + IcebergConstants.DATA_ACCESS
              + "': "
              + dataAccess
              + ", supported values are ["
              + DATA_ACCESS_VENDED_CREDENTIALS
              + ","
              + DATA_ACCESS_REMOTE_SIGNING
              + "]");
    }
  }

  @Override
  protected boolean useDifferentClassLoader() {
    return false;
  }

  private LoadTableResponse injectCredentialConfig(
      TableIdentifier tableIdentifier,
      LoadTableResponse loadTableResponse,
      CredentialPrivilege privilege) {
    List<Credential> credentials = getCredentials(loadTableResponse, privilege);

    credentials.forEach(
        credential ->
            LOG.info(
                "Generate credential: {} for Iceberg table: {}",
                credential.credentialType(),
                tableIdentifier));

    LoadTableResponse.Builder builder =
        LoadTableResponse.builder()
            .withTableMetadata(loadTableResponse.tableMetadata())
            .addAllConfig(loadTableResponse.config())
            .addAllConfig(getCatalogConfigToClient());

    // Merge credential fields from each credential as Iceberg client config entries.
    for (Credential credential : credentials) {
      builder.addAllConfig(CredentialPropertyUtils.toIcebergProperties(credential));
    }

    return builder.build();
  }

  private List<Credential> getCredentials(
      LoadTableResponse loadTableResponse, CredentialPrivilege privilege) {
    return getCredentials(catalogCredentialManager, loadTableResponse.tableMetadata(), privilege);
  }

  @VisibleForTesting
  static List<Credential> getCredentials(
      CatalogCredentialManager credentialManager,
      TableMetadata tableMetadata,
      CredentialPrivilege privilege) {
    List<String> paths =
        Stream.of(
                tableMetadata.location(),
                tableMetadata.property(TableProperties.WRITE_DATA_LOCATION, ""),
                tableMetadata.property(TableProperties.WRITE_METADATA_LOCATION, ""))
            .filter(StringUtils::isNotBlank)
            .collect(Collectors.toList());

    if (paths.isEmpty()) {
      throw new ServiceUnavailableException(
          "Table has no storage location for credential generation");
    }

    // All paths for a single table must share a storage family. This is enforced at CREATE TABLE
    // time; we re-check here defensively in case a pathological table configuration slips through.
    Set<String> prefixes =
        paths.stream()
            .map(CredentialSchemeUtil::credentialTypePrefixFor)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toSet());

    if (prefixes.size() > 1) {
      throw new IllegalStateException(
          String.format(
              "Table paths span multiple storage families %s; a single table must reside entirely"
                  + " on one backend. Paths: %s",
              prefixes, paths));
    }
    if (prefixes.isEmpty()) {
      throw new ServiceUnavailableException(
          "Cannot determine storage family for table paths: %s", paths);
    }

    String prefix = prefixes.iterator().next();
    String credentialType =
        findCredentialTypeByPrefix(credentialManager, prefix)
            .orElseThrow(
                () ->
                    new ServiceUnavailableException(
                        "No credential provider registered for storage family '%s' (paths: %s)",
                        prefix, paths));

    PathBasedCredentialContext context =
        privilege == CredentialPrivilege.WRITE
            ? new PathBasedCredentialContext(
                PrincipalUtils.getCurrentUserName(),
                ImmutableSet.copyOf(paths),
                Collections.emptySet())
            : new PathBasedCredentialContext(
                PrincipalUtils.getCurrentUserName(),
                Collections.emptySet(),
                ImmutableSet.copyOf(paths));

    Credential credential = credentialManager.getCredential(credentialType, context);
    if (credential == null) {
      throw new ServiceUnavailableException(
          "Couldn't generate credential, type: %s, context: %s", credentialType, context);
    }
    return Collections.singletonList(credential);
  }

  private static Optional<String> findCredentialTypeByPrefix(
      CatalogCredentialManager credentialManager, String prefix) {
    return credentialManager.getCredentialTypes().stream()
        .filter(type -> type.equals(prefix) || type.startsWith(prefix + "-"))
        .findFirst();
  }

  private boolean shouldGenerateCredential(
      LoadTableResponse loadTableResponse, boolean requestCredential) {
    if (!requestCredential) {
      return false;
    }

    // RESTCatalog will fetch credential from the remote catalog instead of generating credential
    if (getCatalog() instanceof RESTCatalog) {
      return false;
    }

    validateCredentialLocation(loadTableResponse.tableMetadata().location());
    return !isLocalOrHdfsTable(loadTableResponse.tableMetadata());
  }

  private boolean isLocalOrHdfsTable(TableMetadata tableMetadata) {
    return isLocalOrHdfsLocation(tableMetadata.location());
  }

  @VisibleForTesting
  static void validateCredentialLocation(String location) {
    if (StringUtils.isBlank(location)) {
      throw new IllegalArgumentException(
          "Table location cannot be null or blank when requesting credentials");
    }
  }

  /**
   * Validates that an explicit CREATE TABLE location has a matching credential provider registered
   * on the catalog. Fails fast at CREATE time so a table is never created that the catalog cannot
   * vend credentials for.
   *
   * <p>A blank location is allowed here — it means the table will land in the default warehouse,
   * and the existing provider on that warehouse's scheme will handle it.
   *
   * <p>Local and HDFS locations do not require vended credentials and are not checked.
   */
  @VisibleForTesting
  static void validateCreateLocation(String location, Set<String> registeredCredentialTypes) {
    if (StringUtils.isBlank(location) || isLocalOrHdfsLocation(location)) {
      return;
    }
    Optional<String> prefix = CredentialSchemeUtil.credentialTypePrefixFor(location);
    if (!prefix.isPresent()) {
      throw new IllegalArgumentException(
          String.format(
              "CREATE TABLE location '%s' uses an unrecognized storage scheme", location));
    }
    boolean hasProvider =
        registeredCredentialTypes.stream()
            .anyMatch(type -> type.equals(prefix.get()) || type.startsWith(prefix.get() + "-"));
    if (!hasProvider) {
      throw new IllegalArgumentException(
          String.format(
              "CREATE TABLE location '%s' (storage family '%s') has no registered credential"
                  + " provider on this catalog. Registered providers: %s",
              location, prefix.get(), registeredCredentialTypes));
    }
  }

  @VisibleForTesting
  static boolean isLocalOrHdfsLocation(String location) {
    // Precondition: location is non-blank (enforced by caller).
    if (StringUtils.isBlank(location)) {
      return false;
    }
    URI uri;
    try {
      uri = URI.create(location);
    } catch (IllegalArgumentException e) {
      return false;
    }
    String scheme = uri.getScheme();
    if (scheme == null) {
      // No scheme means a local path.
      return true;
    }
    return "file".equalsIgnoreCase(scheme) || "hdfs".equalsIgnoreCase(scheme);
  }

  /**
   * Plan table scan and return scan tasks.
   *
   * <p>This method performs server-side scan planning to optimize query performance by reducing
   * client-side metadata loading and enabling parallel task execution.
   *
   * <p>Implementation uses synchronous scan planning (COMPLETED status) where tasks are returned
   * immediately as serialized JSON strings. This is different from asynchronous mode (SUBMITTED
   * status) where a plan ID is returned for later retrieval.
   *
   * <p>Referenced from Iceberg PR #13400 for scan planning implementation.
   *
   * @param tableIdentifier The table identifier.
   * @param scanRequest The scan request parameters including filters, projections, snapshot-id,
   *     etc.
   * @return PlanTableScanResponse with status=COMPLETED and serialized planTasks.
   * @throws IllegalArgumentException if scan request validation fails
   * @throws org.apache.gravitino.exceptions.NoSuchTableException if table doesn't exist
   * @throws RuntimeException for other scan planning failures
   */
  public PlanTableScanResponse planTableScan(
      TableIdentifier tableIdentifier, PlanTableScanRequest scanRequest) {

    LOG.debug(
        "Planning scan for table: {}, snapshotId: {}, startSnapshotId: {}, endSnapshotId: {}, select: {}, caseSensitive: {}",
        tableIdentifier,
        scanRequest.snapshotId(),
        scanRequest.startSnapshotId(),
        scanRequest.endSnapshotId(),
        scanRequest.select(),
        scanRequest.caseSensitive());

    try {
      Table table = catalog.loadTable(tableIdentifier);
      Optional<PlanTableScanResponse> cachedResponse =
          scanPlanCache.get(ScanPlanCacheKey.create(tableIdentifier, table, scanRequest));
      if (cachedResponse.isPresent()) {
        LOG.info("Using cached scan plan for table: {}", tableIdentifier);
        return cachedResponse.get();
      }

      List<String> planTasks = new ArrayList<>();
      Map<Integer, PartitionSpec> specsById = new HashMap<>();
      List<DeleteFile> deleteFiles = new ArrayList<>();

      try (CloseableIterable<FileScanTask> fileScanTasks =
          createFilePlanScanTasks(table, tableIdentifier, scanRequest)) {
        for (FileScanTask fileScanTask : fileScanTasks) {
          try {
            String taskString = ScanTaskParser.toJson(fileScanTask);
            planTasks.add(taskString);

            int specId = fileScanTask.spec().specId();
            if (!specsById.containsKey(specId)) {
              specsById.put(specId, fileScanTask.spec());
            }

            if (!fileScanTask.deletes().isEmpty()) {
              deleteFiles.addAll(fileScanTask.deletes());
            }
          } catch (Exception e) {
            throw new RuntimeException(
                String.format(
                    "Failed to serialize scan task for table: %s. Error: %s",
                    tableIdentifier, e.getMessage()),
                e);
          }
        }
      } catch (IOException e) {
        LOG.error("Failed to close scan task iterator for table: {}", tableIdentifier, e);
        throw new RuntimeException("Failed to plan scan tasks: " + e.getMessage(), e);
      }

      List<DeleteFile> uniqueDeleteFiles =
          deleteFiles.stream().distinct().collect(Collectors.toList());

      if (planTasks.isEmpty()) {
        LOG.info(
            "Scan planning returned no tasks for table: {}. Table may be empty or fully filtered.",
            tableIdentifier);
      }

      PlanTableScanResponse.Builder responseBuilder =
          PlanTableScanResponse.builder()
              .withPlanStatus(PlanStatus.COMPLETED)
              .withPlanTasks(planTasks)
              .withSpecsById(specsById);

      if (!uniqueDeleteFiles.isEmpty()) {
        responseBuilder.withDeleteFiles(uniqueDeleteFiles);
        LOG.debug(
            "Included {} delete files in scan plan for table: {}",
            uniqueDeleteFiles.size(),
            tableIdentifier);
      }

      PlanTableScanResponse response = responseBuilder.build();

      // Cache the scan plan response
      scanPlanCache.put(ScanPlanCacheKey.create(tableIdentifier, table, scanRequest), response);
      return response;

    } catch (IllegalArgumentException e) {
      LOG.error("Invalid scan request for table {}: {}", tableIdentifier, e.getMessage());
      throw new IllegalArgumentException("Invalid scan parameters: " + e.getMessage(), e);
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      LOG.error("Table not found during scan planning: {}", tableIdentifier);
      throw e;
    } catch (Exception e) {
      LOG.error("Unexpected error during scan planning for table: {}", tableIdentifier, e);
      throw new RuntimeException(
          "Scan planning failed for table " + tableIdentifier + ": " + e.getMessage(), e);
    }
  }

  /**
   * Create and plan a scan based on the scan request.
   *
   * <p>If both start and end snapshot IDs are provided, uses IncrementalAppendScan. Otherwise, uses
   * regular TableScan.
   *
   * @param table The table to scan
   * @param tableIdentifier The table identifier for logging
   * @param scanRequest The scan request parameters
   * @return CloseableIterable of FileScanTask
   */
  private CloseableIterable<FileScanTask> createFilePlanScanTasks(
      Table table, TableIdentifier tableIdentifier, PlanTableScanRequest scanRequest) {
    Long startSnapshotId = scanRequest.startSnapshotId();
    Long endSnapshotId = scanRequest.endSnapshotId();
    // Use IncrementalAppendScan if both start and end snapshot IDs are provided
    if (startSnapshotId != null && endSnapshotId != null) {
      LOG.debug(
          "Using IncrementalAppendScan for table: {}, from snapshot: {} to snapshot: {}",
          tableIdentifier,
          startSnapshotId,
          endSnapshotId);
      IncrementalAppendScan incrementalScan =
          table
              .newIncrementalAppendScan()
              .fromSnapshotInclusive(startSnapshotId)
              .toSnapshot(endSnapshotId);
      incrementalScan = applyScanRequest(incrementalScan, scanRequest);
      return incrementalScan.planFiles();
    } else {
      TableScan tableScan = table.newScan();
      // Snapshot ID 0 has no special meaning in Iceberg, so we only apply if not null
      if (scanRequest.snapshotId() != null) {
        tableScan = tableScan.useSnapshot(scanRequest.snapshotId());
        LOG.debug("Applied snapshot filter: snapshot-id={}", scanRequest.snapshotId());
      }
      tableScan = applyScanRequest(tableScan, scanRequest);
      return tableScan.planFiles();
    }
  }

  @SuppressWarnings("unchecked")
  private <T extends Scan> T applyScanRequest(T scan, PlanTableScanRequest scanRequest) {
    scan = (T) scan.caseSensitive(scanRequest.caseSensitive());
    LOG.debug("Applied case-sensitive: {}", scanRequest.caseSensitive());
    scan = applyScanFilter(scan, scanRequest);
    scan = applyScanSelect(scan, scanRequest);
    scan = applyScanStatsFields(scan, scanRequest);

    return scan;
  }

  @SuppressWarnings("unchecked")
  private <T extends Scan> T applyScanFilter(T scan, PlanTableScanRequest scanRequest) {
    if (scanRequest.filter() != null) {
      try {
        scan = (T) scan.filter(scanRequest.filter());
        LOG.debug("Applied filter expression: {}", scanRequest.filter());
      } catch (Exception e) {
        LOG.error("Failed to apply filter expression: {}", e.getMessage(), e);
        throw new IllegalArgumentException("Invalid filter expression: " + e.getMessage(), e);
      }
    }
    return scan;
  }

  @SuppressWarnings("unchecked")
  private <T extends Scan> T applyScanSelect(T scan, PlanTableScanRequest scanRequest) {
    if (scanRequest.select() != null && !scanRequest.select().isEmpty()) {
      try {
        scan = (T) scan.select(scanRequest.select());
        LOG.debug("Applied column projection: {}", scanRequest.select());
      } catch (Exception e) {
        LOG.error("Failed to apply column projection: {}", e.getMessage(), e);
        throw new IllegalArgumentException("Invalid column selection: " + e.getMessage(), e);
      }
    }
    return scan;
  }

  @SuppressWarnings("unchecked")
  private <T extends Scan> T applyScanStatsFields(T scan, PlanTableScanRequest scanRequest) {
    if (scanRequest.statsFields() != null && !scanRequest.statsFields().isEmpty()) {
      try {
        scan = (T) scan.includeColumnStats(scanRequest.statsFields());
        LOG.debug("Applied statistics fields: {}", scanRequest.statsFields());
      } catch (Exception e) {
        LOG.error("Failed to apply statistics fields: {}", e.getMessage(), e);
        throw new IllegalArgumentException("Invalid statistics fields: " + e.getMessage(), e);
      }
    }
    return scan;
  }

  private ScanPlanCache loadScanPlanCache(IcebergConfig config) {
    String impl = config.get(IcebergConfig.SCAN_PLAN_CACHE_IMPL);
    if (StringUtils.isBlank(impl)) {
      return ScanPlanCache.DUMMY;
    }

    ScanPlanCache cache =
        ClassUtils.loadAndGetInstance(impl, Thread.currentThread().getContextClassLoader());
    int capacity = config.get(IcebergConfig.SCAN_PLAN_CACHE_CAPACITY);
    int expireMinutes = config.get(IcebergConfig.SCAN_PLAN_CACHE_EXPIRE_MINUTES);
    cache.initialize(capacity, expireMinutes);
    LOG.info(
        "Load scan plan cache for catalog: {}, impl: {}, capacity: {}, expire minutes: {}",
        catalog.name(),
        impl,
        capacity,
        expireMinutes);
    return cache;
  }

  @VisibleForTesting
  static Map<String, String> checkForCompatibility(
      Map<String, String> properties, Map<String, String> deprecatedProperties) {
    Map<String, String> newProperties = new HashMap<>(properties);
    deprecatedProperties.forEach(
        (deprecatedProperty, newProperty) -> {
          replaceDeprecatedProperties(newProperties, deprecatedProperty, newProperty);
        });
    return newProperties;
  }

  private static void replaceDeprecatedProperties(
      Map<String, String> properties, String deprecatedProperty, String newProperty) {
    String deprecatedValue = properties.get(deprecatedProperty);
    String newValue = properties.get(newProperty);
    if (StringUtils.isNotBlank(deprecatedValue) && StringUtils.isNotBlank(newValue)) {
      throw new IllegalArgumentException(
          String.format("Should not set both %s and %s", deprecatedProperty, newProperty));
    }

    if (StringUtils.isNotBlank(deprecatedValue)) {
      LOG.warn("{} is deprecated, please use {} instead.", deprecatedProperty, newProperty);
      properties.remove(deprecatedProperty);
      properties.put(newProperty, deprecatedValue);
    }
  }

  private LoadTableResponse createTableInternal(Namespace namespace, CreateTableRequest request) {

    request.validate();

    if (request.stageCreate()) {
      return stageTableCreateInternal(namespace, request);
    }

    TableIdentifier ident = TableIdentifier.of(namespace, request.name());
    Table table =
        catalog
            .buildTable(ident, request.schema())
            .withLocation(request.location())
            .withPartitionSpec(request.spec())
            .withSortOrder(request.writeOrder())
            .withProperties(request.properties())
            .create();

    if (table instanceof BaseTable) {
      Map<String, String> properties = retrieveFileIOProperties(table.io());
      return LoadTableResponse.builder()
          .withTableMetadata(((BaseTable) table).operations().current())
          .addAllConfig(
              MapUtils.getFilteredMap(
                  properties, key -> catalogPropertiesToClientKeys.contains(key)))
          // Keep only credential fields from FileIO properties before returning them to the client.
          .addAllConfig(CredentialPropertyUtils.filterCredentialProperties(properties))
          .build();
    }

    throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
  }

  private LoadTableResponse stageTableCreateInternal(
      Namespace namespace, CreateTableRequest request) {
    TableIdentifier ident = TableIdentifier.of(namespace, request.name());
    if (catalog.tableExists(ident)) {
      throw new AlreadyExistsException("Table already exists: %s", ident);
    }

    Map<String, String> properties = Maps.newHashMap();
    properties.put("created-at", OffsetDateTime.now(ZoneOffset.UTC).toString());
    properties.putAll(request.properties());

    Map<String, String> config = Maps.newHashMap();
    String location;
    if (request.location() != null) {
      location = request.location();
    } else {
      Table table =
          catalog
              .buildTable(ident, request.schema())
              .withPartitionSpec(request.spec())
              .withSortOrder(request.writeOrder())
              .withProperties(properties)
              .createTransaction()
              .table();
      Map<String, String> tableProperties = retrieveFileIOProperties(table.io());
      config.putAll(
          MapUtils.getFilteredMap(
              tableProperties, key -> catalogPropertiesToClientKeys.contains(key)));
      config.putAll(CredentialPropertyUtils.filterCredentialProperties(tableProperties));
      location = table.location();
    }

    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            request.schema(),
            request.spec() != null ? request.spec() : PartitionSpec.unpartitioned(),
            request.writeOrder() != null ? request.writeOrder() : SortOrder.unsorted(),
            location,
            properties);

    return LoadTableResponse.builder().withTableMetadata(metadata).addAllConfig(config).build();
  }

  private LoadTableResponse loadTableInternal(TableIdentifier ident) {
    Table table = catalog.loadTable(ident);

    if (table instanceof BaseTable) {
      Map<String, String> properties = retrieveFileIOProperties(table.io());
      return LoadTableResponse.builder()
          .withTableMetadata(((BaseTable) table).operations().current())
          .addAllConfig(
              MapUtils.getFilteredMap(
                  properties, key -> catalogPropertiesToClientKeys.contains(key)))
          // Keep only credential fields from FileIO properties before returning them to the client.
          .addAllConfig(CredentialPropertyUtils.filterCredentialProperties(properties))
          .build();
    } else if (table instanceof BaseMetadataTable) {
      // metadata tables are loaded on the client side, return NoSuchTableException for now
      throw new NoSuchTableException("Table does not exist: %s", ident.toString());
    }

    throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
  }

  private static Map<String, String> retrieveFileIOProperties(FileIO fileIO) {
    return fileIO instanceof InMemoryFileIO ? Maps.newHashMap() : fileIO.properties();
  }
}
