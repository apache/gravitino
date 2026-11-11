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
import java.io.IOException;
import java.net.URI;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.CatalogCredentialManager;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.CredentialPrivilege;
import org.apache.gravitino.credential.PathBasedCredentialContext;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.iceberg.service.cache.ScanPlanCache;
import org.apache.gravitino.iceberg.service.cache.ScanPlanCacheKey;
import org.apache.gravitino.storage.GCSProperties;
import org.apache.gravitino.utils.ClassUtils;
import org.apache.gravitino.utils.MapUtils;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.Scan;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.PlanStatus;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ImmutableLoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.apache.iceberg.util.SnapshotUtil;

/** Process Iceberg REST specific operations, like credential vending. */
public class CatalogWrapperForREST extends IcebergCatalogWrapper {

  /** Vends Gravitino-managed credentials and exposes the catalog name for credential refresh. */
  protected final CatalogCredentialManager catalogCredentialManager;

  private volatile Map<String, String> catalogConfigToClients;
  private final Object catalogConfigToClientsLock = new Object();

  private final ScanPlanCache scanPlanCache;

  private static final String DATA_ACCESS_VENDED_CREDENTIALS = "vended-credentials";
  private static final String DATA_ACCESS_REMOTE_SIGNING = "remote-signing";

  /**
   * Client-facing catalog property keys retained when building the IRC {@code /v1/config} defaults
   * and when extracting FileIO-derived config in {@link FederatedCatalogWrapper}.
   */
  protected static final Set<String> catalogPropertiesToClientKeys =
      ImmutableSet.of(
          IcebergConstants.IO_IMPL,
          IcebergConstants.AWS_S3_REGION,
          IcebergConstants.ICEBERG_S3_ENDPOINT,
          IcebergConstants.ICEBERG_OSS_ENDPOINT,
          IcebergConstants.ICEBERG_S3_PATH_STYLE_ACCESS,
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
    // To be compatible with old properties
    Map<String, String> catalogProperties =
        checkForCompatibility(config.getAllConfig(), deprecatedProperties);
    this.catalogCredentialManager = new CatalogCredentialManager(catalogName, catalogProperties);
    this.scanPlanCache = loadScanPlanCache(config);
  }

  public LoadTableResponse createTable(
      Namespace namespace, CreateTableRequest request, boolean requestCredential) {
    LoadTableResponse loadTableResponse = super.createTable(namespace, request);
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
    LoadTableResponse loadTableResponse = super.loadTable(identifier);
    if (shouldGenerateCredential(loadTableResponse, requestCredential)) {
      return injectCredentialConfig(identifier, loadTableResponse, privilege);
    }
    return loadTableResponse;
  }

  /**
   * Registers a table. An overwrite with a metadata file that belongs to a different table (another
   * table UUID) is refused before anything is written: Iceberg would replace the table and then
   * fail the request when it reads the replaced table back.
   *
   * @param namespace the namespace of the table
   * @param request the register request
   * @return the registered table
   */
  @Override
  public LoadTableResponse registerTable(Namespace namespace, RegisterTableRequest request) {
    checkOverwriteKeepsTableUuid(namespace, request);
    return super.registerTable(namespace, request);
  }

  /**
   * Commits table updates, with {@link IcebergCommitValidation} checking the updates against the
   * metadata they are applied to.
   *
   * @param tableIdentifier the table to update
   * @param updateTableRequest the updates and requirements
   * @return the updated table
   */
  @Override
  public LoadTableResponse updateTable(
      TableIdentifier tableIdentifier, UpdateTableRequest updateTableRequest) {
    UpdateTableRequest request =
        IcebergCommitValidation.assignPartitionFieldIds(
            updateTableRequest, () -> currentMetadata(tableIdentifier));
    return super.updateTable(
        tableIdentifier,
        IcebergCommitValidation.withValidation(
            request, () -> getCatalog().loadTable(tableIdentifier).io()));
  }

  @Nullable
  private TableMetadata currentMetadata(TableIdentifier tableIdentifier) {
    Table table = getCatalog().loadTable(tableIdentifier);
    return table instanceof HasTableOperations
        ? ((HasTableOperations) table).operations().current()
        : null;
  }

  public LoadTableResponse registerTable(
      Namespace namespace, RegisterTableRequest request, boolean requestCredential) {
    LoadTableResponse loadTableResponse = registerTable(namespace, request);
    if (shouldGenerateCredential(loadTableResponse, requestCredential)) {
      // Vend WRITE credentials: the registering user becomes the table owner
      // (IcebergNamespaceHookDispatcher.setTableOwner runs after this call
      // returns), consistent with createTable which also vends WRITE.
      return injectCredentialConfig(
          TableIdentifier.of(namespace, request.name()),
          loadTableResponse,
          CredentialPrivilege.WRITE);
    }
    return loadTableResponse;
  }

  /**
   * Get table credentials from the local (Gravitino-managed) catalog backend.
   *
   * <p>{@link FederatedCatalogWrapper} overrides this to fetch credentials from the remote REST
   * catalog instead, so the base implementation never needs an {@code instanceof RESTCatalog}
   * check.
   *
   * @param identifier table identifier
   * @param privilege used for local credential vending
   * @return table credentials response
   */
  public LoadCredentialsResponse getTableCredentials(
      TableIdentifier identifier, CredentialPrivilege privilege) {
    try {
      LoadTableResponse loadTableResponse = super.loadTable(identifier);
      Credential credential = getCredential(loadTableResponse.tableMetadata(), privilege);
      return ImmutableLoadCredentialsResponse.builder()
          .addCredentials(
              IcebergRESTUtils.toRESTCredential(
                  catalogCredentialManager.catalogName(),
                  identifier,
                  credential,
                  loadTableResponse.tableMetadata()))
          .build();
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
    Map<String, String> configToClients = catalogConfigToClients;
    if (configToClients != null) {
      return configToClients;
    }

    synchronized (catalogConfigToClientsLock) {
      if (catalogConfigToClients == null) {
        catalogConfigToClients = buildCatalogConfigToClients();
      }
      return catalogConfigToClients;
    }
  }

  /**
   * Builds properties exposed to Iceberg clients via the IRC {@code /v1/config} defaults.
   *
   * <p>The base implementation uses the static Gravitino catalog configuration as the property
   * source. {@link FederatedCatalogWrapper} overrides this to use {@code RESTCatalog.properties()}
   * so defaults reflect the remote catalog's config response merged with client properties (after
   * REST handshake).
   *
   * <p>{@link IcebergConstants#IO_IMPL} is passed through when present (e.g. Iceberg {@link
   * org.apache.iceberg.io.ResolvingFileIO}), so clients multiplex by URI scheme without server-side
   * rewriting per table.
   *
   * @return the immutable, filtered properties exposed to Iceberg clients.
   */
  @VisibleForTesting
  Map<String, String> buildCatalogConfigToClients() {
    return filterCatalogConfigForClients(getIcebergConfig().getIcebergCatalogProperties());
  }

  /**
   * Keeps only the client-facing catalog properties and validates the data-access property.
   *
   * @param sourceProps the candidate properties to filter.
   * @return the immutable, filtered properties exposed to Iceberg clients.
   */
  protected static Map<String, String> filterCatalogConfigForClients(
      Map<String, String> sourceProps) {
    Map<String, String> filtered =
        new HashMap<>(
            MapUtils.getFilteredMap(
                sourceProps, key -> catalogPropertiesToClientKeys.contains(key)));
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

  @VisibleForTesting
  protected LoadTableResponse injectCredentialConfig(
      TableIdentifier tableIdentifier,
      LoadTableResponse loadTableResponse,
      CredentialPrivilege privilege) {
    final Credential credential = getCredential(loadTableResponse.tableMetadata(), privilege);

    LOG.info(
        "Generate credential: {} for Iceberg table: {}",
        credential.credentialType(),
        tableIdentifier);

    // Vend the temporary credential both as a first-class REST credential (storage-prefix scoped,
    // for Iceberg 1.7+ clients) and, for backward compatibility, flattened into the load-table
    // response config. This mirrors FederatedCatalogWrapper, which also populates both fields.
    org.apache.iceberg.rest.credentials.Credential restCredential =
        IcebergRESTUtils.toRESTCredential(
            catalogCredentialManager.catalogName(),
            tableIdentifier,
            credential,
            loadTableResponse.tableMetadata());
    return LoadTableResponse.builder()
        .withTableMetadata(loadTableResponse.tableMetadata())
        .addAllConfig(loadTableResponse.config())
        .addAllConfig(getCatalogConfigToClient())
        .addAllConfig(restCredential.config())
        .addCredential(restCredential)
        .build();
  }

  private Credential getCredential(TableMetadata tableMetadata, CredentialPrivilege privilege) {
    String[] path =
        Stream.of(
                tableMetadata.location(),
                tableMetadata.property(TableProperties.WRITE_DATA_LOCATION, ""),
                tableMetadata.property(TableProperties.WRITE_METADATA_LOCATION, ""))
            .filter(StringUtils::isNotBlank)
            .toArray(String[]::new);

    PathBasedCredentialContext context =
        privilege == CredentialPrivilege.WRITE
            ? new PathBasedCredentialContext(
                PrincipalUtils.getCurrentUserName(),
                ImmutableSet.copyOf(path),
                Collections.emptySet())
            : new PathBasedCredentialContext(
                PrincipalUtils.getCurrentUserName(),
                Collections.emptySet(),
                ImmutableSet.copyOf(path));
    return catalogCredentialManager
        .getCredentialByPath(tableMetadata.location(), context)
        .orElseThrow(
            () -> new ServiceUnavailableException("Couldn't generate credential, %s", context));
  }

  @VisibleForTesting
  protected boolean shouldGenerateCredential(
      LoadTableResponse loadTableResponse, boolean requestCredential) {
    if (!requestCredential) {
      return false;
    }

    validateCredentialLocation(loadTableResponse.tableMetadata().location());
    return canVendCredentials(loadTableResponse.tableMetadata().location());
  }

  /**
   * Whether this catalog can vend credentials for a location. The access-delegation header only
   * tells the server what the client supports, and the server may supply none of the mechanisms, so
   * a location no configured provider covers is served without credentials instead of failing the
   * request.
   */
  private boolean canVendCredentials(String location) {
    if (isLocalOrHdfsLocation(location)) {
      return false;
    }
    if (!catalogCredentialManager.supportsPath(location)) {
      LOG.debug(
          "No credential provider of catalog {} covers {}, serving without vended credentials",
          catalogCredentialManager.catalogName(),
          location);
      return false;
    }
    return true;
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
   * <p>Implementation uses synchronous scan planning (COMPLETED status) and returns structured
   * {@code file-scan-tasks} per the Iceberg 1.11 REST spec. It does not emit legacy {@code
   * plan-tasks} JSON strings, so clients built for Iceberg &lt; 1.11 are not supported. This is
   * different from asynchronous mode (SUBMITTED status) where a plan ID is returned for later
   * retrieval.
   *
   * <p>Referenced from Iceberg PR #13400 for scan planning implementation.
   *
   * @param tableIdentifier The table identifier.
   * @param scanRequest The scan request parameters including filters, projections, snapshot-id,
   *     etc.
   * @return PlanTableScanResponse with status=COMPLETED and file scan tasks.
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
      Table table = getCatalog().loadTable(tableIdentifier);
      Optional<PlanTableScanResponse> cachedResponse =
          scanPlanCache.get(ScanPlanCacheKey.create(tableIdentifier, table, scanRequest));
      if (cachedResponse.isPresent()) {
        LOG.info("Using cached scan plan for table: {}", tableIdentifier);
        return cachedResponse.get();
      }

      Schema residualFrom = null;
      Schema residualTo = null;
      Expression extraResidual = null;
      List<FileScanTask> fileScanTasks = new ArrayList<>();
      try (CloseableIterable<FileScanTask> scanTasks =
          createFilePlanScanTasks(table, tableIdentifier, scanRequest)) {
        // Recompute whether names were rewritten so residuals can be mapped back to the request
        // schema. createFilePlanScanTasks applies the same rewrite rules.
        if (scanRequest.snapshotId() != null && !scanRequest.useSnapshotSchema()) {
          long snapshotId = scanRequest.snapshotId();
          Optional<String> branch = IcebergScanPlanHelper.branchAt(table, snapshotId);
          if (!branch.isPresent()) {
            residualFrom = SnapshotUtil.schemaFor(table, snapshotId);
            residualTo = table.schema();
            // Keep the complete client filter as a residual. Extracting only
            // post-snapshot predicates by replacing others with always-true
            // collapses OR/NOT (e.g. id=1 OR new_col=2 → true).
            if (scanRequest.filter() != null) {
              extraResidual = scanRequest.filter();
            }
          }
        }
        for (FileScanTask fileScanTask : scanTasks) {
          fileScanTasks.add(fileScanTask);
        }
      } catch (IOException e) {
        LOG.error("Failed to close scan task iterator for table: {}", tableIdentifier, e);
        throw new RuntimeException("Failed to plan scan tasks: " + e.getMessage(), e);
      }
      fileScanTasks =
          IcebergScanPlanHelper.prepareTasksForResponse(
              fileScanTasks,
              table.specs(),
              residualTo,
              residualFrom,
              extraResidual,
              scanRequest.caseSensitive());

      if (fileScanTasks.isEmpty()) {
        LOG.info(
            "Scan planning returned no tasks for table: {}. Table may be empty or fully filtered.",
            tableIdentifier);
      }

      checkDeleteFilesCanBeReferenced(tableIdentifier, fileScanTasks);
      PlanTableScanResponse response;
      try {
        response = buildCompletedPlanTableScanResponse(table, fileScanTasks);
      } catch (Exception e) {
        LOG.error("Failed to build scan plan response for table: {}", tableIdentifier, e);
        throw new RuntimeException(
            String.format(
                "Failed to build scan plan response for table: %s. Error: %s",
                tableIdentifier, e.getMessage()),
            e);
      }

      // Cache the scan plan response
      scanPlanCache.put(ScanPlanCacheKey.create(tableIdentifier, table, scanRequest), response);
      return response;

    } catch (IllegalArgumentException | ValidationException | DateTimeParseException e) {
      // A filter is bound to the schema only while files are planned. Unknown columns and wrong
      // literal types usually surface as ValidationException; a timestamp without a zone compared
      // to timestamptz surfaces as DateTimeParseException. Broader DateTimeException is not
      // caught so unrelated server-side date/time bugs stay 5xx.
      LOG.info("Invalid scan request for table {}: {}", tableIdentifier, e.getMessage());
      throw new IllegalArgumentException(
          "Invalid scan parameters: " + withoutSchemaStruct(e.getMessage()), e);
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
   * Builds a synchronous COMPLETED scan plan response for Iceberg 1.11+ REST clients only.
   *
   * <p>Matches {@link CatalogHandlers#planTableScan}: {@code file-scan-tasks} plus {@code
   * specs-by-id} from {@link Table#specs()}. Does not populate legacy {@code plan-tasks} JSON
   * strings.
   *
   * <p>{@code specs-by-id} uses the table's full spec map ({@link Table#specs()}), not only
   * partition specs referenced by the returned {@code fileScanTasks}. That matches Iceberg 1.11
   * REST behavior and may include historical specs from prior partition evolution, including when a
   * filtered scan returns zero tasks.
   */
  @SuppressWarnings("deprecation")
  private static PlanTableScanResponse buildCompletedPlanTableScanResponse(
      Table table, List<FileScanTask> fileScanTasks) {
    return PlanTableScanResponse.builder()
        .withPlanStatus(PlanStatus.COMPLETED)
        .withFileScanTasks(fileScanTasks)
        .withSpecsById(
            IcebergScanPlanHelper.sanitizeSpecsForSerialization(
                table.specs(), fileScanTasks, table.schemas().values()))
        .build();
  }

  /**
   * Create and plan a scan based on the scan request.
   *
   * <p>If both start and end snapshot IDs are provided, uses IncrementalAppendScan. Otherwise, uses
   * regular TableScan.
   *
   * <p>With {@code use-snapshot-schema} false (the REST default, sent for branch scans) and a
   * {@code snapshot-id}, the request is written against the current table schema. A branch that
   * points at the snapshot is scanned with {@code useRef}, which binds to the current schema like a
   * client-side branch scan. If no branch points there any more, the snapshot is scanned and the
   * request's column names are mapped to the snapshot schema by field id.
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
    if (startSnapshotId != null && startSnapshotId.equals(endSnapshotId)) {
      // The start is exclusive, so the range from a snapshot to itself is empty.
      return CloseableIterable.empty();
    }
    if (startSnapshotId != null && endSnapshotId != null) {
      LOG.debug(
          "Using IncrementalAppendScan for table: {}, from snapshot: {} to snapshot: {}",
          tableIdentifier,
          startSnapshotId,
          endSnapshotId);
      IncrementalAppendScan incrementalScan =
          table
              .newIncrementalAppendScan()
              // The REST spec defines start-snapshot-id as exclusive, and Iceberg's REST client
              // sends it from appendsBetween/appendsAfter, which are exclusive as well.
              .fromSnapshotExclusive(startSnapshotId)
              .toSnapshot(endSnapshotId);
      incrementalScan = applyScanRequest(incrementalScan, scanRequest);
      return incrementalScan.planFiles();
    } else {
      TableScan tableScan = table.newScan();
      PlanTableScanRequest requestToApply = scanRequest;
      // Snapshot ID 0 has no special meaning in Iceberg, so we only apply if not null
      if (scanRequest.snapshotId() != null) {
        long snapshotId = scanRequest.snapshotId();
        Optional<String> branch =
            scanRequest.useSnapshotSchema()
                ? Optional.empty()
                : IcebergScanPlanHelper.branchAt(table, snapshotId);
        if (branch.isPresent()) {
          tableScan = tableScan.useRef(branch.get());
          LOG.debug("Applied branch {} for snapshot-id={}", branch.get(), snapshotId);
        } else {
          tableScan = tableScan.useSnapshot(snapshotId);
          if (!scanRequest.useSnapshotSchema()) {
            requestToApply =
                IcebergScanPlanHelper.rewriteForSnapshot(
                    scanRequest, table.schema(), SnapshotUtil.schemaFor(table, snapshotId));
          }
          LOG.debug("Applied snapshot filter: snapshot-id={}", snapshotId);
        }
      }
      tableScan = applyScanRequest(tableScan, requestToApply);
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
        Expression filter = scanRequest.filter();
        scan = (T) scan.filter(filter);
        LOG.debug("Applied filter expression: {}", filter);
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
      for (String field : scanRequest.statsFields()) {
        boolean known =
            scanRequest.caseSensitive()
                ? scan.schema().findField(field) != null
                : scan.schema().caseInsensitiveFindField(field) != null;
        if (!known) {
          throw new IllegalArgumentException("Invalid statistics fields: unknown field " + field);
        }
      }
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
        "Load scan plan cache, backend: {}, impl: {}, capacity: {}, expire minutes: {}",
        config.get(IcebergConfig.CATALOG_BACKEND),
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
  /**
   * Iceberg's plan response serializer refers to delete files by their location. Deletion vectors
   * that share one Puffin file have the same location, so every task would refer to whichever of
   * them was indexed last, and a client would apply the wrong deletion vector. Such a plan is
   * refused rather than answered wrongly.
   */
  @VisibleForTesting
  static void checkDeleteFilesCanBeReferenced(
      TableIdentifier tableIdentifier, List<FileScanTask> fileScanTasks) {
    Map<String, DeleteFile> byLocation = new HashMap<>();
    for (FileScanTask task : fileScanTasks) {
      for (DeleteFile deleteFile : task.deletes()) {
        DeleteFile other = byLocation.putIfAbsent(deleteFile.location(), deleteFile);
        if (other != null
            && !(Objects.equals(other.contentOffset(), deleteFile.contentOffset())
                && Objects.equals(other.referencedDataFile(), deleteFile.referencedDataFile()))) {
          throw new ServiceFailureException(
              "Server-side scan planning cannot return table %s: several deletion vectors are"
                  + " stored in %s, which the plan response format cannot reference separately;"
                  + " plan the scan on the client",
              tableIdentifier, deleteFile.location());
        }
      }
    }
  }

  private static String withoutSchemaStruct(String message) {
    // Iceberg's binding errors append the whole table struct; it is not useful to a client.
    return message == null ? null : message.replaceAll(" in struct: .*$", "");
  }

  private void checkOverwriteKeepsTableUuid(Namespace namespace, RegisterTableRequest request) {
    if (request == null || !request.overwrite() || request.metadataLocation() == null) {
      return;
    }
    TableIdentifier identifier = TableIdentifier.of(namespace, request.name());
    if (!tableExists(identifier)) {
      return;
    }
    String currentUuid = super.loadTable(identifier).tableMetadata().uuid();
    String newUuid;
    try {
      newUuid = loadTableMetadataFromLocation(request.metadataLocation()).uuid();
    } catch (RuntimeException e) {
      // A metadata file that cannot be read is reported by the register itself.
      return;
    }
    if (currentUuid != null && newUuid != null && !currentUuid.equals(newUuid)) {
      throw new BadRequestException(
          "Cannot overwrite table %s with metadata of a different table: current table UUID %s,"
              + " metadata location %s has table UUID %s",
          identifier, currentUuid, request.metadataLocation(), newUuid);
    }
  }
}
