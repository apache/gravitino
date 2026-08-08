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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.Scan;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchPlanTaskException;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.PlanStatus;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.FetchScanTasksRequest;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.responses.FetchScanTasksResponse;
import org.apache.iceberg.rest.responses.ImmutableLoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;

/** Process Iceberg REST specific operations, like credential vending. */
public class CatalogWrapperForREST extends IcebergCatalogWrapper {

  /** Vends Gravitino-managed credentials and exposes the catalog name for credential refresh. */
  protected final CatalogCredentialManager catalogCredentialManager;

  private volatile Map<String, String> catalogConfigToClients;
  private final Object catalogConfigToClientsLock = new Object();

  private final ScanPlanCache scanPlanCache;

  /** Maximum number of file scan tasks handed out inline by one scan planning response. */
  private final int scanPlanTaskBatchSize;

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
    this.scanPlanTaskBatchSize = config.get(IcebergConfig.SCAN_PLAN_TASK_BATCH_SIZE);
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

  public LoadTableResponse registerTable(
      Namespace namespace, RegisterTableRequest request, boolean requestCredential) {
    LoadTableResponse loadTableResponse = super.registerTable(namespace, request);
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
   * Plan table scan without credential vending.
   *
   * @param tableIdentifier The table identifier.
   * @param scanRequest The scan request parameters.
   * @return PlanTableScanResponse with status=COMPLETED and file scan tasks.
   */
  public PlanTableScanResponse planTableScan(
      TableIdentifier tableIdentifier, PlanTableScanRequest scanRequest) {
    return planTableScan(tableIdentifier, scanRequest, false, CredentialPrivilege.READ);
  }

  /**
   * Plan table scan and optionally inject vended storage credentials.
   *
   * <p>This method performs server-side scan planning to optimize query performance by reducing
   * client-side metadata loading and enabling parallel task execution.
   *
   * <p>Implementation uses synchronous scan planning (COMPLETED status) and returns structured
   * {@code file-scan-tasks} per the Iceberg 1.11 REST spec. This is different from asynchronous
   * mode (SUBMITTED status) where a plan ID is returned for later retrieval.
   *
   * <p>At most {@link IcebergConfig#SCAN_PLAN_TASK_BATCH_SIZE} file scan tasks are returned inline.
   * When a scan plans more tasks than that, the remainder is handed out as {@code plan-tasks}
   * {@code plan-tasks} that the client exchanges for the rest of the tasks through {@link
   * #fetchScanTasks}, so one response never has to carry a plan of unbounded size.
   *
   * <p>When {@code requestCredentialVending} is true and the table is eligible (non-local,
   * non-HDFS), storage credentials are injected directly into the response using the table already
   * loaded for scan planning -- avoiding a redundant {@code loadTable} call.
   *
   * <p>Referenced from Iceberg PR #13400 for scan planning implementation.
   *
   * @param tableIdentifier The table identifier.
   * @param scanRequest The scan request parameters including filters, projections, snapshot-id,
   *     etc.
   * @param requestCredentialVending whether the client requested credential vending
   * @param privilege the credential privilege level for vending
   * @return PlanTableScanResponse with status=COMPLETED and file scan tasks.
   * @throws IllegalArgumentException if scan request validation fails
   * @throws org.apache.gravitino.exceptions.NoSuchTableException if table doesn't exist
   * @throws RuntimeException for other scan planning failures
   */
  public PlanTableScanResponse planTableScan(
      TableIdentifier tableIdentifier,
      PlanTableScanRequest scanRequest,
      boolean requestCredentialVending,
      CredentialPrivilege privilege) {

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

      // Pin the snapshot before planning so that plan tasks issued for this plan keep
      // resolving to the snapshot that was planned, even if the table changes meanwhile. Requests
      // that already name a snapshot, incremental requests (which pin a snapshot range) and tables
      // without a current snapshot are planned exactly as they came in.
      boolean snapshotAlreadyPinned =
          scanRequest.snapshotId() != null
              || scanRequest.startSnapshotId() != null
              || scanRequest.endSnapshotId() != null
              || table.currentSnapshot() == null;
      PlanTableScanRequest pinnedScanRequest =
          snapshotAlreadyPinned
              ? scanRequest
              : PlanTableScanRequest.builder()
                  .withSnapshotId(table.currentSnapshot().snapshotId())
                  .withSelect(scanRequest.select())
                  .withFilter(scanRequest.filter())
                  .withCaseSensitive(scanRequest.caseSensitive())
                  .withUseSnapshotSchema(scanRequest.useSnapshotSchema())
                  .withStatsFields(scanRequest.statsFields())
                  .withMinRowsRequested(scanRequest.minRowsRequested())
                  .build();

      PlanTableScanResponse fullPlan = planFullScan(tableIdentifier, table, pinnedScanRequest);
      PlanTableScanResponse response =
          splitIntoPlanTasks(tableIdentifier, pinnedScanRequest, fullPlan);

      if (requestCredentialVending && !isLocalOrHdfsLocation(table.location())) {
        response = injectScanCredentials(tableIdentifier, table, response, privilege);
      }
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
   * Fetch the scan tasks covered by a {@code plan-task} previously handed out by {@link
   * #planTableScan}, completing the second step of the Iceberg REST scan planning protocol.
   *
   * <p>A plan task describes its own unit of work (see {@link PlanTaskCodec}): the scan request the
   * plan was produced from, with the snapshot pinned at planning time, plus the range of file scan
   * tasks it stands for. The plan is reproduced here from the {@linkplain ScanPlanCache scan plan
   * cache} when it is still cached and re-planned against the pinned snapshot otherwise, then that
   * range is returned. Because no state is kept between the two calls, a plan task remains
   * redeemable after a server restart and on any Gravitino instance serving the same catalog.
   *
   * @param tableIdentifier the table the plan task belongs to.
   * @param request the request carrying the {@code plan-task}.
   * @return the file scan tasks the plan task covers.
   * @throws org.apache.iceberg.exceptions.NoSuchTableException if the table doesn't exist.
   * @throws NoSuchPlanTaskException if the plan task was not issued for this table, or the plan it
   *     refers to can no longer be reproduced (for example its snapshot has expired).
   */
  @SuppressWarnings("deprecation")
  public FetchScanTasksResponse fetchScanTasks(
      TableIdentifier tableIdentifier, FetchScanTasksRequest request) {
    Optional<PlanTaskCodec.PlanTask> decoded = PlanTaskCodec.decode(request.planTask());

    // Validate the table exists first, so a bad table reports 404 for the table rather than
    // masking it as an unknown plan task. Consistent with planTableScan behavior.
    Table table = getCatalog().loadTable(tableIdentifier);

    if (!decoded.isPresent() || !decoded.get().matchesTable(tableIdentifier)) {
      LOG.info(
          "Rejecting unknown plan task '{}' for table {}", request.planTask(), tableIdentifier);
      throw new NoSuchPlanTaskException(
          "Plan task %s was not issued for table %s", request.planTask(), tableIdentifier);
    }

    PlanTaskCodec.PlanTask planTask = decoded.get();
    PlanTableScanResponse fullPlan;
    try {
      fullPlan = planFullScan(tableIdentifier, table, planTask.scanRequest());
    } catch (IllegalArgumentException e) {
      // The pinned snapshot is gone (expired or rolled back), so the plan the plan task refers to
      // can no longer be reproduced. That is a stale plan task, not a bad request.
      LOG.info(
          "Plan task '{}' for table {} can no longer be planned: {}",
          request.planTask(),
          tableIdentifier,
          e.getMessage());
      throw new NoSuchPlanTaskException(
          "Plan task %s is no longer available for table %s: %s",
          request.planTask(), tableIdentifier, e.getMessage());
    }

    List<FileScanTask> allTasks = fullPlan.fileScanTasks();
    int taskCount = allTasks == null ? 0 : allTasks.size();
    if (planTask.offset() >= taskCount) {
      LOG.info(
          "Plan task '{}' for table {} covers tasks from offset {}, but the plan has {} tasks",
          request.planTask(),
          tableIdentifier,
          planTask.offset(),
          taskCount);
      throw new NoSuchPlanTaskException(
          "Plan task %s is no longer available for table %s", request.planTask(), tableIdentifier);
    }

    List<FileScanTask> batch =
        ImmutableList.copyOf(
            allTasks.subList(
                planTask.offset(), Math.min(planTask.offset() + planTask.limit(), taskCount)));
    LOG.info(
        "Returning {} file scan tasks for plan task of table {} at offset {}",
        batch.size(),
        tableIdentifier,
        planTask.offset());

    // withFileScanTasks derives the response's delete files from the tasks, so a merge-on-read
    // batch carries the delete files its tasks reference by index.
    return FetchScanTasksResponse.builder()
        .withFileScanTasks(batch)
        .withSpecsById(fullPlan.specsById())
        .build();
  }

  /**
   * Plans the whole scan and returns every file scan task inline, serving the {@linkplain
   * ScanPlanCache scan plan cache} when the same plan was computed before.
   *
   * <p>Tasks are ordered deterministically so that a plan task, which addresses tasks by position,
   * resolves to the same tasks on a later re-plan of the same snapshot.
   */
  private PlanTableScanResponse planFullScan(
      TableIdentifier tableIdentifier, Table table, PlanTableScanRequest scanRequest) {
    ScanPlanCacheKey cacheKey = ScanPlanCacheKey.create(tableIdentifier, table, scanRequest);
    Optional<PlanTableScanResponse> cachedResponse = scanPlanCache.get(cacheKey);
    if (cachedResponse.isPresent()) {
      LOG.info("Using cached scan plan for table: {}", tableIdentifier);
      return cachedResponse.get();
    }

    List<FileScanTask> fileScanTasks = new ArrayList<>();
    try (CloseableIterable<FileScanTask> scanTasks =
        createFilePlanScanTasks(table, tableIdentifier, scanRequest)) {
      for (FileScanTask fileScanTask : scanTasks) {
        fileScanTasks.add(fileScanTask);
      }
    } catch (IOException e) {
      LOG.error("Failed to close scan task iterator for table: {}", tableIdentifier, e);
      throw new RuntimeException("Failed to plan scan tasks: " + e.getMessage(), e);
    }

    if (fileScanTasks.isEmpty()) {
      LOG.info(
          "Scan planning returned no tasks for table: {}. Table may be empty or fully filtered.",
          tableIdentifier);
    }

    // Iceberg plans manifests in parallel, so the order tasks come back in is not reproducible.
    // Order them totally: a plan task addresses tasks by position, so those positions must
    // resolve to the same tasks across re-plans of the same snapshot.
    //
    // Data file location, offset and length do not order tasks totally on their own, because one
    // path can be referenced by several manifest entries - the same file appended in two snapshots,
    // for instance - and those entries carry different sequence numbers and delete files. A
    // manifest entry is unique within a snapshot, identified by its manifest and its position in
    // it, so comparing that as well makes the order total. Sequence numbers come first because they
    // survive a rewrite into new manifests; when a reader leaves the manifest fields unset they are
    // also the last discriminator, and tasks that tie on every key are interchangeable.
    fileScanTasks.sort(
        Comparator.comparing((FileScanTask task) -> task.file().location())
            .thenComparingLong(FileScanTask::start)
            .thenComparingLong(FileScanTask::length)
            .thenComparing(
                task -> task.file().dataSequenceNumber(), Comparator.nullsFirst(Long::compareTo))
            .thenComparing(
                task -> task.file().fileSequenceNumber(), Comparator.nullsFirst(Long::compareTo))
            .thenComparing(
                task -> task.file().manifestLocation(), Comparator.nullsFirst(String::compareTo))
            .thenComparing(task -> task.file().pos(), Comparator.nullsFirst(Long::compareTo)));

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

    scanPlanCache.put(cacheKey, response);
    return response;
  }

  /**
   * Keeps the first {@link IcebergConfig#SCAN_PLAN_TASK_BATCH_SIZE} file scan tasks of {@code
   * fullPlan} inline and turns the remaining tasks into {@code plan-tasks}, so a single response
   * never carries an unbounded plan.
   *
   * <p>Returns {@code fullPlan} unchanged when batching is disabled or the plan already fits in one
   * batch, which is the common case and keeps a plan a client can consume without a second call.
   */
  @SuppressWarnings("deprecation")
  private PlanTableScanResponse splitIntoPlanTasks(
      TableIdentifier tableIdentifier,
      PlanTableScanRequest scanRequest,
      PlanTableScanResponse fullPlan) {
    List<FileScanTask> allTasks = fullPlan.fileScanTasks();
    if (scanPlanTaskBatchSize <= 0
        || allTasks == null
        || allTasks.size() <= scanPlanTaskBatchSize) {
      return fullPlan;
    }

    List<String> planTasks = new ArrayList<>();
    for (int offset = scanPlanTaskBatchSize;
        offset < allTasks.size();
        offset += scanPlanTaskBatchSize) {
      planTasks.add(
          PlanTaskCodec.encode(tableIdentifier, scanRequest, offset, scanPlanTaskBatchSize));
    }

    List<FileScanTask> firstBatch =
        ImmutableList.copyOf(allTasks.subList(0, scanPlanTaskBatchSize));
    LOG.info(
        "Split scan plan of table {} into {} inline file scan tasks and {} plan tasks",
        tableIdentifier,
        firstBatch.size(),
        planTasks.size());

    // withFileScanTasks derives the response's delete files from the tasks, so the inline batch
    // carries the delete files its own tasks reference by index.
    return PlanTableScanResponse.builder()
        .withPlanStatus(PlanStatus.COMPLETED)
        .withFileScanTasks(firstBatch)
        .withPlanTasks(planTasks)
        .withSpecsById(fullPlan.specsById())
        .build();
  }

  /**
   * Inject vended credentials into a scan response using the already-loaded table, avoiding a
   * redundant {@code loadTable} call. Follows the same eligibility logic as {@link
   * #shouldGenerateCredential} and the same credential generation as {@link #getCredential}.
   */
  private PlanTableScanResponse injectScanCredentials(
      TableIdentifier tableIdentifier,
      Table table,
      PlanTableScanResponse response,
      CredentialPrivilege privilege) {
    try {
      validateCredentialLocation(table.location());
      TableMetadata metadata = ((BaseTable) table).operations().current();
      Credential credential = getCredential(metadata, privilege);
      Map<String, String> config =
          new HashMap<>(CredentialPropertyUtils.toIcebergProperties(credential));
      config.putAll(
          IcebergRESTUtils.buildRefreshProps(
              catalogCredentialManager.catalogName(), tableIdentifier, config));
      org.apache.iceberg.rest.credentials.Credential restCred =
          IcebergRESTUtils.toRESTCredential(table.location(), config);
      return IcebergRESTUtils.copyWithCredentials(response, Collections.singletonList(restCred));
    } catch (ServiceUnavailableException e) {
      LOG.warn("Failed to generate scan credentials for table: {}", tableIdentifier, e);
      return response;
    }
  }

  /**
   * Builds a synchronous COMPLETED scan plan response holding the whole plan, for Iceberg 1.11+
   * REST clients only.
   *
   * <p>Matches {@link CatalogHandlers#planTableScan}: {@code file-scan-tasks} plus {@code
   * specs-by-id} from {@link Table#specs()}. Batching into {@code plan-tasks} happens later, in
   * {@link #splitIntoPlanTasks}, so the cached plan always holds every task.
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
        .withSpecsById(table.specs())
        .build();
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
}
