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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.CatalogCredentialManager;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.CredentialPropertyUtils;
import org.apache.gravitino.credential.PathBasedCredentialContext;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.storage.GCSProperties;
import org.apache.gravitino.utils.MapUtils;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ScanTaskParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.rest.PlanStatus;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.responses.ImmutableLoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;

/** Process Iceberg REST specific operations, like credential vending. */
public class CatalogWrapperForREST extends IcebergCatalogWrapper {

  private final CatalogCredentialManager catalogCredentialManager;

  private final Map<String, String> catalogConfigToClients;

  private final ScanPlanCache scanPlanCache;

  private static final Set<String> catalogPropertiesToClientKeys =
      ImmutableSet.of(
          IcebergConstants.IO_IMPL,
          IcebergConstants.AWS_S3_REGION,
          IcebergConstants.ICEBERG_S3_ENDPOINT,
          IcebergConstants.ICEBERG_OSS_ENDPOINT,
          IcebergConstants.ICEBERG_S3_PATH_STYLE_ACCESS);

  @SuppressWarnings("deprecation")
  private static Map<String, String> deprecatedProperties =
      ImmutableMap.of(
          CredentialConstants.CREDENTIAL_PROVIDER_TYPE,
          CredentialConstants.CREDENTIAL_PROVIDERS,
          "gcs-credential-file-path",
          GCSProperties.GRAVITINO_GCS_SERVICE_ACCOUNT_FILE);

  public CatalogWrapperForREST(String catalogName, IcebergConfig config) {
    super(config);
    this.catalogConfigToClients =
        MapUtils.getFilteredMap(
            config.getIcebergCatalogProperties(),
            key -> catalogPropertiesToClientKeys.contains(key));
    // To be compatible with old properties
    Map<String, String> catalogProperties =
        checkForCompatibility(config.getAllConfig(), deprecatedProperties);
    this.catalogCredentialManager = new CatalogCredentialManager(catalogName, catalogProperties);
    // Initialize scan plan cache.
    this.scanPlanCache =
        new ScanPlanCache(
            config.get(IcebergConfig.SCAN_PLAN_CACHE_CAPACITY),
            config.get(IcebergConfig.SCAN_PLAN_CACHE_EXPIRE_MINUTES));
  }

  public LoadTableResponse createTable(
      Namespace namespace, CreateTableRequest request, boolean requestCredential) {
    LoadTableResponse loadTableResponse = super.createTable(namespace, request);
    if (requestCredential) {
      return injectCredentialConfig(
          TableIdentifier.of(namespace, request.name()), loadTableResponse);
    }
    return loadTableResponse;
  }

  public LoadTableResponse loadTable(TableIdentifier identifier, boolean requestCredential) {
    LoadTableResponse loadTableResponse = super.loadTable(identifier);
    if (requestCredential) {
      return injectCredentialConfig(identifier, loadTableResponse);
    }
    return loadTableResponse;
  }

  /**
   * Get table credentials.
   *
   * @param identifier The table identifier for which to load credentials
   * @return A {@link org.apache.iceberg.rest.responses.LoadCredentialsResponse} object containing
   *     the credentials.
   */
  public LoadCredentialsResponse getTableCredentials(TableIdentifier identifier) {
    try {
      LoadTableResponse loadTableResponse = super.loadTable(identifier);
      Credential credential = getCredential(loadTableResponse);
      org.apache.iceberg.rest.credentials.Credential icebergCredential =
          new org.apache.iceberg.rest.credentials.Credential() {
            @Override
            public String prefix() {
              return "";
            }

            @Override
            public Map<String, String> config() {
              return CredentialPropertyUtils.toIcebergProperties(credential);
            }

            @Override
            public void validate() {}
          };
      return ImmutableLoadCredentialsResponse.builder().addCredentials(icebergCredential).build();
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

  private LoadTableResponse injectCredentialConfig(
      TableIdentifier tableIdentifier, LoadTableResponse loadTableResponse) {
    final Credential credential = getCredential(loadTableResponse);

    LOG.info(
        "Generate credential: {} for Iceberg table: {}",
        credential.credentialType(),
        tableIdentifier);

    Map<String, String> credentialConfig = CredentialPropertyUtils.toIcebergProperties(credential);
    return LoadTableResponse.builder()
        .withTableMetadata(loadTableResponse.tableMetadata())
        .addAllConfig(loadTableResponse.config())
        .addAllConfig(getCatalogConfigToClient())
        .addAllConfig(credentialConfig)
        .build();
  }

  private Credential getCredential(LoadTableResponse loadTableResponse) {
    TableMetadata tableMetadata = loadTableResponse.tableMetadata();
    String[] path =
        Stream.of(
                tableMetadata.location(),
                tableMetadata.property(TableProperties.WRITE_DATA_LOCATION, ""),
                tableMetadata.property(TableProperties.WRITE_METADATA_LOCATION, ""))
            .filter(StringUtils::isNotBlank)
            .toArray(String[]::new);

    PathBasedCredentialContext context =
        new PathBasedCredentialContext(
            PrincipalUtils.getCurrentUserName(), ImmutableSet.copyOf(path), Collections.emptySet());
    Credential credential = catalogCredentialManager.getCredential(context);
    if (credential == null) {
      throw new ServiceUnavailableException("Couldn't generate credential, %s", context);
    }
    return credential;
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
        "Planning scan for table: {}, snapshotId: {}, select: {}, caseSensitive: {}",
        tableIdentifier,
        scanRequest.snapshotId(),
        scanRequest.select(),
        scanRequest.caseSensitive());

    try {
      Table table = catalog.loadTable(tableIdentifier);
      if (table == null) {
        throw new NoSuchTableException("Table not found: %s", tableIdentifier);
      }

      if (scanPlanCache != null) {
        PlanTableScanResponse cachedResponse = scanPlanCache.get(table, scanRequest);
        if (cachedResponse != null) {
          LOG.info("Using cached scan plan for table: {}", tableIdentifier);
          return cachedResponse;
        }
      }

      TableScan tableScan = table.newScan();
      tableScan = applyScanRequest(tableScan, scanRequest);

      List<String> planTasks = new ArrayList<>();
      Map<Integer, PartitionSpec> specsById = new HashMap<>();
      List<org.apache.iceberg.DeleteFile> deleteFiles = new ArrayList<>();

      try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
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
            LOG.warn(
                "Failed to serialize scan task for table: {}, skipping task. Error: {}",
                tableIdentifier,
                e.getMessage());
          }
        }
      } catch (IOException e) {
        LOG.error("Failed to close scan task iterator for table: {}", tableIdentifier, e);
        throw new RuntimeException("Failed to plan scan tasks: " + e.getMessage(), e);
      }

      List<DeleteFile> uniqueDeleteFiles =
          deleteFiles.stream().distinct().collect(java.util.stream.Collectors.toList());

      if (planTasks.isEmpty()) {
        LOG.info(
            "Scan planning returned no tasks for table: {}. Table may be empty or fully filtered.",
            tableIdentifier);
      }

      if (!planTasks.isEmpty() && specsById.isEmpty()) {
        LOG.error(
            "Internal error: planTasks is not empty ({} tasks) but specsById is empty for table: {}",
            planTasks.size(),
            tableIdentifier);
        throw new IllegalStateException("Scan planning produced tasks but no partition specs");
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

      String snapshotInfo =
          scanRequest.snapshotId() != null ? String.valueOf(scanRequest.snapshotId()) : "current";
      LOG.info(
          "Successfully planned {} scan tasks for table: {}, snapshot: {}",
          planTasks.size(),
          tableIdentifier,
          snapshotInfo);

      if (scanPlanCache != null) {
        scanPlanCache.put(table, scanRequest, response);
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

  private TableScan applyScanRequest(TableScan tableScan, PlanTableScanRequest scanRequest) {
    if (scanRequest.snapshotId() != null && scanRequest.snapshotId() != 0L) {
      tableScan = tableScan.useSnapshot(scanRequest.snapshotId());
      LOG.debug("Applied snapshot filter: snapshot-id={}", scanRequest.snapshotId());
    }

    tableScan = tableScan.caseSensitive(scanRequest.caseSensitive());
    LOG.debug("Applied case-sensitive: {}", scanRequest.caseSensitive());

    tableScan = applyScanFilter(tableScan, scanRequest);
    tableScan = applyScanSelect(tableScan, scanRequest);
    tableScan = applyScanStatsFields(tableScan, scanRequest);

    return tableScan;
  }

  private TableScan applyScanFilter(TableScan tableScan, PlanTableScanRequest scanRequest) {
    if (scanRequest.filter() != null) {
      try {
        tableScan = tableScan.filter(scanRequest.filter());
        LOG.debug("Applied filter expression: {}", scanRequest.filter());
      } catch (Exception e) {
        LOG.error("Failed to apply filter expression: {}", e.getMessage(), e);
        throw new IllegalArgumentException("Invalid filter expression: " + e.getMessage(), e);
      }
    }
    return tableScan;
  }

  private TableScan applyScanSelect(TableScan tableScan, PlanTableScanRequest scanRequest) {
    if (scanRequest.select() != null && !scanRequest.select().isEmpty()) {
      try {
        tableScan = tableScan.select(scanRequest.select());
        LOG.debug("Applied column projection: {}", scanRequest.select());
      } catch (Exception e) {
        LOG.error("Failed to apply column projection: {}", e.getMessage(), e);
        throw new IllegalArgumentException("Invalid column selection: " + e.getMessage(), e);
      }
    }
    return tableScan;
  }

  private TableScan applyScanStatsFields(TableScan tableScan, PlanTableScanRequest scanRequest) {
    if (scanRequest.statsFields() != null && !scanRequest.statsFields().isEmpty()) {
      try {
        tableScan = tableScan.includeColumnStats(scanRequest.statsFields());
        LOG.debug("Applied statistics fields: {}", scanRequest.statsFields());
      } catch (Exception e) {
        LOG.error("Failed to apply statistics fields: {}", e.getMessage(), e);
        throw new IllegalArgumentException("Invalid statistics fields: " + e.getMessage(), e);
      }
    }
    return tableScan;
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
