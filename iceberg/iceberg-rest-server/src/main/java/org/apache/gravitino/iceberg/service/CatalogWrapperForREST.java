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
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
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
  public void close() {
    if (catalogCredentialManager != null) {
      catalogCredentialManager.close();
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
   *     etc. Must provide either snapshotId or both startSnapshotId and endSnapshotId.
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
      // 1. Load the table
      Table table = catalog.loadTable(tableIdentifier);
      if (table == null) {
        throw new NoSuchTableException("Table not found: %s", tableIdentifier);
      }

      // 2. Create a table scan
      TableScan tableScan = table.newScan();

      // 3. Apply scan parameters (filters, projections, case-sensitive, snapshot, options)
      tableScan = applyScanRequest(tableScan, scanRequest);

      // 4. Plan scan tasks - serialize to plan task strings and collect specs
      List<String> planTasks = new ArrayList<>();
      Map<Integer, PartitionSpec> specsById = new HashMap<>();

      // Use try-with-resources to ensure CloseableIterable is properly closed
      try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
        for (FileScanTask fileScanTask : fileScanTasks) {

          // Serialize FileScanTask to JSON string using Iceberg's standard parser
          try {
            String taskString = org.apache.iceberg.ScanTaskParser.toJson(fileScanTask);
            planTasks.add(taskString);

            // Collect partition specs (required for proper deserialization by client)
            int specId = fileScanTask.spec().specId();
            if (!specsById.containsKey(specId)) {
              specsById.put(specId, fileScanTask.spec());
            }
          } catch (Exception e) {
            LOG.warn(
                "Failed to serialize scan task for table: {}, skipping task. Error: {}",
                tableIdentifier,
                e.getMessage());
            // Continue with next task instead of failing entire scan
          }
        }
      } catch (IOException e) {
        LOG.error("Failed to close scan task iterator for table: {}", tableIdentifier, e);
        throw new RuntimeException("Failed to plan scan tasks: " + e.getMessage(), e);
      }

      if (planTasks.isEmpty()) {
        LOG.info(
            "Scan planning returned no tasks for table: {}. Table may be empty or fully filtered.",
            tableIdentifier);
      }

      // Ensure we have at least one partition spec if we have tasks
      if (!planTasks.isEmpty() && specsById.isEmpty()) {
        LOG.error(
            "Internal error: planTasks is not empty ({} tasks) but specsById is empty for table: {}",
            planTasks.size(),
            tableIdentifier);
        throw new IllegalStateException("Scan planning produced tasks but no partition specs");
      }

      // 5. Build response following Iceberg REST API format
      // Note: For synchronous scan planning (COMPLETED status), use planTasks not fileScanTasks
      PlanTableScanResponse response =
          PlanTableScanResponse.builder()
              .withPlanStatus(org.apache.iceberg.rest.PlanStatus.COMPLETED)
              .withPlanTasks(planTasks)
              .withSpecsById(specsById)
              .build();

      // Log snapshot information (handle null snapshot for empty tables)
      String snapshotInfo =
          scanRequest.snapshotId() != null ? String.valueOf(scanRequest.snapshotId()) : "current";
      LOG.info(
          "Successfully planned {} scan tasks for table: {}, snapshot: {}",
          planTasks.size(),
          tableIdentifier,
          snapshotInfo);

      return response;

    } catch (IllegalArgumentException e) {
      LOG.error("Invalid scan request for table {}: {}", tableIdentifier, e.getMessage());
      throw new IllegalArgumentException("Invalid scan parameters: " + e.getMessage(), e);
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      LOG.error("Table not found during scan planning: {}", tableIdentifier);
      throw new org.apache.iceberg.exceptions.NoSuchTableException(
          "Table not found: %s", tableIdentifier);
    } catch (Exception e) {
      LOG.error("Unexpected error during scan planning for table: {}", tableIdentifier, e);
      throw new RuntimeException(
          "Scan planning failed for table " + tableIdentifier + ": " + e.getMessage(), e);
    }
  }

  /**
   * Apply scan parameters from PlanTableScanRequest to TableScan.
   *
   * <p>Configures the table scan with parameters from the request following Iceberg REST API spec:
   *
   * <ul>
   *   <li>Snapshot ID (if provided)
   *   <li>Case sensitivity for column name matching
   *   <li>Filter expressions
   *   <li>Column projection (select)
   * </ul>
   *
   * @param tableScan The table scan to configure
   * @param scanRequest The scan request parameters from client
   * @return Configured table scan ready for planning
   */
  private TableScan applyScanRequest(TableScan tableScan, PlanTableScanRequest scanRequest) {
    // Apply snapshot-id (if provided and not 0)
    if (scanRequest.snapshotId() != null && scanRequest.snapshotId() != 0L) {
      tableScan = tableScan.useSnapshot(scanRequest.snapshotId());
      LOG.debug("Applied snapshot filter: snapshot-id={}", scanRequest.snapshotId());
    }

    // Apply case-sensitive - this affects how filters and selects work
    tableScan = tableScan.caseSensitive(scanRequest.caseSensitive());
    LOG.debug("Applied case-sensitive: {}", scanRequest.caseSensitive());

    // Apply filter (expression filtering)
    tableScan = applyScanFilter(tableScan, scanRequest);

    // Apply select (column projection)
    tableScan = applyScanSelect(tableScan, scanRequest);

    return tableScan;
  }

  /**
   * Apply filter expression to table scan.
   *
   * @param tableScan The table scan to configure
   * @param scanRequest The scan request containing filter expression
   * @return Table scan with filter applied
   */
  private TableScan applyScanFilter(TableScan tableScan, PlanTableScanRequest scanRequest) {
    if (scanRequest.filter() != null) {
      try {
        Expression filter = scanRequest.filter();
        tableScan = tableScan.filter(filter);
        LOG.debug("Applied filter expression: {}", filter);
      } catch (Exception e) {
        LOG.error("Failed to apply filter expression: {}", e.getMessage(), e);
        throw new IllegalArgumentException("Invalid filter expression: " + e.getMessage(), e);
      }
    }
    return tableScan;
  }

  /**
   * Apply column projection to table scan.
   *
   * @param tableScan The table scan to configure
   * @param scanRequest The scan request containing selected columns
   * @return Table scan with column projection applied
   */
  private TableScan applyScanSelect(TableScan tableScan, PlanTableScanRequest scanRequest) {
    if (scanRequest.select() != null && !scanRequest.select().isEmpty()) {
      try {
        List<String> selectedColumns = scanRequest.select();
        tableScan = tableScan.select(selectedColumns);
        LOG.debug("Applied column projection: {}", selectedColumns);
      } catch (Exception e) {
        LOG.error("Failed to apply column projection: {}", e.getMessage(), e);
        throw new IllegalArgumentException("Invalid column selection: " + e.getMessage(), e);
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
