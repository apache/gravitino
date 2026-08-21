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
package org.apache.gravitino.trino.connector.catalog;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorContext;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.trino.connector.GravitinoConfig;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.catalog.iceberg.IcebergConnectorAdapter;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;
import org.apache.gravitino.trino.connector.security.GravitinoAuthProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class has the following main functions:
 *
 * <pre>
 * 1. Load catalogs from the Apache Gravitino server and create
 * catalog contexts.
 * 2. Manage all catalog context instances, which primarily handle communication
 * with Trino through Gravitino connectors and inner connectors related to the engine.
 * </pre>
 */
public class CatalogConnectorManager {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogConnectorManager.class);

  private static final int NUMBER_EXECUTOR_THREAD = 1;
  private static final int LOAD_METALAKE_TIMEOUT = 60;

  private int metadataUpdateIntervalSecond = 10;

  private final ScheduledExecutorService executorService;
  private final CatalogRegister catalogRegister;
  private final CatalogConnectorFactory catalogConnectorFactory;

  private final ConcurrentHashMap<String, CatalogConnectorContext> catalogConnectors =
      new ConcurrentHashMap<>();

  // The registration state of every catalog seen by the load loop, keyed by the Trino catalog
  // name. Written only by the load loop thread, read by query threads through the system tables.
  private final ConcurrentHashMap<String, CatalogRegistrationState> catalogStates =
      new ConcurrentHashMap<>();

  // The last error reported by each metalake, keyed by the metalake name.
  private final ConcurrentHashMap<String, String> metalakeErrors = new ConcurrentHashMap<>();

  private volatile boolean trinoStarted = false;
  private volatile long lastLoadAttemptTimeMs = 0L;
  private volatile long lastSuccessfulLoadTimeMs = 0L;
  private volatile String lastLoadError = null;
  private final AtomicLong consecutiveLoadFailures = new AtomicLong();

  private String targetMetalake;
  private final Map<String, GravitinoMetalake> metalakes = new ConcurrentHashMap<>();
  // Tracks which metalakes' Iceberg REST discovery is currently failing, so a failure is logged
  // at WARN only on the transition into/out of that state rather than on every poll.
  private final Set<String> icebergRestDiscoveryFailing = ConcurrentHashMap.newKeySet();

  private GravitinoAdminClient gravitinoClient;
  private GravitinoConfig config;
  private TrinoCatalogNameHandler trinoCatalogNameHandler;

  /**
   * Constructs a new CatalogConnectorManager with the specified catalog register and catalog
   * connector factory.
   *
   * @param catalogRegister the catalog register
   * @param catalogFactory the catalog connector factory
   */
  public CatalogConnectorManager(
      CatalogRegister catalogRegister,
      CatalogConnectorFactory catalogFactory,
      TrinoCatalogNameHandler trinoCatalogNameHandler) {
    this.catalogRegister = catalogRegister;
    this.catalogConnectorFactory = catalogFactory;
    this.executorService = createScheduledThreadPoolExecutor();
    this.trinoCatalogNameHandler = trinoCatalogNameHandler;
  }

  private static ScheduledThreadPoolExecutor createScheduledThreadPoolExecutor() {
    return new ScheduledThreadPoolExecutor(
        NUMBER_EXECUTOR_THREAD,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("gravitino-connector-schedule-%d")
            .setUncaughtExceptionHandler(
                (thread, throwable) ->
                    LOG.warn("{} uncaught exception:", thread.getName(), throwable))
            .build());
  }

  /**
   * Configures the catalog connector manager with the specified Gravitino configuration and client.
   *
   * @param config the Gravitino configuration
   * @param client the Gravitino admin client
   */
  public void config(GravitinoConfig config, GravitinoAdminClient client) {
    updateConfig(config);
    if (client == null) {
      String authType =
          config.getClientConfig().getOrDefault(GravitinoAuthProvider.AUTH_TYPE_KEY, "none");
      LOG.info("Building Gravitino client with authType: {}", authType);
      try {
        this.gravitinoClient = GravitinoAuthProvider.build(config);
      } catch (IllegalArgumentException e) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
            "Invalid Gravitino client configuration for authType '"
                + authType
                + "': "
                + e.getMessage(),
            e);
      } catch (RuntimeException e) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_RUNTIME_ERROR,
            "Runtime failure while building Gravitino client with authType '"
                + authType
                + "': "
                + e.getMessage(),
            e);
      }
    } else {
      this.gravitinoClient = client;
    }
  }

  /**
   * Updates the Gravitino configuration, leaving the Gravitino client untouched.
   *
   * <p>Used to re-apply the configuration of the static connector when a dynamic connector created
   * the manager first, so that the catalog register is started with the `trino.jdbc.*` settings
   * that are only present in the static configuration.
   *
   * @param config the Gravitino configuration
   */
  public void updateConfig(GravitinoConfig config) {
    Preconditions.checkArgument(config != null, "config must not be null");
    this.config = config;
    this.metadataUpdateIntervalSecond = Integer.parseInt(config.getMetadataRefreshIntervalSecond());
    this.targetMetalake = config.getMetalake();
  }

  /**
   * Starts the catalog connector manager with the specified Trino connector context.
   *
   * @throws Exception if the catalog connector manager fails to start
   */
  public void start() throws Exception {
    catalogRegister.init(config);
    executorService.scheduleWithFixedDelay(
        this::loadMetalake,
        metadataUpdateIntervalSecond,
        metadataUpdateIntervalSecond,
        TimeUnit.SECONDS);
    LOG.info("Gravitino CatalogConnectorManager started.");
  }

  private void loadMetalake() {
    lastLoadAttemptTimeMs = System.currentTimeMillis();
    try {
      if (!catalogRegister.isTrinoStarted()) {
        String message = "Waiting for the Trino server to start";
        if (!Objects.equals(lastLoadError, message)) {
          LOG.info("{}.", message);
        }
        lastLoadError = message;
        trinoStarted = false;
        return;
      }
      trinoStarted = true;

      Set<String> usedMetalakes = new HashSet<>();
      if (config.singleMetalakeMode()) {
        usedMetalakes.add(targetMetalake);
        metalakes.computeIfAbsent(targetMetalake, this::retrieveMetalake);
      } else {
        GravitinoMetalake[] allMetalakes = gravitinoClient.listMetalakes();
        for (GravitinoMetalake metalake : allMetalakes) {
          usedMetalakes.add(metalake.name());
          metalakes.put(metalake.name(), metalake);
        }
      }

      for (String usedMetalake : usedMetalakes) {
        try {
          GravitinoMetalake metalake = metalakes.get(usedMetalake);
          LOG.debug("Load metalake: {}", usedMetalake);
          if (config.isIcebergRestRoutingEnabled()
              && StringUtils.isBlank(config.getManualIcebergRestUri(usedMetalake))) {
            refreshIcebergRestUri(usedMetalake);
          }
          loadCatalogs(metalake);
        } catch (Exception e) {
          recordMetalakeError(usedMetalake, e);
        }
      }

      lastSuccessfulLoadTimeMs = System.currentTimeMillis();
      recordLoadSuccess();
    } catch (Exception e) {
      recordLoadFailure(toErrorMessage(e), e);
    }
  }

  private void recordLoadSuccess() {
    if (lastLoadError != null) {
      LOG.info("The Gravitino catalog load loop recovered.");
    }
    lastLoadError = null;
    consecutiveLoadFailures.set(0);
  }

  private void recordLoadFailure(String message, Exception cause) {
    boolean changed = !Objects.equals(lastLoadError, message);
    lastLoadError = message;
    consecutiveLoadFailures.incrementAndGet();
    if (changed) {
      LOG.error("Failed to load catalogs from the Gravitino server: {}", message, cause);
    } else {
      LOG.debug("Failed to load catalogs from the Gravitino server: {}", message, cause);
    }
  }

  private void recordMetalakeError(String metalakeName, Exception cause) {
    String message = toErrorMessage(cause);
    String previous = metalakeErrors.put(metalakeName, message);
    if (!Objects.equals(previous, message)) {
      LOG.error("Load metalake {} failed: {}", metalakeName, message, cause);
    } else {
      LOG.debug("Load metalake {} failed: {}", metalakeName, message, cause);
    }
  }

  private static String toErrorMessage(Exception e) {
    // Report the root cause: the actual reason a registration failed, such as "Access Denied:
    // Cannot create catalog", is wrapped in several layers of TrinoException by the time it gets
    // here, and the outer messages say nothing a user can act on. Do not use
    // GravitinoErrorCode.toSimpleErrorMessage(), it throws a NullPointerException when the
    // exception carries no message.
    Throwable rootCause = e;
    while (rootCause.getCause() != null && rootCause.getCause() != rootCause) {
      rootCause = rootCause.getCause();
    }
    String message = rootCause.getMessage();
    if (StringUtils.isBlank(message)) {
      message = e.getMessage();
    }
    return StringUtils.isBlank(message) ? e.getClass().getName() : message;
  }

  /**
   * Asks the Gravitino server whether it has an Iceberg REST server running for this metalake, and
   * caches the answer on the shared {@link GravitinoConfig} for {@code IcebergConnectorAdapter} to
   * read on the next catalog load. Failures — including talking to a Gravitino server older than
   * this endpoint — must not interrupt catalog loading, so they are swallowed here; Iceberg
   * catalogs simply keep their last known routing decision until the next successful poll. A
   * failure is logged at ERROR on every poll because routing through Iceberg REST is required when
   * enabled. Catalog loading continues so that unrelated catalogs remain available.
   */
  private void refreshIcebergRestUri(String metalakeName) {
    try {
      config.setDiscoveredIcebergRestUri(
          metalakeName, gravitinoClient.icebergRestServiceUri(metalakeName).orElse(null));
      if (icebergRestDiscoveryFailing.remove(metalakeName)) {
        LOG.info("Iceberg REST service discovery for metalake {} recovered.", metalakeName);
      }
    } catch (Exception e) {
      icebergRestDiscoveryFailing.add(metalakeName);
      LOG.error(
          "Failed to query the Iceberg REST service endpoint for metalake {}; Iceberg catalogs "
              + "without a configured REST endpoint cannot be registered until discovery "
              + "recovers. Set gravitino.iceberg.rest-uri explicitly, upgrade the Gravitino "
              + "server to one that supports discovery, or disable Iceberg REST routing with "
              + "gravitino.iceberg.rest-routing-enabled=false to use legacy backend translation.",
          metalakeName,
          e);
    }
  }

  /**
   * Retrieves a metalake by its name.
   *
   * @param metalakeName the name of the metalake
   * @return the metalake
   * @throws TrinoException if the metalake does not exist
   */
  public GravitinoMetalake retrieveMetalake(String metalakeName) {
    try {
      return gravitinoClient.loadMetalake(metalakeName);
    } catch (NoSuchMetalakeException e) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_METALAKE_NOT_EXISTS,
          "Metalake " + metalakeName + " not exists.");
    }
  }

  private void loadCatalogs(GravitinoMetalake metalake) {
    String metalakeName = metalake.name();
    String[] allCatalogNames;
    try {
      allCatalogNames = metalake.listCatalogs();
    } catch (Exception e) {
      // Keep the existing catalog states untouched, a transient listing failure must not turn
      // healthy catalogs into failed ones. The load status system table reports the cause.
      recordMetalakeError(metalakeName, e);
      return;
    }
    metalakeErrors.remove(metalakeName);

    // The Trino names of every catalog the Gravitino server currently reports, including the
    // catalogs that are intentionally not registered.
    Set<String> presentTrinoNames = new HashSet<>();
    List<String> catalogNames = new ArrayList<>();
    for (String catalogName : allCatalogNames) {
      String trinoCatalogName = getTrinoCatalogName(metalakeName, catalogName);
      presentTrinoNames.add(trinoCatalogName);
      if (skipCatalog(trinoCatalogName)) {
        recordCatalogState(
            CatalogRegistrationState.notLoaded(
                metalakeName,
                catalogName,
                trinoCatalogName,
                null,
                CatalogRegistrationState.Status.SKIPPED,
                "Matched gravitino.trino.skip-catalog-patterns"),
            null);
        continue;
      }
      catalogNames.add(catalogName);
    }

    LOG.debug("Load metalake {}'s catalogs. catalogs: {}.", metalakeName, catalogNames);

    // Delete those catalogs that have been deleted in Gravitino server
    Set<String> catalogNameStrings = new HashSet<>();
    for (String catalogName : catalogNames) {
      catalogNameStrings.add(getTrinoCatalogName(metalakeName, catalogName));
    }

    for (Map.Entry<String, CatalogConnectorContext> entry : catalogConnectors.entrySet()) {
      if (!catalogNameStrings.contains(entry.getKey())
          &&
          // Skip the catalog doesn't belong to this metalake.
          entry.getValue().getMetalake().name().equals(metalakeName)) {
        try {
          unloadCatalog(entry.getValue().getCatalog());
        } catch (Exception e) {
          LOG.error("Failed to remove catalog {}.", entry.getKey(), e);
        }
      }
    }

    // Drop the states of catalogs that no longer exist in the Gravitino server, including the
    // states of catalogs that never had a connector.
    catalogStates
        .values()
        .removeIf(
            state ->
                state.getMetalake().equals(metalakeName)
                    && !presentTrinoNames.contains(state.getTrinoCatalogName()));

    // Load new catalogs belows to the metalake.
    for (String catalogName : catalogNames) {
      String trinoCatalogName = getTrinoCatalogName(metalakeName, catalogName);
      // Tracked outside the try so that a failure can still report the provider it knows about.
      String provider = null;
      try {
        Catalog catalog = metalake.loadCatalog(catalogName);
        GravitinoCatalog gravitinoCatalog = new GravitinoCatalog(metalakeName, catalog);
        provider = gravitinoCatalog.getProvider();
        if (catalogConnectors.containsKey(trinoCatalogName)) {
          // Reload catalogs that have been updated in Gravitino server.
          reloadCatalog(gravitinoCatalog);
          recordCatalogState(
              CatalogRegistrationState.succeeded(gravitinoCatalog, trinoCatalogName), null);
        } else if (catalog.type() != Catalog.Type.RELATIONAL) {
          recordCatalogState(
              CatalogRegistrationState.notLoaded(
                  metalakeName,
                  catalogName,
                  trinoCatalogName,
                  gravitinoCatalog.getProvider(),
                  CatalogRegistrationState.Status.UNSUPPORTED,
                  String.format(
                      "Only relational catalogs are supported, the catalog type is %s",
                      catalog.type())),
              null);
        } else if (!catalogConnectorFactory
            .getSupportedCatalogProviders()
            .contains(gravitinoCatalog.getProvider())) {
          recordCatalogState(
              CatalogRegistrationState.notLoaded(
                  metalakeName,
                  catalogName,
                  trinoCatalogName,
                  gravitinoCatalog.getProvider(),
                  CatalogRegistrationState.Status.UNSUPPORTED,
                  String.format(
                      "The catalog provider %s is not supported, the supported providers are %s",
                      gravitinoCatalog.getProvider(),
                      catalogConnectorFactory.getSupportedCatalogProviders())),
              null);
        } else {
          loadCatalog(gravitinoCatalog);
          recordCatalogState(
              CatalogRegistrationState.succeeded(gravitinoCatalog, trinoCatalogName), null);
        }
      } catch (UnsupportedOperationException e) {
        recordCatalogState(
            CatalogRegistrationState.notLoaded(
                metalakeName,
                catalogName,
                trinoCatalogName,
                provider,
                CatalogRegistrationState.Status.UNSUPPORTED,
                toErrorMessage(e)),
            null);
      } catch (Exception e) {
        recordCatalogState(
            CatalogRegistrationState.failed(
                metalakeName,
                catalogName,
                trinoCatalogName,
                provider,
                toErrorMessage(e),
                catalogStates.get(trinoCatalogName)),
            e);
      }
    }
  }

  private void recordCatalogState(CatalogRegistrationState state, Exception cause) {
    CatalogRegistrationState previous = catalogStates.put(state.getTrinoCatalogName(), state);
    boolean changed =
        previous == null
            || previous.getStatus() != state.getStatus()
            || !Objects.equals(previous.getLastError(), state.getLastError());
    if (!changed) {
      LOG.debug("Catalog {} registration state unchanged: {}", state.getTrinoCatalogName(), state);
      return;
    }

    if (state.getStatus() == CatalogRegistrationState.Status.REGISTERED) {
      LOG.info("Catalog {} is registered in Trino.", state.getTrinoCatalogName());
    } else if (state.getStatus() == CatalogRegistrationState.Status.FAILED) {
      LOG.error(
          "Failed to register catalog {} in Trino: {}",
          state.getTrinoCatalogName(),
          state.getLastError(),
          cause);
    } else {
      LOG.warn(
          "Catalog {} is not registered in Trino ({}): {}",
          state.getTrinoCatalogName(),
          state.getStatus(),
          state.getLastError());
    }
  }

  private void reloadCatalog(GravitinoCatalog catalog) {
    String catalogFullName = getTrinoCatalogName(catalog);
    GravitinoCatalog oldCatalog = catalogConnectors.get(catalogFullName).getCatalog();
    // The discovered Iceberg REST endpoint is embedded into the catalog independently of
    // Gravitino's own lastModifiedTime, so it is checked as a separate reload trigger.
    boolean icebergRestUriChanged =
        IcebergConnectorAdapter.hasDiscoveredIcebergRestUriChanged(catalog, oldCatalog, config);
    if (catalog.getLastModifiedTime() <= oldCatalog.getLastModifiedTime()
        && !icebergRestUriChanged) {
      return;
    }

    catalogRegister.unregisterCatalog(catalogFullName);
    catalogConnectors.remove(catalogFullName);

    loadCatalogImpl(catalog);
    LOG.info("Update catalog '{}' in metalake {} successfully.", catalog, catalog.getMetalake());
  }

  private void loadCatalog(GravitinoCatalog catalog) {
    loadCatalogImpl(catalog);
    LOG.info("Load catalog {} in metalake {} successfully.", catalog, catalog.getMetalake());
  }

  private void loadCatalogImpl(GravitinoCatalog catalog) {
    try {
      catalogRegister.registerCatalog(getTrinoCatalogName(catalog), catalog);
    } catch (Exception e) {
      String message =
          String.format("Failed to create internal catalog connector. The catalog is: %s", catalog);
      LOG.debug(message, e);
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_CREATE_INTERNAL_CONNECTOR_ERROR, message, e);
    }
  }

  private void unloadCatalog(GravitinoCatalog catalog) {
    String catalogFullName = getTrinoCatalogName(catalog);
    catalogRegister.unregisterCatalog(catalogFullName);
    catalogConnectors.remove(catalogFullName);
    catalogStates.remove(catalogFullName);
    LOG.info(
        "Remove catalog '{}' in metalake {} successfully.",
        catalog.getName(),
        catalog.getMetalake());
  }

  /**
   * Retrieves the catalog connector context for the specified catalog name.
   *
   * @param catalogName the name of the catalog
   * @return the catalog connector context
   */
  public CatalogConnectorContext getCatalogConnector(String catalogName) {
    return catalogConnectors.get(catalogName);
  }

  /**
   * Checks if a catalog connector exists for the specified catalog name.
   *
   * @param catalogName the name of the catalog
   * @return true if the catalog connector exists, false otherwise
   */
  public boolean catalogConnectorExist(String catalogName) {
    return catalogConnectors.containsKey(catalogName);
  }

  /**
   * Retrieves all catalogs managed by this connector manager.
   *
   * @return a list of Gravitino catalogs
   */
  public List<GravitinoCatalog> getCatalogs() {
    return catalogConnectors.values().stream().map(CatalogConnectorContext::getCatalog).toList();
  }

  /** Shuts down the catalog connector manager. */
  public void shutdown() {
    LOG.info("Gravitino CatalogConnectorManager shutdown.");
    if (catalogRegister != null) {
      catalogRegister.close();
    }

    executorService.shutdown();

    if (gravitinoClient != null) {
      gravitinoClient.close();
    }
  }

  /**
   * Retrieves the Trino catalog name for the specified metalake and catalog.
   *
   * @param metalake the name of the metalake
   * @param catalog the name of the catalog
   * @return the Trino catalog name
   */
  public String getTrinoCatalogName(String metalake, String catalog) {
    return config.singleMetalakeMode()
        ? catalog
        : trinoCatalogNameHandler.getCatalogName(metalake, catalog);
  }

  /**
   * Retrieves the Trino catalog name for the specified catalog.
   *
   * @param catalog the catalog
   * @return the Trino catalog name
   */
  public String getTrinoCatalogName(GravitinoCatalog catalog) {
    return getTrinoCatalogName(catalog.getMetalake(), catalog.getName());
  }

  /**
   * Retrieves a snapshot of the registration state of every Gravitino catalog seen by the load
   * loop.
   *
   * @return the registration states
   */
  public List<CatalogRegistrationState> getCatalogRegistrationStates() {
    return List.copyOf(catalogStates.values());
  }

  /**
   * Checks whether the Trino server has become reachable over JDBC. No catalog can be registered
   * before it does.
   *
   * @return true if the Trino server is started, false otherwise
   */
  public boolean isTrinoStarted() {
    return trinoStarted;
  }

  /**
   * Retrieves the time of the last catalog load attempt.
   *
   * @return the time in milliseconds since the epoch, 0 if the load loop never ran
   */
  public long getLastLoadAttemptTimeMs() {
    return lastLoadAttemptTimeMs;
  }

  /**
   * Retrieves the time of the last successful catalog load.
   *
   * @return the time in milliseconds since the epoch, 0 if the load loop never succeeded
   */
  public long getLastSuccessfulLoadTimeMs() {
    return lastSuccessfulLoadTimeMs;
  }

  /**
   * Retrieves the error that made the last catalog load fail.
   *
   * @return the error message, null if the last load succeeded
   */
  @Nullable
  public String getLastLoadError() {
    return lastLoadError;
  }

  /**
   * Retrieves the number of consecutive failed catalog loads.
   *
   * @return the failure count, 0 if the last load succeeded
   */
  public long getConsecutiveLoadFailures() {
    return consecutiveLoadFailures.get();
  }

  /**
   * Retrieves the last error reported by each metalake, keyed by the metalake name.
   *
   * @return the metalake errors, empty if every metalake was loaded successfully
   */
  public Map<String, String> getMetalakeErrors() {
    return Map.copyOf(metalakeErrors);
  }

  /**
   * Describes why a catalog is not registered in Trino, for use in error messages.
   *
   * @param trinoCatalogName the name the catalog would be registered under in Trino
   * @return a human readable explanation
   */
  public String describeRegistrationFailure(String trinoCatalogName) {
    CatalogRegistrationState state = catalogStates.get(trinoCatalogName);
    if (state != null && state.getLastError() != null) {
      return String.format("%s: %s", state.getStatus(), state.getLastError());
    }
    if (lastLoadError != null) {
      return lastLoadError;
    }
    return "The catalog has not been loaded yet, please retry later.";
  }

  /**
   * Retrieves the set of metalakes that have been used.
   *
   * @return the set of metalakes
   */
  public Set<String> getUsedMetalakes() {
    return metalakes.keySet();
  }

  /**
   * Creates a new connector for the specified catalog name.
   *
   * @param connectorName the name of the connector
   * @param config the Gravitino configuration
   * @param context the Trino connector context
   * @return the created catalog connector context
   */
  public CatalogConnectorContext createCatalogConnectorContext(
      String connectorName, GravitinoConfig config, ConnectorContext context) {
    try {
      String catalogConfig = config.getCatalogConfig();

      GravitinoCatalog catalog = GravitinoCatalog.fromJson(catalogConfig);
      if (this.config.singleMetalakeMode()
          && StringUtils.isNotBlank(targetMetalake)
          && !targetMetalake.equals(catalog.getMetalake())) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_UNSUPPORTED_OPERATION,
            "Multiple metalakes are not supported");
      }
      CatalogConnectorContext.Builder builder =
          catalogConnectorFactory.createCatalogConnectorContextBuilder(catalog);
      builder
          .withMetalake(metalakes.computeIfAbsent(catalog.getMetalake(), this::retrieveMetalake))
          .withContext(context)
          .withConfig(config);

      CatalogConnectorContext connectorContext = builder.build();
      String fullCatalogName = getTrinoCatalogName(catalog);
      catalogConnectors.put(fullCatalogName, connectorContext);
      LOG.info("Create connector {} successful", connectorName);
      return connectorContext;
    } catch (Exception e) {
      LOG.error("Failed to create connector: {}", connectorName, e);
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_OPERATION_FAILED,
          "Failed to create connector: " + connectorName,
          e);
    }
  }

  /**
   * Loads the metalake synchronously.
   *
   * @throws Exception if the metalake fails to load
   */
  public void loadMetalakeSync() throws Exception {
    Future<?> future = executorService.submit(this::loadMetalake);
    future.get(LOAD_METALAKE_TIMEOUT, TimeUnit.SECONDS);
  }

  /**
   * Retrieves the Gravitino metalake for the specified name.
   *
   * @param metalake the name of the metalake
   * @return the Gravitino metalake
   * @throws TrinoException if the metalake is not found
   */
  public GravitinoMetalake getMetalake(String metalake) {
    return metalakes.computeIfAbsent(metalake, this::retrieveMetalake);
  }

  /**
   * Whether skip loading catalog or not
   *
   * @param catalogName catalog name
   * @return whether skip loading catalog or not
   */
  public boolean skipCatalog(String catalogName) {
    for (Pattern pattern : config.getSkipCatalogPatterns()) {
      if (pattern.matcher(catalogName).matches()) {
        LOG.debug(
            "Skip catalog {} with config `gravitino.trino.skip-catalog-patterns`.", catalogName);
        return true;
      }
    }
    return false;
  }

  public interface TrinoCatalogNameHandler {
    String getCatalogName(String metalake, String catalog);
  }
}
