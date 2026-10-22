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
package org.apache.gravitino.catalog;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server-side periodic task that reconciles stale registrations of non-managed entities with their
 * source.
 *
 * <p>For non-managed catalogs the source system is the source of truth and Gravitino keeps only a
 * registration row per entity. When an object is dropped directly in the source, the registration
 * stays behind and poisons catalog-walking consumers. Each round of this task enumerates the schema
 * registrations of every non-managed catalog, confirms absence with an explicit per-object
 * existence probe and produces a {@link StaleCatalogReport} per catalog.
 *
 * <p>A registration is stale only when all of the following hold: the owning catalog does not
 * manage storage for the entity scope; the explicit existence probe reports the object absent; and
 * the probe did not fail for infrastructural reasons (a probe failure marks the catalog unreachable
 * and skips all removals for that catalog in this round). Absence is never inferred from a missing
 * listing entry.
 *
 * <p>This first version is report-only: stale registrations are reported with {@code removed ==
 * false} and nothing is deleted. The auto-remove policy is applied by a follow-up change through
 * the dispatcher chain, so a reconcile-triggered removal behaves exactly like an explicit drop.
 *
 * <p>The task is disabled by default ({@code gravitino.reconcile.enabled} is {@code false}) and
 * follows the {@code RelationalGarbageCollector} pattern: it owns a daemon {@link
 * ScheduledThreadPoolExecutor} with {@code start()}/{@code close()}, wired in {@code GravitinoEnv}.
 */
public class StaleRegistrationReconciler implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(StaleRegistrationReconciler.class);

  /**
   * The delay before the first reconcile round: short enough that a server restarted more often
   * than the interval still runs its first round, long enough to clear the server startup storm.
   * Mirrors {@code RelationalGarbageCollector}'s five-minute initial delay.
   */
  private static final long INITIAL_DELAY_SECS = 300L;

  private final Config config;
  private final EntityStore entityStore;
  private final CatalogManager catalogManager;
  private final long catalogJitterSecs;
  private final ReconcilePolicy policy;

  @VisibleForTesting
  final ScheduledThreadPoolExecutor reconcilePool =
      new ScheduledThreadPoolExecutor(
          1,
          r -> {
            Thread t = new Thread(r, "Stale-Registration-Reconciler");
            t.setDaemon(true);
            return t;
          },
          new ThreadPoolExecutor.AbortPolicy());

  @VisibleForTesting final Random jitterRandom = new Random();

  /** Set by {@link #close()}; checked at the round, catalog and schema boundaries to stop early. */
  private volatile boolean closed;

  /**
   * Creates the reconciler.
   *
   * @param config the server configuration
   * @param entityStore the entity store holding the registrations to reconcile
   * @param catalogManager the catalog manager used to lease catalog wrappers for probing
   */
  public StaleRegistrationReconciler(
      Config config, EntityStore entityStore, CatalogManager catalogManager) {
    this.config = config;
    this.entityStore = entityStore;
    this.catalogManager = catalogManager;
    this.catalogJitterSecs = config.get(Configs.RECONCILE_CATALOG_JITTER_SECS);
    this.policy = ReconcilePolicy.fromString(config.get(Configs.RECONCILE_POLICY));
  }

  /**
   * Starts the periodic reconcile task. This is a no-op when {@code gravitino.reconcile.enabled} is
   * {@code false} (the default). The first round runs after a fixed five-minute delay.
   */
  public void start() {
    if (!config.get(Configs.RECONCILE_ENABLED)) {
      LOG.info("Stale registration reconcile is disabled");
      return;
    }

    long intervalSecs = config.get(Configs.RECONCILE_INTERVAL_SECS);
    LOG.info(
        "Stale registration reconcile is enabled: first run in {} seconds, interval {} seconds, "
            + "per-catalog jitter up to {} seconds, policy {}",
        INITIAL_DELAY_SECS,
        intervalSecs,
        catalogJitterSecs,
        policy.configValue());
    reconcilePool.scheduleAtFixedRate(
        this::runScheduledRound, INITIAL_DELAY_SECS, intervalSecs, TimeUnit.SECONDS);
  }

  /**
   * Runs one reconcile round and logs its summary. This is the task body scheduled by {@link
   * #start()}.
   *
   * @return the summary of the round
   */
  @VisibleForTesting
  RoundSummary runScheduledRound() {
    return logRoundSummary(reconcileRound());
  }

  /**
   * Runs one reconcile round over all metalakes and their catalogs. A failing catalog is logged and
   * skipped without aborting the round. Returns the reports gathered so far when the reconciler is
   * closed or the worker thread is interrupted.
   *
   * @return the per-catalog reports produced in this round
   */
  @VisibleForTesting
  public List<StaleCatalogReport> reconcileRound() {
    List<StaleCatalogReport> reports = new ArrayList<>();
    if (closed) {
      return reports;
    }
    try {
      for (String metalakeName : listMetalakeNames()) {
        for (String catalogName : listCatalogNames(metalakeName)) {
          if (closed) {
            return reports;
          }
          try {
            applyCatalogJitter();
            reports.add(reconcileCatalog(metalakeName, catalogName));
          } catch (InterruptedException e) {
            // Interrupted, most likely by close(); restore the status and stop the round.
            Thread.currentThread().interrupt();
            return reports;
          } catch (Exception e) {
            LOG.warn("Failed to reconcile catalog {}.{}", metalakeName, catalogName, e);
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to enumerate metalakes or catalogs for reconcile", e);
    }
    return reports;
  }

  /**
   * Reconciles the schema registrations of one catalog.
   *
   * @param metalakeName the metalake name
   * @param catalogName the catalog name
   * @return the report for this catalog; never {@code null}
   */
  @VisibleForTesting
  StaleCatalogReport reconcileCatalog(String metalakeName, String catalogName) throws Exception {
    NameIdentifier catalogIdent = NameIdentifier.of(metalakeName, catalogName);
    try {
      return catalogManager.doWithCatalogWrapper(
          catalogIdent, wrapper -> reconcileCatalogWithWrapper(metalakeName, catalogName, wrapper));
    } catch (NoSuchCatalogException | NoSuchEntityException e) {
      // The catalog was removed concurrently with this round; nothing to reconcile.
      LOG.debug("Catalog {} no longer exists; skipping", catalogIdent);
      return StaleCatalogReport.empty(metalakeName, catalogName);
    }
  }

  private StaleCatalogReport reconcileCatalogWithWrapper(
      String metalakeName, String catalogName, CatalogManager.CatalogWrapper wrapper)
      throws Exception {
    // Stale condition 1: the catalog must not manage storage for the schema scope. Managed
    // catalogs are Gravitino's own storage, no source drift is possible.
    if (wrapper.capabilities().managedStorage(Capability.Scope.SCHEMA).supported()) {
      LOG.debug("Catalog {}.{} manages schema storage; skipping", metalakeName, catalogName);
      return StaleCatalogReport.empty(metalakeName, catalogName);
    }

    Namespace schemaNamespace = Namespace.of(metalakeName, catalogName);
    List<SchemaEntity> registrations =
        entityStore.list(schemaNamespace, SchemaEntity.class, Entity.EntityType.SCHEMA);
    if (registrations.isEmpty()) {
      return StaleCatalogReport.empty(metalakeName, catalogName);
    }
    long detectedAt = System.currentTimeMillis();
    LinkedHashSet<String> staleNames = new LinkedHashSet<>();

    for (SchemaEntity registration : registrations) {
      if (closed) {
        // The reconciler is closing; discard the partial scan.
        return StaleCatalogReport.empty(metalakeName, catalogName);
      }
      ProbeResult result =
          probeSchema(wrapper, NameIdentifier.of(schemaNamespace, registration.name()));
      switch (result) {
        case ABSENT:
          staleNames.add(registration.name());
          break;
        case UNSUPPORTED:
          // The catalog exposes no schema operations at all, so absence cannot be confirmed;
          // this is not an infrastructure failure and not staleness either.
          LOG.debug(
              "Catalog {}.{} does not support schema operations; skipping",
              metalakeName,
              catalogName);
          return StaleCatalogReport.empty(metalakeName, catalogName);
        case UNREACHABLE:
          // Stale condition 3 violated: the probe failed for infrastructural reasons, so
          // absence is unconfirmed and the whole catalog is skipped in this round.
          return StaleCatalogReport.unreachable(metalakeName, catalogName);
        case PRESENT:
        default:
          break;
      }
    }

    String separator = HierarchicalSchemaUtil.schemaSeparator();
    List<StaleSchemaEntry> entries =
        selectOutermostStaleNames(staleNames, separator).stream()
            .map(name -> new StaleSchemaEntry(name, detectedAt, false))
            .collect(Collectors.toList());
    return StaleCatalogReport.of(metalakeName, catalogName, entries);
  }

  /**
   * Probes one schema registration against its source.
   *
   * @param wrapper the leased catalog wrapper to probe through
   * @param schemaIdent the schema identifier, with its logical (external separator) name
   * @return the probe outcome
   */
  @VisibleForTesting
  static ProbeResult probeSchema(
      CatalogManager.CatalogWrapper wrapper, NameIdentifier schemaIdent) {
    try {
      return wrapper.doWithSchemaOps(ops -> ops.schemaExists(schemaIdent))
          ? ProbeResult.PRESENT
          : ProbeResult.ABSENT;
    } catch (NoSuchSchemaException e) {
      // Defensive: the load-and-catch probe maps a missing object to false, but a custom
      // implementation may throw instead; absence of the target schema is still unambiguous.
      // Other NotFoundException subtypes (missing catalog or entity) do not prove this schema is
      // gone; they fall through to the infrastructure failure case below.
      return ProbeResult.ABSENT;
    } catch (UnsupportedOperationException e) {
      return ProbeResult.UNSUPPORTED;
    } catch (InterruptedException e) {
      // Interrupted by close(); restore the status so the round loop stops.
      Thread.currentThread().interrupt();
      return ProbeResult.UNREACHABLE;
    } catch (Exception e) {
      LOG.warn("Existence probe for schema {} failed with an infrastructure error", schemaIdent, e);
      return ProbeResult.UNREACHABLE;
    }
  }

  /**
   * Reduces the detected stale names to the outermost ones. For hierarchical namespaces, when a
   * parent schema and its children are all stale, only the parent is kept: a cascading removal of
   * the parent carries the descendants along, so they are not removed (or reported) separately.
   *
   * @param staleNames the detected stale schema names, in their logical form
   * @param separator the external schema separator
   * @return the outermost stale names, in encounter order
   */
  @VisibleForTesting
  static List<String> selectOutermostStaleNames(Collection<String> staleNames, String separator) {
    List<String> outermost = new ArrayList<>();
    for (String name : staleNames) {
      List<String> scopes = HierarchicalSchemaUtil.allScopes(name, separator);
      boolean coveredByStaleAncestor = false;
      for (int i = 1; i < scopes.size(); i++) {
        if (staleNames.contains(scopes.get(i))) {
          coveredByStaleAncestor = true;
          break;
        }
      }
      if (!coveredByStaleAncestor) {
        outermost.add(name);
      }
    }
    return outermost;
  }

  /**
   * Returns the next per-catalog jitter delay in milliseconds, uniformly sampled from [0,
   * catalogJitterSecs * 1000). Spreads probe load across the round instead of probing every catalog
   * at once at the start.
   *
   * @return the delay in milliseconds, 0 when jitter is disabled
   */
  @VisibleForTesting
  long nextJitterMillis() {
    if (catalogJitterSecs <= 0) {
      return 0L;
    }
    return (long) (jitterRandom.nextDouble() * catalogJitterSecs * 1000L);
  }

  private void applyCatalogJitter() throws InterruptedException {
    long jitterMillis = nextJitterMillis();
    if (jitterMillis > 0) {
      Thread.sleep(jitterMillis);
    }
  }

  private List<String> listMetalakeNames() throws IOException {
    return entityStore
        .list(Namespace.empty(), BaseMetalake.class, Entity.EntityType.METALAKE)
        .stream()
        .map(BaseMetalake::name)
        .collect(Collectors.toList());
  }

  private List<String> listCatalogNames(String metalakeName) throws IOException {
    return entityStore
        .list(Namespace.of(metalakeName), CatalogEntity.class, Entity.EntityType.CATALOG)
        .stream()
        .map(CatalogEntity::name)
        .collect(Collectors.toList());
  }

  /**
   * Logs one WARN line per unreachable catalog and per stale schema registration, and one INFO
   * summary line per round.
   *
   * @param reports the per-catalog reports of the round
   * @return the round summary
   */
  private RoundSummary logRoundSummary(List<StaleCatalogReport> reports) {
    int unreachableCatalogs = 0;
    List<String> staleSchemaIdents = new ArrayList<>();
    for (StaleCatalogReport report : reports) {
      if (report.catalogUnreachable()) {
        unreachableCatalogs++;
        LOG.warn(
            "Catalog {}.{} unreachable in this round; skipped",
            report.metalake(),
            report.catalog());
      }
      for (StaleSchemaEntry entry : report.staleSchemas()) {
        staleSchemaIdents.add(report.metalake() + "." + report.catalog() + "." + entry.name());
        LOG.warn(
            "Stale schema registration {}.{}.{}: the source object no longer exists (reported, "
                + "nothing removed)",
            report.metalake(),
            report.catalog(),
            entry.name());
      }
    }
    if (reports.isEmpty()) {
      LOG.debug("Reconcile round completed with no catalogs scanned");
    } else {
      LOG.info(
          "Reconcile round completed: {} catalog(s) scanned, {} unreachable, {} stale schema "
              + "registration(s) reported",
          reports.size(),
          unreachableCatalogs,
          staleSchemaIdents.size());
    }
    return new RoundSummary(reports.size(), unreachableCatalogs, staleSchemaIdents);
  }

  @Override
  public void close() throws IOException {
    closed = true;
    reconcilePool.shutdown();
    try {
      if (!reconcilePool.awaitTermination(5, TimeUnit.SECONDS)) {
        reconcilePool.shutdownNow();
      }
    } catch (InterruptedException ex) {
      reconcilePool.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  /** The outcome of an existence probe against the source. */
  @VisibleForTesting
  enum ProbeResult {
    /** The source reports the object exists. */
    PRESENT,
    /** The source explicitly reports the object absent. */
    ABSENT,
    /** The probe failed for infrastructural reasons; absence is unconfirmed. */
    UNREACHABLE,
    /** The catalog does not support schema operations at all. */
    UNSUPPORTED
  }

  /** Counters and stale identifiers of one reconcile round. */
  @VisibleForTesting
  static final class RoundSummary {

    private final int scannedCatalogs;

    private final int unreachableCatalogs;

    private final List<String> staleSchemaIdents;

    private RoundSummary(int scannedCatalogs, int unreachableCatalogs, List<String> idents) {
      this.scannedCatalogs = scannedCatalogs;
      this.unreachableCatalogs = unreachableCatalogs;
      this.staleSchemaIdents = List.copyOf(idents);
    }

    /** Returns the number of catalogs scanned in the round. */
    int scannedCatalogs() {
      return scannedCatalogs;
    }

    /** Returns the number of catalogs marked unreachable in the round. */
    int unreachableCatalogs() {
      return unreachableCatalogs;
    }

    /** Returns the number of stale schema registrations reported in the round. */
    int staleSchemas() {
      return staleSchemaIdents.size();
    }

    /** Returns the reported stale schema identifiers as {@code metalake.catalog.schema} strings. */
    List<String> staleSchemaIdents() {
      return staleSchemaIdents;
    }
  }
}
