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
package org.apache.gravitino.trino.connector;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.apache.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT;
import static org.apache.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_RUNTIME_ERROR;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.airlift.log.Logger;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorContext;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorFactory;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorManager;
import org.apache.gravitino.trino.connector.catalog.CatalogRegister;
import org.apache.gravitino.trino.connector.catalog.DefaultCatalogConnectorFactory;
import org.apache.gravitino.trino.connector.system.GravitinoSystemConnector;
import org.apache.gravitino.trino.connector.system.storedprocedure.GravitinoStoredProcedureFactory;
import org.apache.gravitino.trino.connector.system.table.GravitinoSystemTableFactory;

/** Gravitino connector factory. */
public class GravitinoConnectorFactory implements ConnectorFactory {

  private static final Logger LOG = Logger.get(GravitinoConnectorFactory.class);
  private static final int MIN_SUPPORT_TRINO_SPI_VERSION = 440;
  private static final int MAX_SUPPORT_TRINO_SPI_VERSION = Integer.MAX_VALUE;
  private static final Pattern TRINO_SPI_VERSION_PATTERN = Pattern.compile("^(\\d+)");
  private static final Set<String> SECURITY_SENSITIVE_PROPERTY_SUFFIXES =
      Set.of("password", "secret", "token", "credential", "accesskey", "secretkey", "privatekey");
  /** The default connector name. */
  public static final String DEFAULT_CONNECTOR_NAME = "gravitino";

  private CatalogConnectorManager catalogConnectorManager;

  private boolean catalogConnectorManagerStartTriggered = false;

  private GravitinoAdminClient client;
  private int trinoVersion;

  public GravitinoConnectorFactory(GravitinoAdminClient client) {
    this.client = client;
  }

  @Override
  public String getName() {
    return DEFAULT_CONNECTOR_NAME;
  }

  /**
   * Retrieves the catalog connector manager.
   *
   * @return the catalog connector manager
   */
  @VisibleForTesting
  public CatalogConnectorManager getCatalogConnectorManager() {
    return catalogConnectorManager;
  }

  /**
   * Returns whether starting the catalog connector manager has been triggered.
   *
   * @return true if starting the catalog connector manager has been triggered
   */
  @VisibleForTesting
  public boolean isCatalogConnectorManagerStartTriggered() {
    return catalogConnectorManagerStartTriggered;
  }

  /**
   * This function call by Trino creates a connector. It creates GravitinoSystemConnector at first.
   * Another time's it get GravitinoConnector by CatalogConnectorManager
   *
   * @param catalogName the connector name of catalog
   * @param requiredConfig the config of connector
   * @return Trino connector
   */
  @Override
  public Connector create(
      String catalogName,
      Map<String, String> requiredConfig,
      ConnectorContext trinoConnectorContext) {
    Preconditions.checkArgument(requiredConfig != null, "requiredConfig is not null");
    GravitinoConfig config = new GravitinoConfig(requiredConfig);

    synchronized (this) {
      // Keep the version check out of the try below so that it keeps reporting its own error code
      // instead of being wrapped as a generic initialization failure.
      if (catalogConnectorManager == null) {
        checkTrinoSpiVersion(trinoConnectorContext, config);
      }

      try {
        if (catalogConnectorManager == null) {
          CatalogRegister catalogRegister = new CatalogRegister();

          CatalogConnectorFactory catalogConnectorFactory = createCatalogConnectorFactory(config);
          CatalogConnectorManager newCatalogConnectorManager =
              new CatalogConnectorManager(
                  catalogRegister, catalogConnectorFactory, this::getTrinoCatalogName);
          newCatalogConnectorManager.config(config, client);

          // Publish the manager only after it has been configured successfully. Otherwise a
          // failed client initialization leaves a shared manager with a null Gravitino client,
          // causing later connector creation attempts to fail with a misleading NPE.
          catalogConnectorManager = newCatalogConnectorManager;
        }

        // The `trino.jdbc.*` settings that CatalogRegister needs to connect back to the
        // coordinator are deliberately not propagated to the dynamic catalogs, so they are only
        // present in the configuration of the static connector. Trino does not guarantee that the
        // static catalog is loaded before the catalogs Gravitino created, therefore the manager is
        // started from the static connector only, re-applying its configuration in case a dynamic
        // connector was created first.
        if (!catalogConnectorManagerStartTriggered
            && !config.isDynamicConnector()
            && isCoordinator(trinoConnectorContext)) {
          // Triggered before start() on purpose: everything that makes it fail is a
          // configuration error, and retrying on the next create() would only open another
          // connection.
          catalogConnectorManagerStartTriggered = true;
          // Only the configuration is re-applied here: rebuilding the Gravitino client would leak
          // the one a dynamic connector may have already built.
          catalogConnectorManager.updateConfig(config);
          // Only the coordinator runs the load loop, so it is the only node holding the
          // registration state the system tables report. Pin their splits to it.
          GravitinoSystemConnector.Split.setCoordinatorAddress(
              getCurrentNodeAddress(trinoConnectorContext));
          catalogConnectorManager.start();
        }
      } catch (Exception e) {
        String message = "Initialization of the GravitinoConnector failed " + e.getMessage();
        LOG.error(e, message);
        throw new TrinoException(GRAVITINO_RUNTIME_ERROR, message, e);
      }
    }

    if (config.isDynamicConnector()) {
      // The dynamic connector is an instance of GravitinoConnector. It is loaded from Gravitino
      // server.
      CatalogConnectorContext catalogConnectorContext =
          catalogConnectorManager.createCatalogConnectorContext(
              catalogName, config, trinoConnectorContext);
      GravitinoConnector catalogConnector = createConnector(catalogConnectorContext);
      catalogConnectorContext.bindConnector(catalogConnector);
      return catalogConnectorContext.getConnector();
    } else {
      // The static connector is an instance of GravitinoSystemConnector. It is loaded by Trino
      // using the connector configuration.
      String metalake = config.getMetalake();
      if (Strings.isNullOrEmpty(metalake)) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_METALAKE_NOT_EXISTS, "No gravitino metalake selected");
      }
      // Built per entry catalog, like the stored procedures: both are scoped to this catalog's
      // metalake even though the underlying manager is shared.
      GravitinoStoredProcedureFactory gravitinoStoredProcedureFactory =
          new GravitinoStoredProcedureFactory(catalogConnectorManager, metalake);
      GravitinoSystemTableFactory systemTableFactory =
          new GravitinoSystemTableFactory(catalogConnectorManager, metalake);
      return createSystemConnector(gravitinoStoredProcedureFactory, systemTableFactory);
    }
  }

  // Note: this method is not annotated with @Override because it does not exist in the
  // ConnectorFactory interface of the baseline open-source Trino SPI version this connector
  // compiles against. Some newer Trino/Starburst SPI versions declare it as an abstract method,
  // where it is dispatched at runtime by signature, providing cross-version compatibility.
  public Set<String> getSecuritySensitivePropertyNames(
      String catalogName, Map<String, String> config, ConnectorContext context) {
    return config.keySet().stream()
        .filter(GravitinoConnectorFactory::isSecuritySensitivePropertyName)
        .collect(Collectors.toUnmodifiableSet());
  }

  protected GravitinoConnector createConnector(CatalogConnectorContext connectorContext) {
    throw new TrinoException(NOT_SUPPORTED, "Should be overridden in subclass");
  }

  protected GravitinoSystemConnector createSystemConnector(
      GravitinoStoredProcedureFactory storedProcedureFactory,
      GravitinoSystemTableFactory systemTableFactory) {
    return new GravitinoSystemConnector(storedProcedureFactory, systemTableFactory);
  }

  protected String getTrinoCatalogName(String metalakeName, String catalogName) {
    return "\"" + metalakeName + "." + catalogName + "\"";
  }

  private void checkTrinoSpiVersion(ConnectorContext context, GravitinoConfig config) {
    String spiVersion = context.getSpiVersion();
    trinoVersion = parseTrinoSpiVersion(spiVersion);

    // check catalog name with metalake are supported in this trino version
    if (!config.singleMetalakeMode() && !supportCatalogNameWithMetalake()) {
      LOG.warn(
          "The trino-connector-%s-%s does not fully support catalog name with metalake. "
              + "The DROP CATALOG operation may not work correctly in multi-metalake mode.",
          getMinSupportTrinoSpiVersion(), getMaxSupportTrinoSpiVersion());
    }

    // skip version validation
    boolean spiVersionCheck = config.isSkipTrinoVersionValidation();
    if (spiVersionCheck) {
      if (trinoVersion < getMinSupportTrinoSpiVersion()
          || trinoVersion > getMaxSupportTrinoSpiVersion()) {
        LOG.warn(
            "Trino version %s has not been tested with Gravitino and may have compatibility issues",
            trinoVersion);
      }
      return;
    }

    // version validation
    if (trinoVersion < getMinSupportTrinoSpiVersion()
        || trinoVersion > getMaxSupportTrinoSpiVersion()) {
      String errmsg =
          String.format(
              "Unsupported Trino version %s. Supported versions are %d to %d. "
                  + "To bypass this check, set gravitino.trino.skip-version-validation=true",
              trinoVersion, getMinSupportTrinoSpiVersion(), getMaxSupportTrinoSpiVersion());
      throw new TrinoException(GravitinoErrorCode.GRAVITINO_UNSUPPORTED_TRINO_VERSION, errmsg);
    }
  }

  @VisibleForTesting
  static boolean isSecuritySensitivePropertyName(String propertyName) {
    if (propertyName.startsWith(GravitinoConfig.GRAVITINO_DYNAMIC_CATALOG_ENV_PREFIX)) {
      return false;
    }
    String normalizedPropertyName = propertyName.toLowerCase(Locale.ROOT).replaceAll("[._-]", "");
    return SECURITY_SENSITIVE_PROPERTY_SUFFIXES.stream().anyMatch(normalizedPropertyName::endsWith);
  }

  @VisibleForTesting
  static int parseTrinoSpiVersion(String spiVersion) {
    Matcher matcher = TRINO_SPI_VERSION_PATTERN.matcher(spiVersion);
    if (!matcher.find()) {
      throw new TrinoException(
          GRAVITINO_ILLEGAL_ARGUMENT,
          String.format("Invalid Trino SPI version '%s': expected leading digits", spiVersion));
    }

    try {
      return Integer.parseInt(matcher.group(1));
    } catch (NumberFormatException e) {
      throw new TrinoException(
          GRAVITINO_ILLEGAL_ARGUMENT,
          String.format(
              "Invalid Trino SPI version '%s': numeric version is out of range", spiVersion),
          e);
    }
  }

  protected boolean supportCatalogNameWithMetalake() {
    return true;
  }

  protected int getMinSupportTrinoSpiVersion() {
    return MIN_SUPPORT_TRINO_SPI_VERSION;
  }

  protected int getMaxSupportTrinoSpiVersion() {
    return MAX_SUPPORT_TRINO_SPI_VERSION;
  }

  @SuppressWarnings("deprecation")
  protected boolean isCoordinator(ConnectorContext connectorContext) {
    return connectorContext.getNodeManager().getCurrentNode().isCoordinator();
  }

  /**
   * Retrieves the address of the Trino node this connector is running on.
   *
   * @param connectorContext the Trino connector context
   * @return the host and port of the current node
   */
  @SuppressWarnings("deprecation")
  protected HostAddress getCurrentNodeAddress(ConnectorContext connectorContext) {
    return connectorContext.getNodeManager().getCurrentNode().getHostAndPort();
  }

  private CatalogConnectorFactory createCatalogConnectorFactory(GravitinoConfig config) {
    // Create a CatalogConnectorFactory. If we specify a customized class name for the
    // CatalogConnectorFactory,
    // it creates a user-customized CatalogConnectorFactory; otherwise, it creates a
    // DefaultCatalogConnectorFactory.
    String className = config.getCatalogConnectorFactoryClassName();
    if (StringUtils.isEmpty(className)) {
      return new DefaultCatalogConnectorFactory(config);
    }

    try {
      Class<?> clazz = Class.forName(className);
      Object obj = clazz.getDeclaredConstructor(GravitinoConfig.class).newInstance(config);
      return (CatalogConnectorFactory) obj;
    } catch (Exception e) {
      throw new TrinoException(GRAVITINO_RUNTIME_ERROR, "Cannot create CatalogConnectorFactory", e);
    }
  }

  public int getTrinoVersion() {
    return trinoVersion;
  }
}
