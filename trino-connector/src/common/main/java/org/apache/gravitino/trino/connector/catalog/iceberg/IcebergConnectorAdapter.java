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
package org.apache.gravitino.trino.connector.catalog.iceberg;

import static java.util.Collections.emptyList;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.trino.connector.GravitinoConfig;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorAdapter;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforming Apache Iceberg connector configuration and components into Apache Gravitino
 * connector.
 */
public class IcebergConnectorAdapter implements CatalogConnectorAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergConnectorAdapter.class);

  private static final String CONNECTOR_ICEBERG = "iceberg";
  private static final String REST_CATALOG_BACKEND = "rest";
  private static final String ICEBERG_PROVIDER = "lakehouse-iceberg";

  /**
   * Synthetic catalog property carrying the Iceberg REST server endpoint the coordinator discovered
   * for this catalog's metalake. {@link GravitinoConfig}'s own discovered-endpoint map is populated
   * only on the coordinator (the periodic discovery poll never runs on a worker), so it cannot be
   * read directly when building a catalog's internal connector config: every node needs the same
   * routing decision for the same catalog. Embedding the resolved endpoint into the catalog itself,
   * at registration time, means it travels to every node through the {@code CREATE CATALOG}
   * statement Trino replicates cluster-wide, the same way any other catalog property does.
   */
  static final String DISCOVERED_ICEBERG_REST_URI_PROPERTY = "__gravitino.iceberg.rest-uri";

  private final IcebergPropertyMeta propertyMetadata;
  private final IcebergCatalogPropertyConverter catalogConverter;
  private final GravitinoConfig config;

  /**
   * Constructs a new IcebergConnectorAdapter. Initializes the property metadata and catalog
   * converter for handling Iceberg-specific configurations.
   *
   * @param config the Gravitino connector configuration
   */
  public IcebergConnectorAdapter(GravitinoConfig config) {
    this.propertyMetadata = new IcebergPropertyMeta();
    this.catalogConverter = new IcebergCatalogPropertyConverter();
    this.config = config;
  }

  @Override
  public Map<String, String> buildInternalConnectorConfig(
      GravitinoCatalog catalog, Credential[] credentials) throws Exception {
    // The catalog backend describes how Gravitino stores the metadata; it does not decide how
    // Trino reaches the data. Whenever an Iceberg REST server endpoint is available for this
    // catalog's metalake, the catalog is loaded through it, the only path that supports temporary
    // credentials. A catalog that already has a REST backend keeps pointing at its own configured
    // endpoint. If no endpoint is available, routing is required unless the compatibility switch
    // explicitly enables the legacy catalog-backend translation.
    //
    // The manual override is plain local config, so it is valid on every node as-is. The
    // discovered endpoint is coordinator-only knowledge, so it is read from the catalog's own
    // properties, where the coordinator embeds it at registration time (see
    // embedDiscoveredIcebergRestUri), rather than from GravitinoConfig directly.
    String restUri = config.getManualIcebergRestUri(catalog.getMetalake());
    if (StringUtils.isBlank(restUri)) {
      restUri = catalog.getProperty(DISCOVERED_ICEBERG_REST_URI_PROPERTY, "");
    }
    if (StringUtils.isNotBlank(restUri)
        && !REST_CATALOG_BACKEND.equalsIgnoreCase(
            catalog.getProperty(IcebergConstants.CATALOG_BACKEND, null))) {
      // `credentials` is intentionally unused here: with vended credentials enabled, Trino obtains
      // a fresh one per table access over the REST protocol, rather than the catalog-level
      // snapshot applyCredentials installs.
      LOG.debug(
          "Routing catalog '{}' through the Iceberg REST server; its {} catalog-level credential(s)"
              + " are not applied because the REST protocol vends one per table access.",
          catalog.getName(),
          credentials.length);
      return catalogConverter.buildIcebergRestProperties(catalog, config, restUri);
    }

    String catalogBackend = catalog.getProperty(IcebergConstants.CATALOG_BACKEND, null);
    if (config.isIcebergRestRoutingEnabled()
        && StringUtils.isBlank(restUri)
        && !REST_CATALOG_BACKEND.equalsIgnoreCase(catalogBackend)) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_RUNTIME_ERROR,
          String.format(
              "Cannot register Iceberg catalog '%s' in metalake '%s': no Iceberg REST endpoint "
                  + "is available. Configure gravitino.iceberg.rest-uri, use a Gravitino server "
                  + "that supports /api/system/iceberg-rest, or set "
                  + "gravitino.iceberg.rest-routing-enabled=false to use legacy backend "
                  + "translation.",
              catalog.getName(), catalog.getMetalake()));
    }

    Map<String, String> connectorConfig =
        new HashMap<>(catalogConverter.gravitinoToEngineProperties(catalog.getProperties()));
    IcebergCatalogPropertyConverter.applyCredentials(credentials, connectorConfig);
    return connectorConfig;
  }

  /**
   * Returns a copy of {@code catalog} with the Iceberg REST server endpoint the coordinator
   * discovered for its metalake embedded as a synthetic property, if the catalog is a
   * lakehouse-iceberg catalog and a discovered endpoint exists. Called only on the coordinator,
   * before a catalog is registered with Trino, so that the routing decision reaches every node
   * through the {@code CREATE CATALOG} statement — see {@link
   * #DISCOVERED_ICEBERG_REST_URI_PROPERTY}.
   *
   * @param catalog the catalog about to be registered
   * @param config the connector configuration holding the discovered endpoints
   * @return {@code catalog} unchanged if it is not a lakehouse-iceberg catalog or no endpoint was
   *     discovered for its metalake; otherwise a copy with the endpoint embedded
   */
  public static GravitinoCatalog embedDiscoveredIcebergRestUri(
      GravitinoCatalog catalog, GravitinoConfig config) {
    if (!ICEBERG_PROVIDER.equals(catalog.getProvider())) {
      return catalog;
    }
    String discoveredUri = config.getDiscoveredIcebergRestUri(catalog.getMetalake());
    if (StringUtils.isBlank(discoveredUri)) {
      return catalog;
    }
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .putAll(catalog.getProperties())
            .put(DISCOVERED_ICEBERG_REST_URI_PROPERTY, discoveredUri)
            .buildKeepingLast();
    return new GravitinoCatalog(
        catalog.getMetalake(),
        catalog.getProvider(),
        catalog.getName(),
        properties,
        catalog.getLastModifiedTime());
  }

  /**
   * Returns whether the Iceberg REST server endpoint currently discovered for {@code catalog}'s
   * metalake differs from the one embedded into {@code oldCatalog} the last time it was registered.
   * The catalog's Gravitino {@code lastModifiedTime} does not advance when only Iceberg REST
   * availability changes, so callers that use it to decide whether to reload a catalog need this as
   * a separate signal.
   *
   * @param catalog the freshly loaded catalog
   * @param oldCatalog the previously registered version of the same catalog
   * @param config the connector configuration holding the discovered endpoints
   * @return {@code true} if {@code catalog} is a lakehouse-iceberg catalog and the discovered
   *     endpoint for its metalake differs from what {@code oldCatalog} was registered with
   */
  public static boolean hasDiscoveredIcebergRestUriChanged(
      GravitinoCatalog catalog, GravitinoCatalog oldCatalog, GravitinoConfig config) {
    if (!ICEBERG_PROVIDER.equals(catalog.getProvider())) {
      return false;
    }
    String currentUri = config.getDiscoveredIcebergRestUri(catalog.getMetalake());
    String previousUri = oldCatalog.getProperty(DISCOVERED_ICEBERG_REST_URI_PROPERTY, "");
    return !currentUri.equals(previousUri);
  }

  @Override
  public String internalConnectorName() {
    return CONNECTOR_ICEBERG;
  }

  @Override
  public CatalogConnectorMetadataAdapter getMetadataAdapter() {
    // TODO yuhui Need to improve schema table and column properties
    return new IcebergMetadataAdapter(getSchemaProperties(), getTableProperties(), emptyList());
  }

  @Override
  public List<PropertyMetadata<?>> getTableProperties() {
    return propertyMetadata.getTablePropertyMetadata();
  }

  @Override
  public List<PropertyMetadata<?>> getSchemaProperties() {
    return propertyMetadata.getSchemaPropertyMetadata();
  }
}
