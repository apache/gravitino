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
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.trino.connector.GravitinoConnector;
import org.apache.gravitino.trino.connector.GravitinoConnectorPluginManager;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;

/**
 * The CatalogConnector serves as a communication bridge between the Apache Gravitino connector and
 * its internal connectors. It manages the lifecycle, configuration, and runtime environment of
 * internal connectors.
 */
public class CatalogConnectorContext {

  private final GravitinoCatalog catalog;
  private final GravitinoMetalake metalake;

  // Connector communicates with Trino
  private final GravitinoConnector connector;

  // Internal connector communicates with data storage
  private final Connector internalConnector;

  private final CatalogConnectorAdapter adapter;

  /**
   * Constructs a new CatalogConnectorContext.
   *
   * @param catalog the Gravitino catalog
   * @param metalake the Gravitino metalake
   * @param internalConnector the internal connector
   * @param adapter the catalog connector adapter
   */
  public CatalogConnectorContext(
      GravitinoCatalog catalog,
      GravitinoMetalake metalake,
      Connector internalConnector,
      CatalogConnectorAdapter adapter) {
    this.catalog = catalog;
    this.metalake = metalake;
    this.internalConnector = internalConnector;
    this.adapter = adapter;

    this.connector = new GravitinoConnector(catalog.geNameIdentifier(), this);
  }

  /**
   * Returns the Gravitino metalake associated with this context.
   *
   * @return the Gravitino metalake
   */
  public GravitinoMetalake getMetalake() {
    return metalake;
  }

  /**
   * Returns the Gravitino catalog associated with this context.
   *
   * @return the Gravitino catalog
   */
  public GravitinoCatalog getCatalog() {
    return catalog;
  }

  /**
   * Returns the Gravitino connector associated with this context.
   *
   * @return the Gravitino connector
   */
  public GravitinoConnector getConnector() {
    return connector;
  }

  /**
   * Returns the internal connector associated with this context.
   *
   * @return the internal connector
   */
  public Connector getInternalConnector() {
    return internalConnector;
  }

  /**
   * Returns the table properties associated with this context.
   *
   * @return the table properties
   */
  public List<PropertyMetadata<?>> getTableProperties() {
    return adapter.getTableProperties();
  }

  /**
   * Returns the schema properties associated with this context.
   *
   * @return the schema properties
   */
  public List<PropertyMetadata<?>> getSchemaProperties() {
    return adapter.getSchemaProperties();
  }

  /**
   * Returns the column properties associated with this context.
   *
   * @return the column properties
   */
  public List<PropertyMetadata<?>> getColumnProperties() {
    return adapter.getColumnProperties();
  }

  /** Closes the internal connector associated with this context. */
  public void close() {
    this.internalConnector.shutdown();
  }

  /**
   * Returns the metadata adapter associated with this context.
   *
   * @return the metadata adapter
   */
  public CatalogConnectorMetadataAdapter getMetadataAdapter() {
    return adapter.getMetadataAdapter();
  }

  /** Builder class for creating CatalogConnectorContext instances. */
  public static class Builder {
    private final CatalogConnectorAdapter connectorAdapter;
    private GravitinoCatalog catalog;
    private GravitinoMetalake metalake;
    private ConnectorContext context;

    /**
     * Constructs a new Builder with the specified connector adapter.
     *
     * @param connectorAdapter the connector adapter to use
     */
    public Builder(CatalogConnectorAdapter connectorAdapter) {
      this.connectorAdapter = connectorAdapter;
    }

    /**
     * Constructs a new Builder with the specified connector adapter and catalog.
     *
     * @param connectorAdapter the connector adapter to use
     * @param catalog the catalog to use
     */
    private Builder(CatalogConnectorAdapter connectorAdapter, GravitinoCatalog catalog) {
      this.connectorAdapter = connectorAdapter;
      this.catalog = catalog;
    }

    /**
     * Clones the builder with a new catalog.
     *
     * @param catalog the new catalog to use
     * @return a new builder with the specified catalog
     */
    public Builder clone(GravitinoCatalog catalog) {
      return new Builder(connectorAdapter, catalog);
    }

    /**
     * Sets the metalake to use for the connector.
     *
     * @param metalake the metalake to use
     * @return the builder
     */
    public Builder withMetalake(GravitinoMetalake metalake) {
      this.metalake = metalake;
      return this;
    }

    /**
     * Sets the context to use for the connector.
     *
     * @param context the context to use
     * @return the builder
     */
    public Builder withContext(ConnectorContext context) {
      this.context = context;
      return this;
    }

    /**
     * Builds a new CatalogConnectorContext instance.
     *
     * @return the new CatalogConnectorContext instance
     * @throws Exception if the metalake, catalog, or context is not set
     */
    public CatalogConnectorContext build() throws Exception {
      Preconditions.checkArgument(metalake != null, "metalake is not null");
      Preconditions.checkArgument(catalog != null, "catalog is not null");
      Preconditions.checkArgument(context != null, "context is not null");
      Map<String, String> connectorConfig = connectorAdapter.buildInternalConnectorConfig(catalog);
      String internalConnectorName = connectorAdapter.internalConnectorName();

      Connector connector =
          GravitinoConnectorPluginManager.instance(context.getClass().getClassLoader())
              .createConnector(internalConnectorName, connectorConfig, context);
      return new CatalogConnectorContext(catalog, metalake, connector, connectorAdapter);
    }
  }
}
