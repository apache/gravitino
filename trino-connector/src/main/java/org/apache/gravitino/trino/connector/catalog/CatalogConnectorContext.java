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

  public GravitinoMetalake getMetalake() {
    return metalake;
  }

  public GravitinoCatalog getCatalog() {
    return catalog;
  }

  public GravitinoConnector getConnector() {
    return connector;
  }

  public Connector getInternalConnector() {
    return internalConnector;
  }

  public List<PropertyMetadata<?>> getTableProperties() {
    return adapter.getTableProperties();
  }

  public List<PropertyMetadata<?>> getSchemaProperties() {
    return adapter.getSchemaProperties();
  }

  public List<PropertyMetadata<?>> getColumnProperties() {
    return adapter.getColumnProperties();
  }

  public void close() {
    this.internalConnector.shutdown();
  }

  public CatalogConnectorMetadataAdapter getMetadataAdapter() {
    return adapter.getMetadataAdapter();
  }

  public static class Builder {
    private final CatalogConnectorAdapter connectorAdapter;
    private GravitinoCatalog catalog;
    private GravitinoMetalake metalake;
    private ConnectorContext context;

    public Builder(CatalogConnectorAdapter connectorAdapter) {
      this.connectorAdapter = connectorAdapter;
    }

    private Builder(CatalogConnectorAdapter connectorAdapter, GravitinoCatalog catalog) {
      this.connectorAdapter = connectorAdapter;
      this.catalog = catalog;
    }

    public Builder clone(GravitinoCatalog catalog) {
      return new Builder(connectorAdapter, catalog);
    }

    public Builder withMetalake(GravitinoMetalake metalake) {
      this.metalake = metalake;
      return this;
    }

    public Builder withContext(ConnectorContext context) {
      this.context = context;
      return this;
    }

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
