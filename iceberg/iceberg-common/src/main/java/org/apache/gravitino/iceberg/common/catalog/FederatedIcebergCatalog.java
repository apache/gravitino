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
package org.apache.gravitino.iceberg.common.catalog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A read-federating Iceberg {@link Catalog} that unifies two backends — an Iceberg REST catalog
 * (e.g. Apache Polaris) and an HMS-backed Iceberg catalog (Hive) — behind a single catalog.
 *
 * <p>Resolution order is REST-first (Polaris), HMS-fallback. Reads (loadTable/listTables/
 * listNamespaces) federate; writes/DDL throw — create tables directly in the target backend.
 *
 * <p>Backend configs are passed namespaced under the catalog properties:
 *
 * <ul>
 *   <li>{@code federated.rest.*} — REST backend (uri, credential, scope, oauth2-server-uri,
 *       warehouse, io-impl, s3.*)
 *   <li>{@code federated.hive.*} — Hive backend (uri, warehouse, io-impl, s3.*)
 * </ul>
 *
 * <p>Activated by setting {@code catalog-backend=federated} in gravitino.conf or catalog
 * properties.
 */
public class FederatedIcebergCatalog implements Catalog, SupportsNamespaces, Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(FederatedIcebergCatalog.class);
  private static final String REST_PREFIX = "federated.rest.";
  private static final String HIVE_PREFIX = "federated.hive.";

  private String name;
  private Configuration conf;
  private RESTCatalog restBackend;
  private HiveCatalog hiveBackend;

  @Override
  public void initialize(String name, Map<String, String> properties) {
    this.name = name;

    Map<String, String> restProps = subProps(properties, REST_PREFIX);
    Map<String, String> hiveProps = subProps(properties, HIVE_PREFIX);

    RESTCatalog rest = new RESTCatalog();
    if (conf != null) {
      rest.setConf(conf);
    }
    rest.initialize("federated-rest", restProps);

    HiveCatalog hive = new HiveCatalog();
    if (conf != null) {
      hive.setConf(conf);
    }
    hive.initialize("federated-hive", hiveProps);

    this.restBackend = rest;
    this.hiveBackend = hive;
    LOG.info(
        "FederatedIcebergCatalog '{}' initialized: rest={} hive={}",
        name,
        restProps.get("uri"),
        hiveProps.get("uri"));
  }

  @Override
  public String name() {
    return name;
  }

  // ── Reads: federate (REST first, Hive fallback) ─────────────────────────────

  @Override
  public Table loadTable(TableIdentifier identifier) {
    try {
      return restBackend.loadTable(identifier);
    } catch (NoSuchTableException | NoSuchNamespaceException e) {
      return hiveBackend.loadTable(identifier);
    }
  }

  @Override
  public boolean tableExists(TableIdentifier identifier) {
    return restBackend.tableExists(identifier) || hiveBackend.tableExists(identifier);
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    Set<TableIdentifier> merged = new LinkedHashSet<>();
    merged.addAll(safeListTables(restBackend, namespace));
    merged.addAll(safeListTables(hiveBackend, namespace));
    return new ArrayList<>(merged);
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) {
    Set<Namespace> merged = new LinkedHashSet<>();
    merged.addAll(safeListNamespaces(restBackend, namespace));
    merged.addAll(safeListNamespaces(hiveBackend, namespace));
    return new ArrayList<>(merged);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
    try {
      return restBackend.loadNamespaceMetadata(namespace);
    } catch (NoSuchNamespaceException e) {
      return hiveBackend.loadNamespaceMetadata(namespace);
    }
  }

  @Override
  public boolean namespaceExists(Namespace namespace) {
    return restBackend.namespaceExists(namespace) || hiveBackend.namespaceExists(namespace);
  }

  // ── Writes / DDL: read-only federation ────────────────────────────────────────

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    throw readOnly();
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    throw readOnly();
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    throw readOnly();
  }

  @Override
  public boolean dropNamespace(Namespace namespace) {
    throw readOnly();
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) {
    throw readOnly();
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) {
    throw readOnly();
  }

  // ── Configurable (Hadoop) — CatalogUtil.loadCatalog injects conf ─────────────

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  // ── helpers ───────────────────────────────────────────────────────────────────

  private static Map<String, String> subProps(Map<String, String> props, String prefix) {
    Map<String, String> out = new HashMap<>();
    for (Map.Entry<String, String> e : props.entrySet()) {
      if (e.getKey().startsWith(prefix)) {
        out.put(e.getKey().substring(prefix.length()), e.getValue());
      }
    }
    return out;
  }

  private static List<TableIdentifier> safeListTables(Catalog cat, Namespace ns) {
    try {
      return cat.listTables(ns);
    } catch (NoSuchNamespaceException e) {
      return List.of();
    }
  }

  private static List<Namespace> safeListNamespaces(SupportsNamespaces cat, Namespace ns) {
    try {
      return cat.listNamespaces(ns);
    } catch (NoSuchNamespaceException e) {
      return List.of();
    }
  }

  private static UnsupportedOperationException readOnly() {
    return new UnsupportedOperationException(
        "Federated Iceberg catalog is read-only. Create/modify tables directly in the "
            + "target backend catalog.");
  }
}
