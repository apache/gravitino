/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.flink.connector.catalog;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.client.GravitinoAdminClient;
import com.datastrato.gravitino.client.GravitinoMetalake;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** GravitinoCatalogManager is used to retrieve catalogs from Gravitino server. */
public class GravitinoCatalogManager {
  private static final Logger LOG = LoggerFactory.getLogger(GravitinoCatalogManager.class);
  private static GravitinoCatalogManager gravitinoCatalogManager;

  private volatile boolean isClosed = false;
  private final String metalakeName;
  private final GravitinoMetalake metalake;
  private final GravitinoAdminClient gravitinoClient;

  private GravitinoCatalogManager(String gravitinoUri, String metalakeName) {
    this.metalakeName = metalakeName;
    this.gravitinoClient = GravitinoAdminClient.builder(gravitinoUri).build();
    this.metalake = gravitinoClient.loadMetalake(metalakeName);
  }

  /**
   * Create GravitinoCatalogManager with Gravitino server uri and metalake name.
   *
   * @param gravitinoUri Gravitino server uri
   * @param metalakeName Metalake name
   * @return GravitinoCatalogManager
   */
  public static GravitinoCatalogManager create(String gravitinoUri, String metalakeName) {
    Preconditions.checkState(
        gravitinoCatalogManager == null, "Should not create duplicate GravitinoCatalogManager");
    gravitinoCatalogManager = new GravitinoCatalogManager(gravitinoUri, metalakeName);
    return gravitinoCatalogManager;
  }

  /**
   * Get GravitinoCatalogManager instance.
   *
   * @return GravitinoCatalogManager
   */
  public static GravitinoCatalogManager get() {
    Preconditions.checkState(
        gravitinoCatalogManager != null, "GravitinoCatalogManager has not created yet");
    Preconditions.checkState(
        !gravitinoCatalogManager.isClosed, "GravitinoCatalogManager is already closed");
    return gravitinoCatalogManager;
  }

  /**
   * Close GravitinoCatalogManager.
   *
   * <p>After close, GravitinoCatalogManager can not be used anymore.
   */
  public void close() {
    Preconditions.checkState(!isClosed, "Gravitino Catalog is already closed");
    isClosed = true;
    gravitinoClient.close();
  }

  /**
   * Get GravitinoCatalog by name.
   *
   * @param name Catalog name
   * @return The Gravitino Catalog
   */
  public Catalog getGravitinoCatalogInfo(String name) {
    Catalog catalog = metalake.loadCatalog(name);
    Preconditions.checkArgument(
        Catalog.Type.RELATIONAL.equals(catalog.type()), "Only support relational catalog");
    LOG.info("Load catalog {} from Gravitino successfully.", name);
    return catalog;
  }

  /**
   * Get the metalake.
   *
   * @return the metalake name.
   */
  public String getMetalakeName() {
    return metalakeName;
  }

  /**
   * Create catalog in Gravitino.
   *
   * @param catalogName Catalog name
   * @param type Catalog type
   * @param comment Catalog comment
   * @param provider Catalog provider
   * @param properties Catalog properties
   * @return Catalog
   */
  public Catalog createCatalog(
      String catalogName,
      Catalog.Type type,
      String comment,
      String provider,
      Map<String, String> properties) {
    return metalake.createCatalog(catalogName, type, provider, comment, properties);
  }

  /**
   * Drop catalog in Gravitino.
   *
   * @param catalogName Catalog name
   * @return boolean
   */
  public boolean dropCatalog(String catalogName) {
    return metalake.dropCatalog(catalogName);
  }

  /**
   * List catalogs in Gravitino.
   *
   * @return Set of catalog names
   */
  public Set<String> listCatalogs() {
    String[] catalogNames = metalake.listCatalogs();
    LOG.info(
        "Load metalake {}'s catalogs. catalogs: {}.",
        metalake.name(),
        Arrays.toString(catalogNames));
    return Sets.newHashSet(catalogNames);
  }

  /**
   * Check if catalog exists in Gravitino.
   *
   * @param catalogName Catalog name
   * @return boolean
   */
  public boolean contains(String catalogName) {
    return metalake.catalogExists(catalogName);
  }
}
