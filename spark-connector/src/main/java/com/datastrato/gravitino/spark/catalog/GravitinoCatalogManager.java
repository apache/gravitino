/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.spark.catalog;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.client.GravitinoClient;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** GravitinoCatalogManager is used to load catalogs from Gravitino and caches */
public class GravitinoCatalogManager {
  private static final Logger LOG = LoggerFactory.getLogger(GravitinoCatalogManager.class);
  private final HashMap<String, Catalog> gravitinoCatalogs = new HashMap<>();
  private String metalakeName;
  private GravitinoClient gravitinoClient;
  private volatile boolean isClosed = false;
  private static GravitinoCatalogManager gravitinoCatalogManager;

  private GravitinoCatalogManager(String gravitinoUrl, String metalakeName) {
    this.metalakeName = metalakeName;
    this.gravitinoClient = GravitinoClient.builder(gravitinoUrl).build();
  }

  public static GravitinoCatalogManager createCatalogManager(
      String gravitinoUrl, String metalakeName) {
    if (gravitinoCatalogManager != null) {
      throw new RuntimeException("should not create duplicate GravitinoCatalogManager");
    }
    gravitinoCatalogManager = new GravitinoCatalogManager(gravitinoUrl, metalakeName);
    return gravitinoCatalogManager;
  }

  public static GravitinoCatalogManager getGravitinoCatalogManager() {
    if (gravitinoCatalogManager == null) {
      throw new RuntimeException("GravitinoCatalogManager has not created yet");
    }
    if (gravitinoCatalogManager.isClosed) {
      throw new RuntimeException("GravitinoCatalogManager " + "is already closed");
    }
    return gravitinoCatalogManager;
  }

  public void close() {
    isClosed = true;
    gravitinoClient.close();
  }

  public Catalog getGravitinoCatalogInfo(String name) {
    return gravitinoCatalogs.get(name);
  }

  public Map<String, Catalog> getGravitinoCatalogs() {
    return gravitinoCatalogs;
  }

  public String getMetalakeName() {
    return metalakeName;
  }

  public void loadCatalogsFromGravitino() {
    LOG.info("Load metalake: " + metalakeName);
    GravitinoMetaLake metalake =
        gravitinoClient.loadMetalake(NameIdentifier.ofMetalake(metalakeName));
    loadCatalogs(metalake);
  }

  private void loadCatalogs(GravitinoMetaLake metalake) {
    NameIdentifier[] catalogNames = metalake.listCatalogs(Namespace.ofCatalog(metalake.name()));
    LOG.info(
        "Load metalake {}'s catalogs. catalogs: {}.",
        metalake.name(),
        Arrays.toString(catalogNames));

    // should lazy load?
    Arrays.stream(catalogNames)
        .forEach(
            (NameIdentifier nameIdentifier) -> {
              try {
                String catalogName = nameIdentifier.name();
                Catalog catalog = metalake.loadCatalog(nameIdentifier);
                gravitinoCatalogs.put(catalogName, catalog);
                LOG.info(
                    "Load catalog {} in metalake {} successfully.", catalogName, metalake.name());
              } catch (Exception e) {
                LOG.error(
                    "Failed to load metalake {}'s catalog {}.",
                    metalake.name(),
                    nameIdentifier.name(),
                    e);
                // todo throw a error?
              }
            });
  }
}
