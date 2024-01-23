/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.spark.catalog;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.client.GravitinoClient;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** GravitinoCatalogManager is used to sync catalogs from Gravitino server. */
public class GravitinoCatalogManager {
  private static final Logger LOG = LoggerFactory.getLogger(GravitinoCatalogManager.class);
  private static GravitinoCatalogManager gravitinoCatalogManager;

  private volatile boolean isClosed = false;
  private HashMap<String, Catalog> gravitinoCatalogs = new HashMap<>();
  private String metalakeName;
  private GravitinoClient gravitinoClient;

  private GravitinoCatalogManager(String gravitinoUri, String metalakeName) {
    this.metalakeName = metalakeName;
    this.gravitinoClient = GravitinoClient.builder(gravitinoUri).build();
  }

  // It's not thread safe, and is expected to be invoked only once.
  public static GravitinoCatalogManager createGravitinoCatalogManager(
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
    Preconditions.checkArgument(isClosed == false);
    isClosed = true;
    gravitinoClient.close();
  }

  public Catalog getGravitinoCatalogInfo(String name) {
    return gravitinoCatalogs.get(name);
  }

  public Map<String, Catalog> getGravitinoCatalogs() {
    return ImmutableMap.copyOf(gravitinoCatalogs);
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
                // the catalog maybe not used by SQL, delay the error to SQL analysis phase
                LOG.warn(
                    "Failed to load metalake {}'s catalog {}.",
                    metalake.name(),
                    nameIdentifier.name(),
                    e);
              }
            });
  }
}
