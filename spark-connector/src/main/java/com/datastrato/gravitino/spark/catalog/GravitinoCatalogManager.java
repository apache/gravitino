/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.spark.catalog;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Catalog.Type;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.client.GravitinoClient;
import com.datastrato.gravitino.client.GravitinoMetaLake;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** GravitinoCatalogManager is used to retrieve catalogs from Gravitino server. */
public class GravitinoCatalogManager {
  private static final Logger LOG = LoggerFactory.getLogger(GravitinoCatalogManager.class);
  private static GravitinoCatalogManager gravitinoCatalogManager;

  private volatile boolean isClosed = false;
  private final Cache<String, Catalog> gravitinoCatalogs;
  private final String metalakeName;
  private final GravitinoMetaLake metalake;
  private final GravitinoClient gravitinoClient;

  private GravitinoCatalogManager(String gravitinoUri, String metalakeName) {
    this.metalakeName = metalakeName;
    this.gravitinoClient = GravitinoClient.builder(gravitinoUri).build();
    // Will not evict catalog by default
    this.gravitinoCatalogs = CacheBuilder.newBuilder().build();
    this.metalake = gravitinoClient.loadMetalake(NameIdentifier.ofMetalake(metalakeName));
  }

  public static GravitinoCatalogManager create(String gravitinoUrl, String metalakeName) {
    Preconditions.checkState(
        gravitinoCatalogManager == null, "Should not create duplicate GravitinoCatalogManager");
    gravitinoCatalogManager = new GravitinoCatalogManager(gravitinoUrl, metalakeName);
    return gravitinoCatalogManager;
  }

  public static GravitinoCatalogManager get() {
    Preconditions.checkState(
        gravitinoCatalogManager != null, "GravitinoCatalogManager has not created yet");
    Preconditions.checkState(
        !gravitinoCatalogManager.isClosed, "GravitinoCatalogManager is already closed");
    return gravitinoCatalogManager;
  }

  public void close() {
    Preconditions.checkState(!isClosed, "Gravitino Catalog is already closed");
    isClosed = true;
    gravitinoClient.close();
  }

  /**
   * List all catalog names under this catalog manager with specified metalake namespace.
   *
   * @return A list of {@link NameIdentifier} of the catalogs under this catalog manager.
   */
  public Set<String> listCatalogs() {
    return Arrays.stream(listCatalogNames()).map(NameIdentifier::name).collect(Collectors.toSet());
  }

  /**
   * List all catalog infos under this catalog manager with specified metalake namespace.
   *
   * @return A list of {@link Catalog} of the catalogs under this catalog manager.
   */
  public Set<Catalog> listCatalogInfos() {
    return Arrays.stream(listCatalogNames())
        .map(
            nameIdentifier -> {
              try {
                return getCatalogInfo(nameIdentifier.name());
              } catch (RuntimeException e) {
                return null;
              }
            })
        .collect(Collectors.toSet());
  }

  /**
   * Get the catalog info with specified catalog name.
   *
   * @param name The name of the catalog to get.
   * @return The {@link Catalog} with specified catalog name.
   */
  public Catalog getCatalogInfo(String name) {
    try {
      return gravitinoCatalogs.get(name, () -> loadCatalog(name));
    } catch (ExecutionException e) {
      LOG.error(String.format("Load catalog %s failed", name), e);
      throw new RuntimeException(e);
    }
  }

  public String getMetalakeName() {
    return metalakeName;
  }

  private NameIdentifier[] listCatalogNames() {
    NameIdentifier[] catalogNames = metalake.listCatalogs(Namespace.ofCatalog(metalake.name()));
    LOG.info(
        "Load metalake {}'s catalogs. catalogs: {}.",
        metalake.name(),
        Arrays.toString(catalogNames));
    return catalogNames;
  }

  private Catalog loadCatalog(String catalogName) {
    Catalog catalog = metalake.loadCatalog(NameIdentifier.ofCatalog(metalakeName, catalogName));
    Preconditions.checkArgument(
        Type.RELATIONAL.equals(catalog.type()), "Only support relational catalog");
    LOG.info("Load catalog {} from Gravitino successfully.", catalogName);
    return catalog;
  }
}
