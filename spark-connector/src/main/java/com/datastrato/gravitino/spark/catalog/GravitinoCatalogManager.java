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
  private Cache<String, Catalog> gravitinoCatalogs;
  private String metalakeName;
  private GravitinoMetaLake metalake;
  private GravitinoClient gravitinoClient;

  private GravitinoCatalogManager(String gravitinoUri, String metalakeName) {
    this.metalakeName = metalakeName;
    this.gravitinoClient = GravitinoClient.builder(gravitinoUri).build();
    // Will not evict catalog by default
    this.gravitinoCatalogs = CacheBuilder.newBuilder().build();
    this.metalake = gravitinoClient.loadMetalake(NameIdentifier.ofMetalake(metalakeName));
  }

  public static GravitinoCatalogManager create(String gravitinoUrl, String metalakeName) {
    if (gravitinoCatalogManager != null) {
      throw new RuntimeException("should not create duplicate GravitinoCatalogManager");
    }
    gravitinoCatalogManager = new GravitinoCatalogManager(gravitinoUrl, metalakeName);
    return gravitinoCatalogManager;
  }

  public static GravitinoCatalogManager get() {
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

  public Set<String> listCatalogs() {
    NameIdentifier[] catalogNames = metalake.listCatalogs(Namespace.ofCatalog(metalake.name()));
    LOG.info(
        "Load metalake {}'s catalogs. catalogs: {}.",
        metalake.name(),
        Arrays.toString(catalogNames));
    return Arrays.stream(catalogNames)
        .map(identifier -> identifier.name())
        .collect(Collectors.toSet());
  }

  private Catalog loadCatalog(String catalogName) {
    Catalog catalog = metalake.loadCatalog(NameIdentifier.ofCatalog(metalakeName, catalogName));
    Preconditions.checkArgument(
        Type.RELATIONAL.equals(catalog.type()), "Only support relational catalog");
    LOG.info("Load catalog {} from Gravitino successfully.", catalogName);
    return catalog;
  }
}
