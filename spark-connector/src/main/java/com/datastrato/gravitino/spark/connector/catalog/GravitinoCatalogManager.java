/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.spark.connector.catalog;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Catalog.Type;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.client.GravitinoClient;
import com.datastrato.gravitino.client.RelationalCatalog;
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
  private final Cache<String, RelationalCatalog> gravitinoCatalogs;
  private final String metalakeName;
  private final GravitinoClient gravitinoClient;

  private GravitinoCatalogManager(String gravitinoUri, String metalakeName) {
    this.metalakeName = metalakeName;
    this.gravitinoClient = GravitinoClient.builder(gravitinoUri).withMetalake(metalakeName).build();
    // Will not evict catalog by default
    this.gravitinoCatalogs = CacheBuilder.newBuilder().build();
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

  public RelationalCatalog getGravitinoCatalogInfo(String name) {
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
    NameIdentifier[] catalogNames = gravitinoClient.listCatalogs();
    LOG.info(
        "Load metalake {}'s catalogs. catalogs: {}.", metalakeName, Arrays.toString(catalogNames));
    return Arrays.stream(catalogNames)
        .map(identifier -> identifier.name())
        .collect(Collectors.toSet());
  }

  private RelationalCatalog loadCatalog(String catalogName) {
    Catalog catalog = gravitinoClient.loadCatalog(catalogName);
    Preconditions.checkArgument(
        Type.RELATIONAL.equals(catalog.type()), "Only support relational catalog");
    RelationalCatalog result = (RelationalCatalog) catalog;
    LOG.info("Load catalog {} from Gravitino successfully.", catalogName);
    return result;
  }
}
