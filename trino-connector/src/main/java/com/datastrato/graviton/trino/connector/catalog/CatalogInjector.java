/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.catalog;

import static com.datastrato.graviton.trino.connector.GravitonErrorCode.GRAVITON_CREATE_INNER_CONNECTOR_FAILED;
import static com.datastrato.graviton.trino.connector.GravitonErrorCode.GRAVITON_UNSUPPORTED_TRIO_VERSION;

import com.datastrato.graviton.trino.connector.GravitonErrorCode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.MetadataProvider;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class dynamically injects the Catalog managed by Graviton into Trino using reflection
 * techniques. It allows it to be used in Trino like a regular Trino catalog. In Graviton, the
 * catalog name consists of the "metalake" and catalog name, for example, "user_0.hive_us." We can
 * use it directly in Trino.
 */
public class CatalogInjector {

  private static final Logger LOG = Logger.get(CatalogInjector.class);

  private static final int MIN_TRINO_SPI_VERSION = 360;

  private ConcurrentHashMap catalogs;
  private Object catalogFactoryObject;
  private String trinoVersion = "";

  private void checkTrinoSpiVersion(ConnectorContext context) {
    this.trinoVersion = context.getSpiVersion();

    int trino_version = Integer.parseInt(context.getSpiVersion());
    if (trino_version < MIN_TRINO_SPI_VERSION) {
      String errmsg =
          String.format(
              "Unsupported trino-%d version. min support version is trino-%d",
              trinoVersion, MIN_TRINO_SPI_VERSION);
      throw new TrinoException(GravitonErrorCode.GRAVITON_UNSUPPORTED_TRIO_VERSION, errmsg);
    }
  }

  public void bindCatalogManager(ConnectorContext context) {
    // injector trino catalog need NodeManger support allCatalogsOnAllNodes;
    checkTrinoSpiVersion(context);

    // Try to get trino CatalogFactory instance. normally we can get the catalog from
    // CatalogFactory. then add catalog to it that loaded from graviton.

    try {
      // set NodeManger  allCatalogsOnAllNodes = true;
      Object nodeManager = context.getNodeManager();
      Field field = nodeManager.getClass().getDeclaredField("nodeManager");
      field.setAccessible(true);
      nodeManager = field.get(nodeManager);

      field = nodeManager.getClass().getDeclaredField("allCatalogsOnAllNodes");
      field.setAccessible(true);
      field.setBoolean(nodeManager, true);
      Preconditions.checkState(field.getBoolean(nodeManager), "allCatalogsOnAllNodes shoud true");

      // find CatalogManager
      field = nodeManager.getClass().getDeclaredField("activeNodesByCatalogHandle");
      field.setAccessible(true);
      field.set(nodeManager, Optional.empty());

      MetadataProvider metadataProvider = context.getMetadataProvider();

      field = metadataProvider.getClass().getDeclaredField("metadata");
      field.setAccessible(true);
      Object metadata = field.get(metadataProvider);

      field = metadata.getClass().getDeclaredField("delegate");
      field.setAccessible(true);
      Object metadataManager = field.get(metadata);

      field = metadataManager.getClass().getDeclaredField("transactionManager");
      field.setAccessible(true);
      Object transactionManager = field.get(metadataManager);

      field = transactionManager.getClass().getDeclaredField("catalogManager");
      field.setAccessible(true);
      Object catalogManager = field.get(transactionManager);

      // find CatalogManger.catalogs
      field = catalogManager.getClass().getDeclaredField("catalogs");
      field.setAccessible(true);
      catalogs = (ConcurrentHashMap) field.get(catalogManager);
      Preconditions.checkNotNull(catalogs, "catalogs should not be null");

      // find catalog factory
      field = catalogManager.getClass().getDeclaredField("catalogFactory");
      field.setAccessible(true);
      catalogFactoryObject = field.get(catalogManager);
      Preconditions.checkNotNull(catalogFactoryObject, "catalogFactoryObject should not be null");

      LOG.info("Bind Trino catalog manger successfully.");
    } catch (Throwable t) {
      String meesgae =
          String.format(
              "Bind Trino catalog manger failed, Unsuported trino-%d version", trinoVersion);
      LOG.error(meesgae, t.getMessage());
      throw new TrinoException(GRAVITON_UNSUPPORTED_TRIO_VERSION, meesgae);
    }
  }

  void injectCatalogConnector(String catalogName) {
    try {
      Class catalogConnectorClass =
          catalogFactoryObject
              .getClass()
              .getClassLoader()
              .loadClass("io.trino.connector.CatalogProperties");

      String catalogProperties = createCatalogProperties(catalogName);

      ObjectMapper objectMapper = new ObjectMapper();
      Object catalogPropertiesObject =
          objectMapper.readValue(catalogProperties, catalogConnectorClass);

      // call CatalogFactory:createCatalog
      Method method =
          catalogFactoryObject.getClass().getDeclaredMethod("createCatalog", catalogConnectorClass);
      Object catalogConnector = method.invoke(catalogFactoryObject, catalogPropertiesObject);

      // put catalog to CatalogManger.catalogs
      catalogs.put(catalogName, catalogConnector);

      LOG.info("Inject trino catalog {} successfully.", catalogName);
    } catch (Throwable t) {
      LOG.error("Inject trino catalog {} failed.", catalogName, t.getMessage());
      throw new TrinoException(GRAVITON_CREATE_INNER_CONNECTOR_FAILED, t.getMessage());
    }
  }

  String createCatalogProperties(String catalogName) {
    String catalogPropertiesTemplate =
        "{\"catalogHandle\": \"%s:normal:default\",\"connectorName\":\"graviton\", \"properties\": "
            + "{\"graviton.internal\": \"true\"}"
            + "}";
    return String.format(catalogPropertiesTemplate, catalogName);
  }

  Connector createConnector(String connectorName, Map<String, Object> properties) {
    String connectorProperties = "";
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      connectorProperties = objectMapper.writeValueAsString(properties);

      // call CatalogFactory:createCatalog
      Class catalogConnectorClass =
          catalogFactoryObject
              .getClass()
              .getClassLoader()
              .loadClass("io.trino.connector.CatalogProperties");
      Method method =
          catalogFactoryObject.getClass().getDeclaredMethod("createCatalog", catalogConnectorClass);

      Object catalogPropertyObject =
          objectMapper.readValue(connectorProperties, catalogConnectorClass);
      Object catalogConnector = method.invoke(catalogFactoryObject, catalogPropertyObject);

      // get connector object from trino CatalogConnector.
      Field field = catalogConnector.getClass().getDeclaredField("catalogConnector");
      field.setAccessible(true);
      Object connectorService = field.get(catalogConnector);

      field = connectorService.getClass().getDeclaredField("connector");
      field.setAccessible(true);
      Object connector = field.get(connectorService);

      LOG.info("Create internal catalog connector {} successfully.", connectorName);
      return (Connector) connector;
    } catch (Throwable t) {
      LOG.info(
          "Create internal catalog connector {} failed. connector properties: {} ",
          connectorName,
          properties.toString(),
          t.getMessage());
      throw new TrinoException(GRAVITON_CREATE_INNER_CONNECTOR_FAILED, t.getMessage());
    }
  }
}
