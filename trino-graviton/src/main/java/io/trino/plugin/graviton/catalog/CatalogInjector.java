/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package io.trino.plugin.graviton.catalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.log.Logger;
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

  private static final Logger log = Logger.get(CatalogInjector.class);

  private ConcurrentHashMap catalogs;
  private Object catalogFactoryObject;

  public void bindCatalogManager(ConnectorContext context) {
    try {
      Object nodeManager = context.getNodeManager();
      Field field = nodeManager.getClass().getDeclaredField("nodeManager");
      field.setAccessible(true);
      nodeManager = field.get(nodeManager);

      field = nodeManager.getClass().getDeclaredField("allCatalogsOnAllNodes");
      field.setAccessible(true);
      field.setBoolean(nodeManager, true);

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

      field = catalogManager.getClass().getDeclaredField("catalogs");
      field.setAccessible(true);
      catalogs = (ConcurrentHashMap) field.get(catalogManager);

      field = catalogManager.getClass().getDeclaredField("catalogFactory");
      field.setAccessible(true);
      catalogFactoryObject = field.get(catalogManager);

      log.info("Bind Trino catalog manger successfully.");
    } catch (Throwable t) {
      log.error("Bind Trino catalog manger failed", t.getMessage());
      throw new RuntimeException(t);
    }
  }

  void injectCatalogConnector(String catalogName) {
    try {
      System.out.println("loading graviton catalogs");
      Class catalogConnectorClass =
          catalogFactoryObject
              .getClass()
              .getClassLoader()
              .loadClass("io.trino.connector.CatalogProperties");
      String catalogPropertiesTemplate =
          "{\"catalogHandle\": \"%s:normal:default\",\"connectorName\":\"graviton\", \"properties\": {\"graviton.internal\": \"true\"}}";
      String catalogProperties = String.format(catalogPropertiesTemplate, catalogName);

      ObjectMapper objectMapper = new ObjectMapper();
      Object person = objectMapper.readValue(catalogProperties, catalogConnectorClass);

      Method method =
          catalogFactoryObject.getClass().getDeclaredMethod("createCatalog", catalogConnectorClass);
      Object catalogConnector = method.invoke(catalogFactoryObject, person);
      catalogs.put(catalogName, catalogConnector);

      log.info("Inject trino catalog {} successfully.", catalogName);
    } catch (Throwable t) {
      log.info("Inject trino catalog {} failed.", catalogName, t.getMessage());
      throw new RuntimeException(t);
    }
  }

  Connector createConnector(String connectorName, Map<String, Object> properties) {
    String connectorProperties = "";
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      connectorProperties = objectMapper.writeValueAsString(properties);

      Class catalogConnectorClass =
          catalogFactoryObject
              .getClass()
              .getClassLoader()
              .loadClass("io.trino.connector.CatalogProperties");
      Method method =
          catalogFactoryObject.getClass().getDeclaredMethod("createCatalog", catalogConnectorClass);

      Object person = objectMapper.readValue(connectorProperties, catalogConnectorClass);
      Object catalogConnector = method.invoke(catalogFactoryObject, person);

      Field field = catalogConnector.getClass().getDeclaredField("catalogConnector");
      field.setAccessible(true);
      Object connectorService = field.get(catalogConnector);

      field = connectorService.getClass().getDeclaredField("connector");
      field.setAccessible(true);
      Object connector = field.get(connectorService);

      log.info("create internal catalog connector {} successfully.", connectorName);
      return (Connector) connector;
    } catch (Throwable t) {
      log.info(
          "create internal catalog connector {} failed. connector properties: {} ",
          connectorName,
          properties.toString(),
          t.getMessage());
      throw new RuntimeException(t);
    }
  }
}
