/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.trino.connector.catalog;

import static com.datastrato.graviton.trino.connector.GravitonErrorCode.GRAVITON_CREATE_INNER_CONNECTOR_FAILED;
import static com.datastrato.graviton.trino.connector.GravitonErrorCode.GRAVITON_UNSUPPORTED_TRINO_VERSION;

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

  private InjectCatalogHandle injectHandle;
  private CreateCatalogHandle createHandle;
  private String trinoVersion;

  private void checkTrinoSpiVersion(ConnectorContext context) {
    this.trinoVersion = context.getSpiVersion();

    int version = Integer.parseInt(context.getSpiVersion());
    if (version < MIN_TRINO_SPI_VERSION) {
      String errmsg =
          String.format(
              "Unsupported trino-%s version. min support version is trino-%d",
              trinoVersion, MIN_TRINO_SPI_VERSION);
      throw new TrinoException(GravitonErrorCode.GRAVITON_UNSUPPORTED_TRINO_VERSION, errmsg);
    }
  }

  public void bindCatalogManager(ConnectorContext context) {
    // injector trino catalog need NodeManager support allCatalogsOnAllNodes;
    checkTrinoSpiVersion(context);

    // Try to get trino CatalogFactory instance, normally we can get the catalog from
    // CatalogFactory, then add catalog to it that loaded from graviton.

    try {
      // set NodeManager  allCatalogsOnAllNodes = true & activeNodesByCatalogHandle = empty
      Object nodeManager = context.getNodeManager();
      Field field = nodeManager.getClass().getDeclaredField("nodeManager");
      field.setAccessible(true);
      nodeManager = field.get(nodeManager);

      // Need to skip this step in the testing environment, that's using InMemoryNodeManager
      if (nodeManager.getClass().getName().endsWith("DiscoveryNodeManager")) {
        field = nodeManager.getClass().getDeclaredField("allCatalogsOnAllNodes");
        field.setAccessible(true);
        field.setBoolean(nodeManager, true);
        Preconditions.checkState(
            field.getBoolean(nodeManager), "allCatalogsOnAllNodes should be true");

        field = nodeManager.getClass().getDeclaredField("activeNodesByCatalogHandle");
        field.setAccessible(true);
        field.set(nodeManager, Optional.empty());
      }

      // find CatalogManager
      MetadataProvider metadataProvider = context.getMetadataProvider();

      field = metadataProvider.getClass().getDeclaredField("metadata");
      field.setAccessible(true);
      Object metadata = field.get(metadataProvider);

      Object metadataManager;
      if (metadata.getClass().getName().endsWith("TracingMetadata")) {
        field = metadata.getClass().getDeclaredField("delegate");
        field.setAccessible(true);
        metadataManager = field.get(metadata);
      } else {
        metadataManager = metadata;
      }
      Preconditions.checkNotNull(metadataManager, "metadataManager should not be null");

      field = metadataManager.getClass().getDeclaredField("transactionManager");
      field.setAccessible(true);
      Object transactionManager = field.get(metadataManager);

      field = transactionManager.getClass().getDeclaredField("catalogManager");
      field.setAccessible(true);
      Object catalogManager = field.get(transactionManager);
      Preconditions.checkNotNull(catalogManager, "catalogManager should not be null");

      // find CatalogFactory, createCatalog method, and CatalogProperties.
      field = catalogManager.getClass().getDeclaredField("catalogFactory");
      field.setAccessible(true);
      Object catalogFactory = field.get(catalogManager);
      Preconditions.checkNotNull(catalogFactory, "catalogFactory should not be null");

      Class catalogPropertiesClass =
          catalogManager
              .getClass()
              .getClassLoader()
              .loadClass("io.trino.connector.CatalogProperties");

      Method createCatalogMethod =
          catalogFactory.getClass().getDeclaredMethod("createCatalog", catalogPropertiesClass);
      Preconditions.checkNotNull(createCatalogMethod, "createCatalogMethod should not be null");

      createHandle =
          (catalogName, catalogProperties) -> {
            ObjectMapper objectMapper = new ObjectMapper();
            Object catalogPropertiesObject =
                objectMapper.readValue(catalogProperties, catalogPropertiesClass);

            Object catalogConnector =
                createCatalogMethod.invoke(catalogFactory, catalogPropertiesObject);

            // get a connector object from trino CatalogConnector.
            Field connectorField = catalogConnector.getClass().getDeclaredField("catalogConnector");
            connectorField.setAccessible(true);
            Object connectorService = connectorField.get(catalogConnector);

            connectorField = connectorService.getClass().getDeclaredField("connector");
            connectorField.setAccessible(true);
            Object connector = connectorField.get(connectorService);
            return connector;
          };

      // find CatalogManager.catalogs
      if (catalogManager.getClass().getName().endsWith("CoordinatorDynamicCatalogManager")) {
        field = catalogManager.getClass().getDeclaredField("activeCatalogs");
        field.setAccessible(true);
        ConcurrentHashMap activeCatalogs = (ConcurrentHashMap) field.get(catalogManager);
        Preconditions.checkNotNull(activeCatalogs, "activeCatalogs should not be null");

        field = catalogManager.getClass().getDeclaredField("allCatalogs");
        field.setAccessible(true);
        ConcurrentHashMap allCatalogs = (ConcurrentHashMap) field.get(catalogManager);
        Preconditions.checkNotNull(allCatalogs, "allCatalogs should not be null");

        injectHandle =
            (catalogName, catalogProperties) -> {
              // call CatalogFactory:createCatalog and add the catalog to
              // CoordinatorDynamicCatalogManager
              ObjectMapper objectMapper = new ObjectMapper();
              Object catalogPropertiesObject =
                  objectMapper.readValue(catalogProperties, catalogPropertiesClass);
              Object catalogConnector =
                  createCatalogMethod.invoke(catalogFactory, catalogPropertiesObject);

              Field catelogField = catalogConnector.getClass().getDeclaredField("catalog");
              catelogField.setAccessible(true);
              Object catalog = catelogField.get(catalogConnector);
              activeCatalogs.put(catalogName, catalog);

              Field catelogHandleField =
                  catalogConnector.getClass().getDeclaredField("catalogHandle");
              catelogHandleField.setAccessible(true);
              Object catalogHandle = catelogHandleField.get(catalogConnector);
              allCatalogs.put(catalogHandle, catalogConnector);
            };
      } else {
        field = catalogManager.getClass().getDeclaredField("catalogs");
        field.setAccessible(true);
        ConcurrentHashMap catalogs = (ConcurrentHashMap) field.get(catalogManager);
        Preconditions.checkNotNull(catalogs, "catalogs should not be null");

        injectHandle =
            (catalogName, catalogProperties) -> {
              // call CatalogFactory:createCatalog and add the catalog to StaticCatalogManager
              ObjectMapper objectMapper = new ObjectMapper();
              Object catalogPropertiesObject =
                  objectMapper.readValue(catalogProperties, catalogPropertiesClass);

              Object catalogConnector =
                  createCatalogMethod.invoke(catalogFactory, catalogPropertiesObject);
              catalogs.put(catalogName, catalogConnector);
            };
      }

      LOG.info("Bind Trino catalog manager successfully.");
    } catch (Exception e) {
      String message =
          String.format(
              "Bind Trino catalog manager failed, unsupported trino-%s version", trinoVersion);
      LOG.error(message, e);
      throw new TrinoException(GRAVITON_UNSUPPORTED_TRINO_VERSION, message, e);
    }
  }

  void injectCatalogConnector(String catalogName) {
    try {
      String catalogProperties = createCatalogProperties(catalogName);
      injectHandle.invoke(catalogName, catalogProperties);

      LOG.info("Inject trino catalog {} successfully.", catalogName);
    } catch (Exception e) {
      LOG.error("Inject trino catalog {} failed.", catalogName, e);
      throw new TrinoException(GRAVITON_CREATE_INNER_CONNECTOR_FAILED, e);
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
    String connectorProperties;
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      connectorProperties = objectMapper.writeValueAsString(properties);
      LOG.debug(
          "Create internal catalog connector {}. The config:{} .",
          connectorName,
          connectorProperties);

      Object catalogConnector = createHandle.invoke(connectorName, connectorProperties);

      LOG.info("Create internal catalog connector {} successfully.", connectorName);
      return (Connector) catalogConnector;
    } catch (Exception e) {
      LOG.error(
          "Create internal catalog connector {} failed. Connector properties: {} ",
          connectorName,
          properties.toString(),
          e);
      throw new TrinoException(GRAVITON_CREATE_INNER_CONNECTOR_FAILED, e);
    }
  }

  interface InjectCatalogHandle {
    void invoke(String name, String properties) throws Exception;
  }

  interface CreateCatalogHandle {
    Object invoke(String name, String properties) throws Exception;
  }
}
