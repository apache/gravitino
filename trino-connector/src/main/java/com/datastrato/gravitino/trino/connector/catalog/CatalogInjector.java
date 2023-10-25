/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.catalog;

import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_CREATE_INNER_CONNECTOR_FAILED;
import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_UNSUPPORTED_TRINO_VERSION;

import com.datastrato.gravitino.trino.connector.GravitinoErrorCode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.MetadataProvider;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class dynamically injects the Catalog managed by Gravitino into Trino using reflection
 * techniques. It allows it to be used in Trino like a regular Trino catalog. In Gravitino, the
 * catalog name consists of the "metalake" and catalog name, for example, "user_0.hive_us." We can
 * use it directly in Trino.
 */
public class CatalogInjector {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogInjector.class);

  private static final int MIN_TRINO_SPI_VERSION = 360;

  // It is used to inject catalogs to trino
  private InjectCatalogHandle injectHandle;

  // It is used to create internal catalogs.
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
      throw new TrinoException(GravitinoErrorCode.GRAVITINO_UNSUPPORTED_TRINO_VERSION, errmsg);
    }
  }

  private static Field getField(Object targetObject, String fieldName) throws NoSuchFieldException {
    Field field = targetObject.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    return field;
  }

  private static Object getFiledObject(Object targetObject, String fieldName)
      throws NoSuchFieldException, IllegalAccessException {
    return getField(targetObject, fieldName).get(targetObject);
  }

  private static boolean isClassObject(Object targetObject, String className) {
    return targetObject.getClass().getName().endsWith(className);
  }

  private static Class getClass(ClassLoader classLoader, String className)
      throws ClassNotFoundException {
    return classLoader.loadClass(className);
  }

  /**
   * @param context
   *     <pre>
   *  This function does the following tasks by ConnectorContext:
   *  1. Retrieve the DiscoveryNodeManager object.
   *  2. To enable Trino to handle tables on every node,
   *  set 'allCatalogsOnAllNodes' to 'true' and 'activeNodesByCatalogHandle' to empty.
   *  3. Retrieve the catalogManager object.
   *  4. Get createCatalog function in catalogFactory
   *  5. Create a CreateCatalogHandle for the Gravitino connector's internal connector.
   *  6. Create InjectCatalogHandle for injection catalogs to trino.
   *
   *  A runtime ConnectorContext hierarchy:
   *  context (ConnectorContext)
   *  --nodeManager (ConnectorAwareNodeManager)
   *  ----nodeManager (DiscoveryNodeManager)
   *  ------nodeManager (DiscoveryNodeManager)
   *  ------allCatalogsOnAllNodes (boolean)
   *  ------activeNodesByCatalogHandle (SetMultimap)
   *  --metadataProvider(InternalMetadataProvider)
   *  ----metadata (TracingMetadata)
   *  ------delegate (MetadataManager)
   *  --------transactionManager (InMemoryTransactionManager)
   *  ----------catalogManager (StaticCatalogManager)
   *  ------------catalogFactory (LazyCatalogFactory)
   *  --------------createCatalog() (Function)
   *  ------------catalogs (ConcurrentHashMap)
   * </pre>
   */
  public void init(ConnectorContext context) {
    // Injector trino catalog need NodeManager support allCatalogsOnAllNodes;
    checkTrinoSpiVersion(context);

    try {
      // 1. Retrieve the DiscoveryNodeManager object.
      Object nodeManager = context.getNodeManager();
      nodeManager = getFiledObject(nodeManager, "nodeManager");

      if (isClassObject(nodeManager, "DiscoveryNodeManager")) {
        // 2. To enable Trino to handle tables on every node
        Field allCatalogsOnAllNodes = getField(nodeManager, "allCatalogsOnAllNodes");
        allCatalogsOnAllNodes.setBoolean(nodeManager, true);

        Field activeNodesByCatalogHandle = getField(nodeManager, "activeNodesByCatalogHandle");
        activeNodesByCatalogHandle.set(nodeManager, Optional.empty());
      }

      // 3. Retrieve the catalogManager object.
      MetadataProvider metadataProvider = context.getMetadataProvider();

      Object metadata = getFiledObject(metadataProvider, "metadata");
      Object metadataManager = metadata;
      if (isClassObject(metadata, "TracingMetadata")) {
        metadataManager = getFiledObject(metadata, "delegate");
      }
      Preconditions.checkNotNull(metadataManager, "metadataManager should not be null");

      Object transactionManager = getFiledObject(metadataManager, "transactionManager");
      Object catalogManager = getFiledObject(transactionManager, "catalogManager");
      Preconditions.checkNotNull(catalogManager, "catalogManager should not be null");

      // 4. Get createCatalog function in catalogFactory
      Object catalogFactory = getFiledObject(catalogManager, "catalogFactory");
      Preconditions.checkNotNull(catalogFactory, "catalogFactory should not be null");

      Class catalogPropertiesClass =
          getClass(
              catalogManager.getClass().getClassLoader(), "io.trino.connector.CatalogProperties");
      Method createCatalogMethod =
          catalogFactory.getClass().getDeclaredMethod("createCatalog", catalogPropertiesClass);
      Preconditions.checkNotNull(createCatalogMethod, "createCatalogMethod should not be null");

      // 5. Create a CreateCatalogHandle
      createHandle =
          (catalogName, catalogProperties) -> {
            ObjectMapper objectMapper = new ObjectMapper();
            Object catalogPropertiesObject =
                objectMapper.readValue(catalogProperties, catalogPropertiesClass);

            // Call catalogFactory.createCatalog() return CatalogConnector
            Object catalogConnector =
                createCatalogMethod.invoke(catalogFactory, catalogPropertiesObject);

            // The catalogConnector hierarchy:
            // --catalogConnector (CatalogConnector)
            // ----catalogConnector (ConnectorServices)
            // ------connector (Connector)

            // Get a connector object from trino CatalogConnector.
            Object catalogConnectorObject = getFiledObject(catalogConnector, "catalogConnector");
            return getFiledObject(catalogConnectorObject, "connector");
          };

      // 6. Create InjectCatalogHandle
      createInjectHandler(
          catalogManager, catalogFactory, createCatalogMethod, catalogPropertiesClass);

      LOG.info("Bind Trino catalog manager successfully.");
    } catch (Exception e) {
      String message =
          String.format(
              "Bind Trino catalog manager failed, unsupported trino-%s version", trinoVersion);
      LOG.error(message, e);
      throw new TrinoException(GRAVITINO_UNSUPPORTED_TRINO_VERSION, message, e);
    }
  }

  private void createInjectHandler(
      Object catalogManager,
      Object catalogFactory,
      Method createCatalogMethod,
      Class catalogPropertiesClass)
      throws NoSuchFieldException, IllegalAccessException {
    // The catalogManager is an instance of CoordinatorDynamicCatalogManager
    if (isClassObject(catalogManager, "CoordinatorDynamicCatalogManager")) {
      ConcurrentHashMap activeCatalogs =
          (ConcurrentHashMap) getFiledObject(catalogManager, "activeCatalogs");
      Preconditions.checkNotNull(activeCatalogs, "activeCatalogs should not be null");

      ConcurrentHashMap allCatalogs =
          (ConcurrentHashMap) getFiledObject(catalogManager, "allCatalogs");
      Preconditions.checkNotNull(allCatalogs, "allCatalogs should not be null");

      injectHandle =
          (catalogName, catalogProperties) -> {
            // Call CatalogFactory:createCatalog and add the catalog to
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
      // The catalogManager is an instance of StaticCatalogManager
      ConcurrentHashMap catalogs = (ConcurrentHashMap) getFiledObject(catalogManager, "catalogs");
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
  }

  void injectCatalogConnector(String catalogName) {
    try {
      String catalogProperties = createCatalogProperties(catalogName);
      injectHandle.invoke(catalogName, catalogProperties);

      LOG.info("Inject trino catalog {} successfully.", catalogName);
    } catch (Exception e) {
      LOG.error("Inject trino catalog {} failed.", catalogName, e);
      throw new TrinoException(GRAVITINO_CREATE_INNER_CONNECTOR_FAILED, e);
    }
  }

  String createCatalogProperties(String catalogName) {
    String catalogPropertiesTemplate =
        "{\"catalogHandle\": \"%s:normal:default\",\"connectorName\":\"gravitino\", \"properties\": "
            + "{\"gravitino.internal\": \"true\"}"
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
      throw new TrinoException(GRAVITINO_CREATE_INNER_CONNECTOR_FAILED, e);
    }
  }

  interface InjectCatalogHandle {
    void invoke(String name, String properties) throws Exception;
  }

  interface CreateCatalogHandle {
    Object invoke(String name, String properties) throws Exception;
  }
}
