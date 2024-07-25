package org.apache.gravitino.iceberg.common.ops;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.EntityStoreFactory;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.meta.CatalogEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergTableOpsManager implements AutoCloseable {
  public static final Logger LOG = LoggerFactory.getLogger(IcebergTableOpsManager.class);

  private final Map<String, IcebergTableOps> icebergTableOpsMap;

  private final IcebergConfig icebergConfig;

  private EntityStore entityStore;

  public IcebergTableOpsManager(IcebergConfig config) {
    this.icebergTableOpsMap = new ConcurrentHashMap<>();
    this.icebergConfig = config;
    if (icebergConfig.get(IcebergConfig.REST_PROXY)) {
      this.entityStore = EntityStoreFactory.createEntityStore(config);
      entityStore.initialize(config);
    }
  }

  public IcebergTableOpsManager(IcebergConfig config, EntityStore entityStore) {
    this.icebergTableOpsMap = new ConcurrentHashMap<>();
    this.icebergConfig = config;
    this.entityStore = entityStore;
  }

  public IcebergTableOps getOps(String prefix) {
    if (!icebergConfig.get(IcebergConfig.REST_PROXY)) {
      LOG.debug("server's rest-proxy is false, return default iceberg catalog");
      return icebergTableOpsMap.computeIfAbsent("default", k -> new IcebergTableOps(icebergConfig));
    }

    if (prefix == null || prefix.length() == 0) {
      LOG.debug("prefix is empty, return default iceberg catalog");
      return icebergTableOpsMap.computeIfAbsent("default", k -> new IcebergTableOps(icebergConfig));
    }

    String[] segments = prefix.split("/");
    String metalake = segments[0];
    String catalog = segments[1];
    return icebergTableOpsMap.computeIfAbsent(
        String.format("%s_%s", metalake, catalog),
        k -> {
          CatalogEntity entity;
          try {
            entity =
                entityStore.get(
                    NameIdentifier.of(metalake, catalog),
                    Entity.EntityType.CATALOG,
                    CatalogEntity.class);
          } catch (NoSuchEntityException e) {
            throw new RuntimeException(String.format("%s.%s does not exist", metalake, catalog));
          } catch (IOException ioe) {
            LOG.error("Failed to get {}.{}", metalake, catalog, ioe);
            throw new RuntimeException(ioe);
          }

          if (!"lakehouse-iceberg".equals(entity.getProvider())) {
            String errorMsg = String.format("%s.%s is not iceberg catalog", metalake, catalog);
            LOG.error(errorMsg);
            throw new RuntimeException(errorMsg);
          }

          Map<String, String> properties = entity.getProperties();
          if (!Boolean.parseBoolean(
              properties.getOrDefault(IcebergConstants.GRAVITINO_REST_PROXY, "true"))) {
            String errorMsg = String.format("%s.%s rest-proxy is false", metalake, catalog);
            LOG.error(errorMsg);
            throw new RuntimeException(errorMsg);
          }

          Map<String, String> catalogProperties = new HashMap<>(properties);
          catalogProperties.merge(
              IcebergConstants.CATALOG_BACKEND_NAME, catalog, (oldValue, newValue) -> oldValue);
          catalogProperties.put(
              IcebergConstants.ICEBERG_JDBC_PASSWORD,
              properties.getOrDefault(IcebergConstants.GRAVITINO_JDBC_PASSWORD, ""));
          catalogProperties.put(
              IcebergConstants.ICEBERG_JDBC_USER,
              properties.getOrDefault(IcebergConstants.GRAVITINO_JDBC_USER, ""));

          return new IcebergTableOps(new IcebergConfig(catalogProperties));
        });
  }

  @Override
  public void close() throws Exception {
    for (String catalog : icebergTableOpsMap.keySet()) {
      icebergTableOpsMap.get(catalog).close();
    }
    entityStore.close();
  }
}
