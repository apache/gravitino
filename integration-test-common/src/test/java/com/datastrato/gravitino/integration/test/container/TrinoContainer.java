/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.container;

import static java.lang.String.format;

import com.google.common.collect.ImmutableSet;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.rnorth.ducttape.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

public class TrinoContainer extends BaseContainer {
  public static final Logger LOG = LoggerFactory.getLogger(TrinoContainer.class);

  public static final String DEFAULT_IMAGE = System.getenv("GRAVITINO_CI_TRINO_DOCKER_IMAGE");
  public static final String HOST_NAME = "gravitino-ci-trino";
  public static final int TRINO_PORT = 8080;

  static Connection trinoJdbcConnection = null;

  public static final String TRINO_CONTAINER_PLUGIN_GRAVITINO_DIR =
      "/usr/lib/trino/plugin/gravitino";

  public static Builder builder() {
    return new Builder();
  }

  protected TrinoContainer(
      String image,
      String hostName,
      Set<Integer> ports,
      Map<String, String> extraHosts,
      Map<String, String> filesToMount,
      Map<String, String> envVars,
      Optional<Network> network) {
    super(image, hostName, ports, extraHosts, filesToMount, envVars, network);
  }

  @Override
  protected void setupContainer() {
    super.setupContainer();
    withLogConsumer(new PrintingContainerLog(format("%-15s| ", "TrinoContainer")));
  }

  @Override
  public void start() {
    super.start();

    Preconditions.check("Initialization Trino JDBC connect failed!", initTrinoJdbcConnection());
    Preconditions.check("Trino container startup failed!", checkContainerStatus(5));
  }

  @Override
  protected boolean checkContainerStatus(int retryLimit) {
    int nRetry = 0;
    boolean isTrinoJdbcConnectionReady = false;
    int sleepTime = 5000;
    while (nRetry++ < retryLimit && !isTrinoJdbcConnectionReady) {
      isTrinoJdbcConnectionReady = testTrinoJdbcConnection();
      if (isTrinoJdbcConnectionReady) {
        break;
      } else {
        try {
          Thread.sleep(sleepTime);
          LOG.warn("Waiting for trino server to be ready... ({}ms)", nRetry * sleepTime);
        } catch (InterruptedException e) {
          // ignore
        }
      }
    }
    return isTrinoJdbcConnectionReady;
  }

  @Override
  public void close() {
    if (trinoJdbcConnection != null) {
      try {
        trinoJdbcConnection.close();
      } catch (SQLException e) {
        LOG.error(e.getMessage(), e);
      }
    }

    super.close();
  }

  public boolean initTrinoJdbcConnection() {
    final String dbUrl =
        String.format("jdbc:trino://127.0.0.1:%d", getMappedPort(TrinoContainer.TRINO_PORT));

    long now = System.currentTimeMillis();
    boolean result = false;

    while (!result && System.currentTimeMillis() - now <= 20000) {
      try {
        trinoJdbcConnection = DriverManager.getConnection(dbUrl, "admin", "");
        result = true;
      } catch (SQLException e) {
        LOG.error(e.getMessage(), e);
      }
    }

    return result;
  }

  // Check tha Trino has synchronized the catalog from Gravitino
  public boolean checkSyncCatalogFromGravitino(
      int retryLimit, String metalakeName, String catalogName) {
    int nRetry = 0;
    int sleepTime = 5000;
    while (nRetry++ < retryLimit) {
      ArrayList<ArrayList<String>> queryData =
          executeQuerySQL(format("SHOW CATALOGS LIKE '%s.%s'", metalakeName, catalogName));
      for (ArrayList<String> record : queryData) {
        String columnValue = record.get(0);
        if (columnValue.equals(String.format("%s.%s", metalakeName, catalogName))) {
          return true;
        }
      }
      try {
        Thread.sleep(sleepTime);
        LOG.warn(
            "Waiting for Trino synchronized the catalog from Gravitino... ({}ms)",
            nRetry * sleepTime);
      } catch (InterruptedException e) {
        // ignore
      }
    }
    return false;
  }

  private boolean testTrinoJdbcConnection() {
    try (Statement stmt = trinoJdbcConnection.createStatement();
        ResultSet rs = stmt.executeQuery("select 1")) {
      while (rs.next()) {
        int one = rs.getInt(1);
        Preconditions.check("Trino JDBC connection test failed!", one == 1);
      }
    } catch (SQLException e) {
      // Maybe Trino server is still initialing
      LOG.warn(e.getMessage(), e);
      return false;
    }

    return true;
  }

  public ArrayList<ArrayList<String>> executeQuerySQL(String sql) {
    LOG.info("executeQuerySQL: {}", sql);
    ArrayList<ArrayList<String>> queryData = new ArrayList<>();
    try (Statement stmt = trinoJdbcConnection.createStatement();
        ResultSet rs = stmt.executeQuery(sql)) {
      ResultSetMetaData metaData = rs.getMetaData();
      int columnCount = metaData.getColumnCount();

      while (rs.next()) {
        ArrayList<String> record = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
          String columnValue = rs.getString(i);
          record.add(columnValue);
        }
        queryData.add(record);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return queryData;
  }

  public void executeUpdateSQL(String sql) {
    LOG.info("executeUpdateSQL: {}", sql);
    try (Statement stmt = trinoJdbcConnection.createStatement()) {
      stmt.executeUpdate(sql);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public static class Builder extends BaseContainer.Builder<Builder, TrinoContainer> {
    protected String trinoConfDir;
    protected String metalakeName;
    protected String hiveContainerIP;

    private Builder() {
      this.image = DEFAULT_IMAGE;
      this.hostName = HOST_NAME;
      this.exposePorts = ImmutableSet.of();
    }

    public Builder withTrinoConfDir(String trinoConfDir) {
      this.trinoConfDir = trinoConfDir;
      return self;
    }

    public Builder withMetalakeName(String metalakeName) {
      this.metalakeName = metalakeName;
      return self;
    }

    public Builder withHiveContainerIP(String hiveContainerIP) {
      this.hiveContainerIP = hiveContainerIP;
      return self;
    }

    @Override
    public TrinoContainer build() {
      return new TrinoContainer(
          image, hostName, exposePorts, extraHosts, filesToMount, envVars, network);
    }
  }
}
