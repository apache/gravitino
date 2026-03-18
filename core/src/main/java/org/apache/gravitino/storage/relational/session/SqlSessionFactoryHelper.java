/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.storage.relational.session;

import com.google.common.base.Preconditions;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ServiceLoader;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.BaseObjectPoolConfig;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.metrics.MetricsSystem;
import org.apache.gravitino.metrics.source.MetricsSource;
import org.apache.gravitino.metrics.source.RelationDatasourceMetricsSource;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.mapper.provider.MapperPackageProvider;
import org.apache.gravitino.utils.JdbcUrlUtils;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SqlSessionFactoryHelper maintains MyBatis {@link SqlSessionFactory} instances for the entity
 * store: a primary (write) factory and optionally a separate read-only factory when any read-only
 * config is set (jdbcReadOnlyUrl, jdbcReadOnlyUser, jdbcReadOnlyPassword, readOnlyMaxConnections, or
 * readOnlyMaxWaitMillis). When none are set, a single factory is used for both reads and writes.
 */
public class SqlSessionFactoryHelper {
  private static final Logger LOG = LoggerFactory.getLogger(SqlSessionFactoryHelper.class);
  private static volatile SqlSessionFactory writeSqlSessionFactory;
  private static volatile SqlSessionFactory readSqlSessionFactory;
  private static final SqlSessionFactoryHelper INSTANCE = new SqlSessionFactoryHelper();

  public static SqlSessionFactoryHelper getInstance() {
    return INSTANCE;
  }

  private SqlSessionFactoryHelper() {}

  /**
   * Initialize SqlSessionFactory instances from config. When no read-only config is set, a single
   * factory serves both reads and writes. When any read-only config is set, a separate read pool
   * is created.
   */
  @SuppressWarnings("deprecation")
  public void init(Config config) {
    if (writeSqlSessionFactory != null) {
      return;
    }

    synchronized (SqlSessionFactoryHelper.class) {
      if (writeSqlSessionFactory != null) {
        return;
      }

      String jdbcUrl = config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL);
      String driverClass = config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER);
      JdbcUrlUtils.validateJdbcConfig(driverClass, jdbcUrl, config.getAllConfig());

      JDBCBackendType jdbcType = JDBCBackendType.fromURI(jdbcUrl);
      BasicDataSource writeDataSource = createDataSource(config, jdbcUrl, driverClass);

      String readJdbcUrlRaw = config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_READ_ONLY_URL);
      boolean readOnlyUrlSet = StringUtils.isNotBlank(readJdbcUrlRaw);
      String readJdbcUrl = readOnlyUrlSet ? readJdbcUrlRaw : jdbcUrl;
      if (readOnlyUrlSet) {
        JdbcUrlUtils.validateJdbcConfig(driverClass, readJdbcUrl, config.getAllConfig());
        JDBCBackendType readType = JDBCBackendType.fromURI(readJdbcUrl);
        Preconditions.checkArgument(
            readType == jdbcType,
            "Read-only JDBC URL must use the same database kind as the primary URL (e.g. both"
                + " MySQL). Primary: %s, read-only: %s",
            jdbcType,
            readType);
      }

      Integer readOnlyMaxConn = config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_READ_ONLY_MAX_CONNECTIONS);
      Long readOnlyMaxWait = config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_READ_ONLY_MAX_WAIT_MILLISECONDS);
      boolean anyReadOnlyConfigSet =
          readOnlyUrlSet
              || StringUtils.isNotBlank(
                  config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_READ_ONLY_USER))
              || StringUtils.isNotBlank(
                  config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_READ_ONLY_PASSWORD))
              || (readOnlyMaxConn != null
                  && readOnlyMaxConn != Configs.JDBC_READ_ONLY_POOL_INHERIT_MAX_CONNECTIONS)
              || (readOnlyMaxWait != null
                  && readOnlyMaxWait != Configs.JDBC_READ_ONLY_POOL_INHERIT_MAX_WAIT_MILLIS);

      writeSqlSessionFactory = buildSqlSessionFactory(writeDataSource, jdbcType);

      if (!anyReadOnlyConfigSet) {
        readSqlSessionFactory = writeSqlSessionFactory;
      } else {
        LOG.info("Read replica JDBC pool enabled for entity store; read traffic will use replica.");
        BasicDataSource readDataSource = createReadOnlyDataSource(config, readJdbcUrl, driverClass);
        readSqlSessionFactory = buildSqlSessionFactory(readDataSource, jdbcType);
      }

      MetricsSystem metricsSystem = GravitinoEnv.getInstance().metricsSystem();
      if (metricsSystem != null) {
        metricsSystem.register(new RelationDatasourceMetricsSource(writeDataSource));
        if (readSqlSessionFactory != writeSqlSessionFactory) {
          BasicDataSource readDs =
              (BasicDataSource)
                  readSqlSessionFactory.getConfiguration().getEnvironment().getDataSource();
          metricsSystem.register(
              new RelationDatasourceMetricsSource(
                  readDs, MetricsSource.GRAVITINO_RELATIONAL_STORE_READ_METRIC_NAME));
        }
      }
    }
  }

  @SuppressWarnings("deprecation")
  private static BasicDataSource createDataSource(
      Config config, String jdbcUrl, String driverClass) {
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl(jdbcUrl);
    dataSource.setDriverClassName(driverClass);
    dataSource.setUsername(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER));
    dataSource.setPassword(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD));
    dataSource.setDefaultAutoCommit(false);
    setWritePoolProperties(config, dataSource);
    return dataSource;
  }

  /**
   * Creates a DataSource for the read-only replica, using read-only URL and optional credentials.
   */
  @SuppressWarnings("deprecation")
  private static BasicDataSource createReadOnlyDataSource(
      Config config, String readJdbcUrl, String driverClass) {
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl(readJdbcUrl);
    dataSource.setDriverClassName(driverClass);
    String readUser = config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_READ_ONLY_USER);
    String readPassword = config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_READ_ONLY_PASSWORD);
    dataSource.setUsername(
        StringUtils.isBlank(readUser)
            ? config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER)
            : readUser);
    dataSource.setPassword(
        StringUtils.isBlank(readPassword)
            ? config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD)
            : readPassword);
    dataSource.setDefaultAutoCommit(false);
    setReadOnlyPoolProperties(config, dataSource);
    return dataSource;
  }

  @SuppressWarnings("deprecation")
  private static void setWritePoolProperties(Config config, BasicDataSource dataSource) {
    dataSource.setMaxWaitMillis(
        config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS));
    dataSource.setMaxTotal(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS));
    applySharedPoolProperties(dataSource);
  }

  @SuppressWarnings("deprecation")
  private static void setReadOnlyPoolProperties(Config config, BasicDataSource dataSource) {
    Integer readMax = config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_READ_ONLY_MAX_CONNECTIONS);
    int maxTotal =
        (readMax == null || readMax == Configs.JDBC_READ_ONLY_POOL_INHERIT_MAX_CONNECTIONS)
            ? config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS)
            : readMax;
    dataSource.setMaxTotal(maxTotal);
    Long readWait = config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_READ_ONLY_MAX_WAIT_MILLISECONDS);
    long maxWait =
        (readWait == null || readWait == Configs.JDBC_READ_ONLY_POOL_INHERIT_MAX_WAIT_MILLIS)
            ? config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS)
            : readWait;
    dataSource.setMaxWaitMillis(maxWait);
    applySharedPoolProperties(dataSource);
  }

  @SuppressWarnings("deprecation")
  private static void applySharedPoolProperties(BasicDataSource dataSource) {
    dataSource.setMaxIdle(5);
    dataSource.setMinIdle(0);
    dataSource.setLogAbandoned(true);
    dataSource.setRemoveAbandonedOnBorrow(true);
    dataSource.setRemoveAbandonedTimeout(60);
    dataSource.setTimeBetweenEvictionRunsMillis(Duration.ofMillis(10 * 60 * 1000L).toMillis());
    dataSource.setTestOnBorrow(true);
    dataSource.setTestWhileIdle(true);
    dataSource.setMinEvictableIdleTimeMillis(1000);
    dataSource.setNumTestsPerEvictionRun(BaseObjectPoolConfig.DEFAULT_NUM_TESTS_PER_EVICTION_RUN);
    dataSource.setTestOnReturn(BaseObjectPoolConfig.DEFAULT_TEST_ON_RETURN);
    dataSource.setSoftMinEvictableIdleTimeMillis(
        BaseObjectPoolConfig.DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME.toMillis());
    dataSource.setLifo(BaseObjectPoolConfig.DEFAULT_LIFO);
  }

  private static SqlSessionFactory buildSqlSessionFactory(
      BasicDataSource dataSource, JDBCBackendType jdbcType) {
    TransactionFactory transactionFactory = new JdbcTransactionFactory();
    Environment environment = new Environment("development", transactionFactory, dataSource);
    Configuration configuration = new Configuration(environment);
    configuration.setDatabaseId(jdbcType.name().toLowerCase());
    ServiceLoader<MapperPackageProvider> loader = ServiceLoader.load(MapperPackageProvider.class);
    for (MapperPackageProvider provider : loader) {
      provider.getMapperClasses().forEach(configuration::addMapper);
    }
    return new SqlSessionFactoryBuilder().build(configuration);
  }

  /** SqlSessionFactory for writes and transactional reads within a write transaction. */
  public SqlSessionFactory getWriteSqlSessionFactory() {
    Preconditions.checkState(
        writeSqlSessionFactory != null, "SqlSessionFactory is not initialized.");
    return writeSqlSessionFactory;
  }

  /**
   * SqlSessionFactory for read-only queries. When no read-only config is set, identical to the
   * write factory.
   */
  public SqlSessionFactory getReadSqlSessionFactory() {
    Preconditions.checkState(
        readSqlSessionFactory != null, "SqlSessionFactory is not initialized.");
    return readSqlSessionFactory;
  }

  /**
   * Returns the primary (write) SqlSessionFactory. Prefer {@link #getWriteSqlSessionFactory()} or
   * {@link #getReadSqlSessionFactory()} for clarity.
   */
  public SqlSessionFactory getSqlSessionFactory() {
    return getWriteSqlSessionFactory();
  }

  public void close() {
    if (writeSqlSessionFactory == null) {
      return;
    }
    synchronized (SqlSessionFactoryHelper.class) {
      if (writeSqlSessionFactory == null) {
        return;
      }
      try {
        if (readSqlSessionFactory != null && readSqlSessionFactory != writeSqlSessionFactory) {
          closeDataSourceOf(readSqlSessionFactory);
        }
        closeDataSourceOf(writeSqlSessionFactory);
      } finally {
        writeSqlSessionFactory = null;
        readSqlSessionFactory = null;
      }
    }
  }

  private static void closeDataSourceOf(SqlSessionFactory factory) {
    try {
      BasicDataSource dataSource =
          (BasicDataSource) factory.getConfiguration().getEnvironment().getDataSource();
      dataSource.close();
    } catch (SQLException e) {
      // ignore
    }
  }
}
