/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.session;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.storage.relational.mapper.CatalogMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.FilesetMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.FilesetVersionMapper;
import com.datastrato.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.SchemaMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.TableMetaMapper;
import com.google.common.base.Preconditions;
import java.sql.SQLException;
import java.time.Duration;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.pool2.impl.BaseObjectPoolConfig;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;

/**
 * SqlSessionFactoryHelper maintains the MyBatis's {@link SqlSessionFactory} object, which is used
 * to create the {@link org.apache.ibatis.session.SqlSession} object. It is a singleton class and
 * should be initialized only once.
 */
public class SqlSessionFactoryHelper {
  private static volatile SqlSessionFactory sqlSessionFactory;
  private static final SqlSessionFactoryHelper INSTANCE = new SqlSessionFactoryHelper();

  public static SqlSessionFactoryHelper getInstance() {
    return INSTANCE;
  }

  private SqlSessionFactoryHelper() {}

  /**
   * Initialize the SqlSessionFactory object.
   *
   * @param config Config object to get the jdbc connection details from the config.
   */
  @SuppressWarnings("deprecation")
  public void init(Config config) {
    // Initialize the data source
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL));
    dataSource.setDriverClassName(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER));
    dataSource.setUsername(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER));
    dataSource.setPassword(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD));
    // Close the auto commit, so that we can control the transaction manual commit
    dataSource.setDefaultAutoCommit(false);
    dataSource.setMaxWaitMillis(1000L);
    dataSource.setMaxTotal(20);
    dataSource.setMaxIdle(5);
    dataSource.setMinIdle(0);
    dataSource.setLogAbandoned(true);
    dataSource.setRemoveAbandonedOnBorrow(true);
    dataSource.setRemoveAbandonedTimeout(60);
    dataSource.setTimeBetweenEvictionRunsMillis(Duration.ofMillis(10 * 60 * 1000L).toMillis());
    dataSource.setTestOnBorrow(BaseObjectPoolConfig.DEFAULT_TEST_ON_BORROW);
    dataSource.setTestWhileIdle(BaseObjectPoolConfig.DEFAULT_TEST_WHILE_IDLE);
    dataSource.setMinEvictableIdleTimeMillis(1000);
    dataSource.setNumTestsPerEvictionRun(BaseObjectPoolConfig.DEFAULT_NUM_TESTS_PER_EVICTION_RUN);
    dataSource.setTestOnReturn(BaseObjectPoolConfig.DEFAULT_TEST_ON_RETURN);
    dataSource.setSoftMinEvictableIdleTimeMillis(
        BaseObjectPoolConfig.DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME.toMillis());
    dataSource.setLifo(BaseObjectPoolConfig.DEFAULT_LIFO);

    // Create the transaction factory and env
    TransactionFactory transactionFactory = new JdbcTransactionFactory();
    Environment environment = new Environment("development", transactionFactory, dataSource);

    // Initialize the configuration
    Configuration configuration = new Configuration(environment);
    configuration.addMapper(MetalakeMetaMapper.class);
    configuration.addMapper(CatalogMetaMapper.class);
    configuration.addMapper(SchemaMetaMapper.class);
    configuration.addMapper(TableMetaMapper.class);
    configuration.addMapper(FilesetMetaMapper.class);
    configuration.addMapper(FilesetVersionMapper.class);

    // Create the SqlSessionFactory object, it is a singleton object
    if (sqlSessionFactory == null) {
      synchronized (SqlSessionFactoryHelper.class) {
        if (sqlSessionFactory == null) {
          sqlSessionFactory = new SqlSessionFactoryBuilder().build(configuration);
        }
      }
    }
  }

  public SqlSessionFactory getSqlSessionFactory() {
    Preconditions.checkState(sqlSessionFactory != null, "SqlSessionFactory is not initialized.");
    return sqlSessionFactory;
  }

  public void close() {
    if (sqlSessionFactory != null) {
      synchronized (SqlSessionFactoryHelper.class) {
        if (sqlSessionFactory != null) {
          try {
            BasicDataSource dataSource =
                (BasicDataSource)
                    sqlSessionFactory.getConfiguration().getEnvironment().getDataSource();
            dataSource.close();
          } catch (SQLException e) {
            // silently ignore the error report
          }
          sqlSessionFactory = null;
        }
      }
    }
  }
}
