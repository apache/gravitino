/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relation.mysql.orm;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.storage.relation.mysql.mapper.MetalakeMetaMapper;
import com.google.common.base.Preconditions;
import java.time.Duration;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.pool2.impl.BaseObjectPoolConfig;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;

public class SqlSessionFactoryHelper {
  private static volatile SqlSessionFactory sqlSessionFactory;
  private static final SqlSessionFactoryHelper INSTANCE = new SqlSessionFactoryHelper();

  public static SqlSessionFactoryHelper getInstance() {
    return INSTANCE;
  }

  @SuppressWarnings("deprecation")
  public void init(Config config) {
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUrl(config.getRawString(Configs.MYSQL_ENTITY_STORE_URL_KEY));
    dataSource.setDriverClassName(config.getRawString(Configs.MYSQL_ENTITY_STORE_DRIVER_NAME_KEY));
    dataSource.setUsername(config.getRawString(Configs.MYSQL_ENTITY_STORE_USERNAME_KEY, ""));
    dataSource.setPassword(config.getRawString(Configs.MYSQL_ENTITY_STORE_PASSWORD_KEY, ""));
    // close the auto commit, so that need manual commit
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
    TransactionFactory transactionFactory = new JdbcTransactionFactory();
    Environment environment = new Environment("development", transactionFactory, dataSource);
    Configuration configuration = new Configuration(environment);
    configuration.addMapper(MetalakeMetaMapper.class);
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
}
