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
package org.apache.gravitino.integration.test.util;

/**
 * An enum representing the different test database names used for testing purposes in the Gravitino
 * project.
 *
 * <p>This enum provides a set of predefined database names that can be used in test cases to
 * specify the target MySQL database for testing various functionalities and components of
 * Gravitino.
 *
 * <p>The available test database names are:
 *
 * <ul>
 *   <li>{@link #MYSQL_JDBC_BACKEND}: Represents the MySQL database used for testing the JDBC
 *       backend of Gravitino.
 *   <li>{@link #MYSQL_MYSQL_ABSTRACT_IT}: Represents the MySQL database used for testing the
 *       MysqlAbstractIT and its subclasses.
 *   <li>{@link #MYSQL_AUDIT_CATALOG_MYSQL_IT}: Represents the MySQL database used for testing the
 *       AuditCatalogMysqlIT.
 *   <li>{@link #MYSQL_CATALOG_MYSQL_IT}: Represents the MySQL database used for testing the catalog
 *       integration with MySQL.
 *   <li>{@link #PG_CATALOG_POSTGRESQL_IT}: Represents the PostgreSQL database for
 *       CatalogPostgreSqlIT.
 *   <li>{@link #PG_TEST_PG_CATALOG_MULTIPLE_JDBC_LOAD}: Represents the PostgreSQL database for
 *       postgresql.integration.test.TestMultipleJDBCLoad.
 *   <li>{@link #PG_TEST_ICEBERG_CATALOG_MULTIPLE_JDBC_LOAD}: Represents the PostgreSQL database for
 *       lakehouse.iceberg.integration.test.TestMultipleJDBCLoad.
 *   <li>{@link #PG_TEST_PAIMON_CATALOG_MULTIPLE_JDBC_LOAD}: Represents the PostgreSQL database for
 *       lakehouse.paimon.integration.test.CatalogPaimonMultipleJDBCLoadIT.
 * </ul>
 */
public enum TestDatabaseName {
  /** Represents the MySQL database used for JDBC backend of Gravitino. */
  MYSQL_JDBC_BACKEND,

  /** Represents the MySQL database for MysqlAbstractIT and its subclasses. */
  MYSQL_MYSQL_ABSTRACT_IT,

  /** Represents the MySQL database for AuditCatalogMysqlIT. */
  MYSQL_AUDIT_CATALOG_MYSQL_IT,

  /** Represents the MySQL database used for testing the catalog integration with MySQL. */
  MYSQL_CATALOG_MYSQL_IT,

  PG_JDBC_BACKEND,

  /** Represents the PostgreSQL database for CatalogPostgreSqlIT. */
  PG_CATALOG_POSTGRESQL_IT {
    /** PostgreSQL only accept lowercase database name */
    @Override
    public String toString() {
      return this.name().toLowerCase();
    }
  },

  /** Represents the PostgreSQL database for postgresql.integration.test.TestMultipleJDBCLoad. */
  PG_TEST_PG_CATALOG_MULTIPLE_JDBC_LOAD {
    /** PostgreSQL only accept lowercase database name */
    @Override
    public String toString() {
      return this.name().toLowerCase();
    }
  },

  /**
   * Represents the PostgreSQL database for lakehouse.iceberg.integration.test.TestMultipleJDBCLoad.
   */
  PG_TEST_ICEBERG_CATALOG_MULTIPLE_JDBC_LOAD {
    /** PostgreSQL only accept lowercase database name */
    @Override
    public String toString() {
      return this.name().toLowerCase();
    }
  },

  /**
   * Represents the PostgreSQL database for
   * lakehouse.paimon.integration.test.CatalogPaimonMultipleJDBCLoadIT.
   */
  PG_TEST_PAIMON_CATALOG_MULTIPLE_JDBC_LOAD {
    /** PostgreSQL only accept lowercase database name */
    @Override
    public String toString() {
      return this.name().toLowerCase();
    }
  },

  /**
   * Represents the PostgreSQL database for
   * org.apache.gravitino.spark.connector.integration.test.jdbc.SparkJdbcPostgreSqlCatalogIT.
   */
  PG_CATALOG_PG_IT {
    /** PostgreSQL only accept lowercase database name */
    @Override
    public String toString() {
      return this.name().toLowerCase();
    }
  },
  PG_ICEBERG_AUTHZ_IT,
}
