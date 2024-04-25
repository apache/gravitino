/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.util;

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
 * </ul>
 */
public enum TestDatabaseName {
  /** Represents the MySQL database used for JDBC backend of Gravitino. */
  MYSQL_JDBC_BACKEND,

  /** Represents the MySQL database for MysqlAbstractIT and its subclasses. */
  MYSQL_MYSQL_ABSTRACT_IT,

  /** Represents the MySQL database for AudtCatalogMysqlIT. */
  MYSQL_AUDIT_CATALOG_MYSQL_IT,

  /** Represents the MySQL database used for testing the catalog integration with MySQL. */
  MYSQL_CATALOG_MYSQL_IT,

  /** Represents the PostgreSQL database for AudtCatalogPostgreSqlIT. */
  PG_AUDIT_CATALOG_PostgreSql_IT {
    /** PostgreSQL only accept lowercase database name */
    @Override
    public String toString() {
      return this.name().toLowerCase();
    }
  },

  /** Represents the PostgreSQL database for postgresql.integration.test.TestMultipleJDBCLoad. */
  PG_TEST_PG_CATALOG_MULTIPLE_JDBC_lOAD {
    /** PostgreSQL only accept lowercase database name */
    @Override
    public String toString() {
      return this.name().toLowerCase();
    }
  },

    /** Represents the PostgreSQL database for lakehouse.iceberg.integration.test.TestMultipleJDBCLoad. */
  PG_TEST_ICEBERG_CATALOG_MULTIPLE_JDBC_lOAD {
    /** PostgreSQL only accept lowercase database name */
    @Override
    public String toString() {
      return this.name().toLowerCase();
    }
  },
}
