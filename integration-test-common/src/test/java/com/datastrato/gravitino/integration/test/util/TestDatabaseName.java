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
 *   <li>{@link #MYSQL_JdbcBackend}: Represents the MySQL database used for testing the JDBC backend
 *       of Gravitino.
 *   <li>{@link #MYSQL_MysqlAbstractIT}: Represents the MySQL database used for testing the
 *       MysqlAbstractIT and its subclasses.
 *   <li>{@link #MYSQL_AuditCatalogMysqlIT}: Represents the MySQL database used for testing the
 *       AuditCatalogMysqlIT.
 *   <li>{@link #MYSQL_CatalogMysqlIT}: Represents the MySQL database used for testing the catalog
 *       integration with MySQL.
 * </ul>
 */
public enum TestDatabaseName {
  /** Represents the MySQL database used for JDBC backend of Gravitino. */
  MYSQL_JdbcBackend,

  /** Represents the MySQL database for MysqlAbstractIT and its subclasses. */
  MYSQL_MysqlAbstractIT,

  /** Represents the MySQL database for AudtCatalogMysqlIT. */
  MYSQL_AuditCatalogMysqlIT,

  /** Represents the MySQL database used for testing the catalog integration with MySQL. */
  MYSQL_CatalogMysqlIT
}
