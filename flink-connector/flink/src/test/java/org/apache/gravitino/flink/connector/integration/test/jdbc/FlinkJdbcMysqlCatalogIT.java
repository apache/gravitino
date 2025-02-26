package org.apache.gravitino.flink.connector.integration.test.jdbc;

import static org.apache.gravitino.integration.test.util.TestDatabaseName.MYSQL_CATALOG_MYSQL_IT;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.flink.connector.integration.test.FlinkCommonIT;
import org.apache.gravitino.flink.connector.jdbc.JdbcPropertiesConstants;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

@Tag("gravitino-docker-test")
public class FlinkJdbcMysqlCatalogIT extends FlinkCommonIT {

  protected String mysqlUrl;
  protected String mysqlUsername;
  protected String mysqlPassword;
  protected String mysqlDriver;
  protected String mysqlDefaultDatabase = MYSQL_CATALOG_MYSQL_IT.name();

  protected Catalog catalog;

  protected static final String CATALOG_NAME = "test_flink_jdbc_catalog";

  @Override
  protected Catalog currentCatalog() {
    return catalog;
  }

  @Override
  protected String getProvider() {
    return "jdbc-mysql";
  }

  @BeforeAll
  void setup() {
    init();
  }

  @Override
  protected boolean supportDropCascade() {
    return true;
  }

  private void init() {
    Preconditions.checkNotNull(metalake);
    catalog =
        metalake.createCatalog(
            CATALOG_NAME,
            org.apache.gravitino.Catalog.Type.RELATIONAL,
            getProvider(),
            null,
            ImmutableMap.of(
                JdbcPropertiesConstants.GRAVITINO_JDBC_USER,
                mysqlUsername,
                JdbcPropertiesConstants.GRAVITINO_JDBC_PASSWORD,
                mysqlPassword,
                JdbcPropertiesConstants.GRAVITINO_JDBC_URL,
                mysqlUrl,
                JdbcPropertiesConstants.GRAVITINO_JDBC_DRIVER,
                mysqlDriver,
                JdbcPropertiesConstants.GRAVITINO_JDBC_DEFAULT_DATABASE,
                mysqlDefaultDatabase));
  }

  @Override
  protected void initCatalogEnv() throws Exception {
    ContainerSuite containerSuite = ContainerSuite.getInstance();
    containerSuite.startMySQLContainer(MYSQL_CATALOG_MYSQL_IT);
    mysqlUrl = containerSuite.getMySQLContainer().getJdbcUrl();
    mysqlUsername = containerSuite.getMySQLContainer().getUsername();
    mysqlPassword = containerSuite.getMySQLContainer().getPassword();
    mysqlDriver = containerSuite.getMySQLContainer().getDriverClassName(MYSQL_CATALOG_MYSQL_IT);
  }
}
