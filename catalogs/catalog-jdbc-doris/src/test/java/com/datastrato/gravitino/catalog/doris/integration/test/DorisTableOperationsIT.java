/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.doris.integration.test;

import com.datastrato.gravitino.catalog.doris.converter.DorisColumnDefaultValueConverter;
import com.datastrato.gravitino.catalog.doris.converter.DorisExceptionConverter;
import com.datastrato.gravitino.catalog.doris.converter.DorisTypeConverter;
import com.datastrato.gravitino.catalog.doris.operation.DorisDatabaseOperations;
import com.datastrato.gravitino.catalog.doris.operation.DorisTableOperations;
import com.datastrato.gravitino.catalog.jdbc.JdbcColumn;
import com.datastrato.gravitino.catalog.jdbc.JdbcTable;
import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.datastrato.gravitino.catalog.jdbc.integration.test.TestJdbcAbstractIT;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import com.datastrato.gravitino.catalog.jdbc.utils.DataSourceUtils;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.DorisContainer;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-it")
public class DorisTableOperationsIT extends TestJdbcAbstractIT {
  public static final Logger LOG = LoggerFactory.getLogger(DorisTableOperationsIT.class);

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  protected static final String DRIVER_CLASS_NAME = "com.mysql.jdbc.Driver";
  protected static DataSource dataSource;

  protected static JdbcDatabaseOperations databaseOperations;

  protected static JdbcTableOperations tableOperations;

  protected static JdbcExceptionConverter jdbcExceptionConverter;

  private static final Type VARCHAR_255 = Types.VarCharType.of(255);
  private static final Type VARCHAR_1024 = Types.VarCharType.of(1024);

  private static final String databaseName = GravitinoITUtils.genRandomName("doris_test_db");
  private static final String tableName = GravitinoITUtils.genRandomName("doris_test_table");

  private static Type INT = Types.IntegerType.get();

  @BeforeAll
  public static void startup() {
    containerSuite.startDorisContainer();

    dataSource = DataSourceUtils.createDataSource(getCatalogProperties());

    databaseOperations = new DorisDatabaseOperations();
    tableOperations = new DorisTableOperations();
    jdbcExceptionConverter = new DorisExceptionConverter();
    databaseOperations.initialize(dataSource, jdbcExceptionConverter, Collections.emptyMap());
    tableOperations.initialize(
        dataSource,
        jdbcExceptionConverter,
        new DorisTypeConverter(),
        new DorisColumnDefaultValueConverter(),
        Collections.emptyMap());

    createDatabase();
  }

  private static void createDatabase() {
    databaseOperations.create(databaseName, "test_comment", null);
  }

  private static Map<String, String> getCatalogProperties() {
    Map<String, String> catalogProperties = Maps.newHashMap();

    DorisContainer dorisContainer = containerSuite.getDorisContainer();

    String jdbcUrl =
        String.format(
            "jdbc:mysql://%s:%d/",
            dorisContainer.getContainerIpAddress(), DorisContainer.FE_MYSQL_PORT);

    catalogProperties.put(JdbcConfig.JDBC_URL.getKey(), jdbcUrl);
    catalogProperties.put(JdbcConfig.JDBC_DRIVER.getKey(), DRIVER_CLASS_NAME);
    catalogProperties.put(JdbcConfig.USERNAME.getKey(), DorisContainer.USER_NAME);
    catalogProperties.put(JdbcConfig.PASSWORD.getKey(), DorisContainer.PASSWORD);

    return catalogProperties;
  }

  private static Map<String, String> createProperties() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("replication_allocation", "tag.location.default: 1");
    return properties;
  }

  private static void waitForDorisOperation() {
    // TODO: use a better way to wait for the operation to complete
    // see: https://doris.apache.org/docs/1.2/advanced/alter-table/schema-change/
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      // do nothing
    }
  }

  @Test
  public void testAlterTable() {

    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    JdbcColumn col_1 =
        new JdbcColumn.Builder().withName("col_1").withType(INT).withComment("id").build();
    columns.add(col_1);
    JdbcColumn col_2 =
        new JdbcColumn.Builder()
            .withName("col_2")
            .withType(VARCHAR_255)
            .withComment("col_2")
            .build();
    columns.add(col_2);
    JdbcColumn col_3 =
        new JdbcColumn.Builder()
            .withName("col_3")
            .withType(VARCHAR_255)
            .withComment("col_3")
            .build();
    columns.add(col_3);
    Map<String, String> properties = new HashMap<>();

    Distribution distribution = Distributions.hash(32, NamedReference.field("col_1"));
    Index[] indexes = new Index[] {};

    // create table
    tableOperations.create(
        databaseName,
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        createProperties(),
        null,
        distribution,
        indexes);
    JdbcTable load = tableOperations.load(databaseName, tableName);
    assertionsTableInfo(tableName, tableComment, columns, properties, indexes, load);

    tableOperations.alterTable(
        databaseName,
        tableName,
        TableChange.updateColumnType(new String[] {col_3.name()}, VARCHAR_1024));

    waitForDorisOperation();

    load = tableOperations.load(databaseName, tableName);

    // After modifying the type, check it
    columns.clear();
    col_3 =
        new JdbcColumn.Builder()
            .withName(col_3.name())
            .withType(VARCHAR_1024)
            .withComment(col_3.comment())
            .build();
    columns.add(col_1);
    columns.add(col_2);
    columns.add(col_3);
    assertionsTableInfo(tableName, tableComment, columns, properties, indexes, load);

    String colNewComment = "new_comment";
    // update column comment

    tableOperations.alterTable(
        databaseName,
        tableName,
        TableChange.updateColumnComment(new String[] {col_2.name()}, colNewComment));
    load = tableOperations.load(databaseName, tableName);

    columns.clear();
    col_2 =
        new JdbcColumn.Builder()
            .withName(col_2.name())
            .withType(col_2.dataType())
            .withComment(colNewComment)
            .build();
    columns.add(col_1);
    columns.add(col_2);
    columns.add(col_3);
    assertionsTableInfo(tableName, tableComment, columns, properties, indexes, load);

    // add new column
    tableOperations.alterTable(
        databaseName,
        tableName,
        TableChange.addColumn(new String[] {"col_4"}, VARCHAR_255, "txt4", true));

    waitForDorisOperation();
    load = tableOperations.load(databaseName, tableName);

    columns.clear();
    JdbcColumn col_4 =
        new JdbcColumn.Builder()
            .withName("col_4")
            .withType(VARCHAR_255)
            .withComment("txt4")
            .build();
    columns.add(col_1);
    columns.add(col_2);
    columns.add(col_3);
    columns.add(col_4);
    assertionsTableInfo(tableName, tableComment, columns, properties, indexes, load);

    // change column position
    tableOperations.alterTable(
        databaseName,
        tableName,
        TableChange.updateColumnPosition(
            new String[] {"col_3"}, TableChange.ColumnPosition.after("col_4")));
    waitForDorisOperation();
    load = tableOperations.load(databaseName, tableName);

    columns.clear();
    columns.add(col_1);
    columns.add(col_2);
    columns.add(col_4);
    columns.add(col_3);
    assertionsTableInfo(tableName, tableComment, columns, properties, indexes, load);
  }

  @Test
  public void testCreateAllTypeTable() {
    String tableName = GravitinoITUtils.genRandomName("type_table");
    String tableComment = "test_comment";
    List<JdbcColumn> columns = new ArrayList<>();
    columns.add(
        new JdbcColumn.Builder().withName("col_1").withType(Types.IntegerType.get()).build());
    columns.add(
        new JdbcColumn.Builder().withName("col_2").withType(Types.BooleanType.get()).build());
    columns.add(new JdbcColumn.Builder().withName("col_3").withType(Types.ByteType.get()).build());
    columns.add(new JdbcColumn.Builder().withName("col_4").withType(Types.ShortType.get()).build());
    columns.add(
        new JdbcColumn.Builder().withName("col_5").withType(Types.IntegerType.get()).build());
    columns.add(new JdbcColumn.Builder().withName("col_6").withType(Types.LongType.get()).build());
    columns.add(new JdbcColumn.Builder().withName("col_7").withType(Types.FloatType.get()).build());
    columns.add(
        new JdbcColumn.Builder().withName("col_8").withType(Types.DoubleType.get()).build());
    columns.add(
        new JdbcColumn.Builder().withName("col_9").withType(Types.DecimalType.of(10, 2)).build());
    columns.add(new JdbcColumn.Builder().withName("col_10").withType(Types.DateType.get()).build());
    columns.add(new JdbcColumn.Builder().withName("col_11").withType(Types.TimeType.get()).build());
    columns.add(
        new JdbcColumn.Builder().withName("col_12").withType(Types.FixedCharType.of(10)).build());
    columns.add(
        new JdbcColumn.Builder().withName("col_13").withType(Types.VarCharType.of(10)).build());
    columns.add(
        new JdbcColumn.Builder().withName("col_14").withType(Types.StringType.get()).build());

    Distribution distribution = Distributions.hash(32, NamedReference.field("col_1"));
    Index[] indexes = new Index[] {};
    // create table
    tableOperations.create(
        databaseName,
        tableName,
        columns.toArray(new JdbcColumn[0]),
        tableComment,
        createProperties(),
        null,
        distribution,
        indexes);

    JdbcTable load = tableOperations.load(databaseName, tableName);
    assertionsTableInfo(tableName, tableComment, columns, Collections.emptyMap(), null, load);
  }
}
