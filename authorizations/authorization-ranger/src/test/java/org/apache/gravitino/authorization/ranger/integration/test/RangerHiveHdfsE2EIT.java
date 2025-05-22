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
package org.apache.gravitino.authorization.ranger.integration.test;

import static org.apache.gravitino.Catalog.AUTHORIZATION_PROVIDER;
import static org.apache.gravitino.catalog.hive.HiveConstants.IMPERSONATION_ENABLE;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.common.RangerAuthorizationProperties;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.container.RangerContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.types.Types;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-test")
public class RangerHiveHdfsE2EIT extends RangerBaseE2EIT {

  private static final Logger LOG = LoggerFactory.getLogger(RangerHiveHdfsE2EIT.class);
  private static final String provider = "hive";
  private static final String HADOOP_USER_NAME = "HADOOP_USER_NAME";
  public static final String HIVE_COL_NAME1 = "hive_col_name1";
  public static final String HIVE_COL_NAME2 = "hive_col_name2";
  public static final String HIVE_COL_NAME3 = "hive_col_name3";
  public static final String testUserName = "test";

  private static String DEFAULT_FS;

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    metalakeName = GravitinoITUtils.genRandomName("metalake").toLowerCase();

    // Enable Gravitino Authorization mode
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), String.valueOf(true));
    configs.put(Configs.SERVICE_ADMINS.getKey(), RangerITEnv.HADOOP_USER_NAME);
    configs.put(Configs.AUTHENTICATORS.getKey(), AuthenticatorType.SIMPLE.name().toLowerCase());
    configs.put("SimpleAuthUserName", AuthConstants.ANONYMOUS_USER);
    registerCustomConfigs(configs);

    super.startIntegrationTest();
    RangerITEnv.init(RangerBaseE2EIT.metalakeName, false);
    RangerITEnv.startHiveRangerContainer();

    DEFAULT_FS =
        String.format(
            "hdfs://%s:%d/user/hive/warehouse",
            containerSuite.getHiveRangerContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT);

    HIVE_METASTORE_URIS =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveRangerContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);

    createMetalake();
    createCatalog();

    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", DEFAULT_FS);

    RangerITEnv.cleanup();

    try {
      metalake.addUser(System.getenv(HADOOP_USER_NAME));
      metalake.addUser(testUserName);
    } catch (UserAlreadyExistsException e) {
      LOG.error("Failed to add user: {}", System.getenv(HADOOP_USER_NAME), e);
    }
  }

  @AfterAll
  public void stop() {
    cleanIT();
  }

  protected void createCatalog() {
    Map<String, String> properties =
        ImmutableMap.of(
            HiveConstants.METASTORE_URIS,
            HIVE_METASTORE_URIS,
            IMPERSONATION_ENABLE,
            "true",
            AUTHORIZATION_PROVIDER,
            "ranger",
            RangerAuthorizationProperties.RANGER_SERVICE_TYPE,
            "HDFS",
            RangerAuthorizationProperties.RANGER_SERVICE_NAME,
            RangerITEnv.RANGER_HDFS_REPO_NAME,
            RangerAuthorizationProperties.RANGER_ADMIN_URL,
            RangerITEnv.RANGER_ADMIN_URL,
            RangerAuthorizationProperties.RANGER_AUTH_TYPE,
            RangerContainer.authType,
            RangerAuthorizationProperties.RANGER_USERNAME,
            RangerContainer.rangerUserName,
            RangerAuthorizationProperties.RANGER_PASSWORD,
            RangerContainer.rangerPassword);

    metalake.createCatalog(catalogName, Catalog.Type.RELATIONAL, provider, "comment", properties);
    catalog = metalake.loadCatalog(catalogName);
    LOG.info("Catalog created: {}", catalog);
  }

  @Test
  public void testRenameTable() throws Exception {
    cleanSchemaPath();
    MetadataObject metadataObject =
        MetadataObjects.of(Lists.newArrayList(metalakeName), MetadataObject.Type.METALAKE);
    MetadataObject catalogObject =
        MetadataObjects.of(Lists.newArrayList(catalogName), MetadataObject.Type.CATALOG);
    metalake.setOwner(metadataObject, AuthConstants.ANONYMOUS_USER, Owner.Type.USER);
    metalake.setOwner(catalogObject, AuthConstants.ANONYMOUS_USER, Owner.Type.USER);
    waitForUpdatingPolicies();

    // 1. Create a table
    TableCatalog tableCatalog = catalog.asTableCatalog();
    tableCatalog.createTable(
        NameIdentifier.of("default", "test"), createColumns(), "comment1", ImmutableMap.of());

    // 2. check the privileges, should throw an exception
    UserGroupInformation.createProxyUser(testUserName, UserGroupInformation.getCurrentUser())
        .doAs(
            (PrivilegedExceptionAction<Void>)
                () -> {
                  Configuration conf = new Configuration();
                  conf.set("fs.defaultFS", DEFAULT_FS);
                  String path = String.format("%s/test", DEFAULT_FS);
                  FileSystem userFileSystem = FileSystem.get(conf);
                  Exception e =
                      Assertions.assertThrows(
                          Exception.class,
                          () ->
                              userFileSystem.mkdirs(
                                  new Path(String.format("%s/%s", path, "test1"))));
                  Assertions.assertTrue(e.getMessage().contains("Permission denied"));
                  userFileSystem.close();
                  return null;
                });

    // 3. Grant the privileges to the table
    SecurableObject catalogSecObject =
        SecurableObjects.ofCatalog(catalogName, Collections.emptyList());
    SecurableObject schemaObject =
        SecurableObjects.ofSchema(catalogSecObject, "default", Collections.emptyList());
    SecurableObject tableObject =
        SecurableObjects.ofTable(
            schemaObject, "test", Lists.newArrayList(Privileges.ModifyTable.allow()));
    metalake.createRole(
        "hdfs_rename_role", Collections.emptyMap(), Lists.newArrayList(tableObject));

    metalake.grantRolesToUser(Lists.newArrayList("hdfs_rename_role"), testUserName);
    waitForUpdatingPolicies();

    UserGroupInformation.createProxyUser(testUserName, UserGroupInformation.getCurrentUser())
        .doAs(
            (PrivilegedExceptionAction<Void>)
                () -> {
                  Configuration conf = new Configuration();
                  conf.set("fs.defaultFS", DEFAULT_FS);
                  String path = String.format("%s/test", DEFAULT_FS);
                  FileSystem userFileSystem = FileSystem.get(conf);
                  Assertions.assertDoesNotThrow(() -> userFileSystem.listStatus(new Path(path)));
                  Assertions.assertDoesNotThrow(
                      () -> userFileSystem.mkdirs(new Path(String.format("%s/%s", path, "test1"))));
                  userFileSystem.close();
                  return null;
                });
    // 4. Rename the table
    tableCatalog.alterTable(NameIdentifier.of("default", "test"), TableChange.rename("test1"));

    // 5. Check the privileges
    UserGroupInformation.createProxyUser(testUserName, UserGroupInformation.getCurrentUser())
        .doAs(
            (PrivilegedExceptionAction<Void>)
                () -> {
                  Configuration conf = new Configuration();
                  conf.set("fs.defaultFS", DEFAULT_FS);
                  String path = String.format("%s/test1", DEFAULT_FS);
                  FileSystem userFileSystem = FileSystem.get(conf);
                  Assertions.assertDoesNotThrow(() -> userFileSystem.listStatus(new Path(path)));
                  Assertions.assertDoesNotThrow(
                      () -> userFileSystem.mkdirs(new Path(String.format("%s/%s", path, "test1"))));
                  userFileSystem.close();
                  return null;
                });

    // 6. Delete the table
    tableCatalog.dropTable(NameIdentifier.of("default", "test1"));
  }

  private Column[] createColumns() {
    Column col1 = Column.of(HIVE_COL_NAME1, Types.ByteType.get(), "col_1_comment");
    Column col2 = Column.of(HIVE_COL_NAME2, Types.DateType.get(), "col_2_comment");
    Column col3 = Column.of(HIVE_COL_NAME3, Types.StringType.get(), "col_3_comment");
    return new Column[] {col1, col2, col3};
  }

  @Override
  protected String testUserName() {
    return testUserName;
  }

  @Override
  protected void checkTableAllPrivilegesExceptForCreating() {
    // - a. Succeed to insert data into the table
    sparkSession.sql(SQL_INSERT_TABLE);

    // - b. Succeed to select data from the table
    sparkSession.sql(SQL_SELECT_TABLE).collectAsList();

    // - c: Fail to update data in the table. Because Hive doesn't support
    Assertions.assertThrows(
        SparkUnsupportedOperationException.class, () -> sparkSession.sql(SQL_UPDATE_TABLE));

    // - d: Fail to delete data from the table, Because Hive doesn't support
    Assertions.assertThrows(AnalysisException.class, () -> sparkSession.sql(SQL_DELETE_TABLE));

    // - e: Succeed to alter the table
    sparkSession.sql(SQL_ALTER_TABLE);

    // - f: Succeed to drop the table
    sparkSession.sql(SQL_DROP_TABLE);
  }

  @Override
  protected void checkUpdateSQLWithSelectModifyPrivileges() {
    Assertions.assertThrows(
        SparkUnsupportedOperationException.class, () -> sparkSession.sql(SQL_UPDATE_TABLE));
  }

  @Override
  protected void checkUpdateSQLWithSelectPrivileges() {
    Assertions.assertThrows(
        SparkUnsupportedOperationException.class, () -> sparkSession.sql(SQL_UPDATE_TABLE));
  }

  @Override
  protected void checkUpdateSQLWithModifyPrivileges() {
    Assertions.assertThrows(
        SparkUnsupportedOperationException.class, () -> sparkSession.sql(SQL_UPDATE_TABLE));
  }

  @Override
  protected void checkDeleteSQLWithSelectModifyPrivileges() {
    Assertions.assertThrows(AnalysisException.class, () -> sparkSession.sql(SQL_DELETE_TABLE));
  }

  @Override
  protected void checkDeleteSQLWithSelectPrivileges() {
    Assertions.assertThrows(AnalysisException.class, () -> sparkSession.sql(SQL_DELETE_TABLE));
  }

  @Override
  protected void checkDeleteSQLWithModifyPrivileges() {
    Assertions.assertThrows(AnalysisException.class, () -> sparkSession.sql(SQL_DELETE_TABLE));
  }

  @Override
  protected void reset() {
    cleanSchemaPath();
    initSparkSession();
  }

  @Override
  protected void checkWithoutPrivileges() {
    // Ignore this, because if we create a schema, maybe we have nearly
    // whole privileges of this directory.
  }

  @Override
  protected void testAlterTable() {
    // We have added extra test case to test alter, ignore this.
  }

  @Override
  protected void testDropTable() {
    Assertions.assertDoesNotThrow(() -> sparkSession.sql(SQL_DROP_TABLE));
  }

  private void cleanSchemaPath() {
    BaseIT.runInEnv(
        "HADOOP_USER_NAME",
        "gravitino",
        () -> {
          Configuration conf = new Configuration();
          conf.set("fs.defaultFS", DEFAULT_FS);
          conf.set("fs.hdfs.impl.disable.cache", "false");
          try (FileSystem userFileSystem = FileSystem.get(conf)) {
            String schemaPath = String.format("%s/%s.db", DEFAULT_FS, schemaName);
            userFileSystem.delete(new Path(schemaPath), true);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }

  private void initSparkSession() {
    BaseIT.runInEnv(
        "HADOOP_USER_NAME",
        testUserName,
        () -> {
          sparkSession =
              SparkSession.builder()
                  .master("local[1]")
                  .appName("Ranger Hive E2E integration test")
                  .config("hive.metastore.uris", HIVE_METASTORE_URIS)
                  .config(
                      "spark.sql.warehouse.dir",
                      String.format(
                          "hdfs://%s:%d/user/hive/warehouse",
                          containerSuite.getHiveRangerContainer().getContainerIpAddress(),
                          HiveContainer.HDFS_DEFAULTFS_PORT))
                  .config("spark.sql.storeAssignmentPolicy", "LEGACY")
                  .config("mapreduce.input.fileinputformat.input.dir.recursive", "true")
                  .enableHiveSupport()
                  .getOrCreate();
          sparkSession.sql(SQL_SHOW_DATABASES); // must be called to activate the Spark session
        });
  }

  protected TestCreateSchemaChecker getTestCreateSchemaChecker() {
    return new TestCreateSchemaChecker() {
      void checkCreateTable() {
        // If the schema path is created by the user, the user can have the privilege
        // to create the table. So we ignore the check
      }
    };
  }

  protected TestCreateTableChecker getTestCreateTableChecker() {
    return new TestCreateTableChecker() {
      @Override
      void checkTableReadWrite() {
        // If the table path is created by the user, the user can have the privilege
        // to read and write the table. So we ignore the check
      }

      void checkCreateTable() {
        // If the schema path is created by the user, the user can have the privilege
        // to create the table. So we ignore the check
      }
    };
  }

  protected TestDeleteAndRecreateRoleChecker getTestDeleteAndRecreateRoleChecker() {
    return new TestDeleteAndRecreateRoleChecker() {
      @Override
      void checkDropSchema() {
        Assertions.assertThrows(
            NoSuchNamespaceException.class, () -> sparkSession.sql(SQL_DROP_SCHEMA));
      }

      @Override
      void checkCreateSchema() {
        Assertions.assertThrows(AnalysisException.class, () -> sparkSession.sql(SQL_CREATE_SCHEMA));
      }
    };
  }

  @Override
  TestSelectOnlyTableChecker getTestSelectOnlyTableChecker() {
    return new TestSelectOnlyTableChecker() {
      @Override
      void checkUpdateSQL() {
        // If the schema path is created by the user, the user can have the privilege
        // to update the table. So we ignore the check
      }

      @Override
      void checkDeleteSQL() {
        // If the schema path is created by the user, the user can have the privilege
        // to delete the table. So we ignore the check
      }

      @Override
      void checkNoPrivSQL() {
        // If the schema path is created by the user, the user can have the privilege
        // to do any operation about the table. So we ignore the check
      }

      @Override
      void checkInsertSQL() {
        // If the schema path is created by the user, the user can have the privilege
        // to insert the table. So we ignore the check
      }

      @Override
      void checkDropSQL() {
        // If the schema path is created by the user, the user can have the privilege
        // to drop the table. So we ignore the check
      }

      @Override
      void checkAlterSQL() {
        // If the schema path is created by the user, the user can have the privilege
        // to alter the table. So we ignore the check
      }
    };
  }

  @Override
  TestDeleteAndRecreateMetadataObject getTestDeleteAndRecreateMetadataObject() {
    return new TestDeleteAndRecreateMetadataObject() {
      @Override
      void checkDropSchema() {
        // If the schema path is created by the user, the user can have the privilege
        // to alter the table. So we ignore the check
      }

      @Override
      void checkRecreateSchema() {
        Assertions.assertThrows(AnalysisException.class, () -> sparkSession.sql(SQL_CREATE_SCHEMA));
      }
    };
  }

  @Override
  protected TestAllowUseSchemaPrivilegeChecker getTestAllowUseSchemaPrivilegeChecker() {
    return new TestAllowUseSchemaPrivilegeChecker() {
      @Override
      void checkShowSchemas() {
        // Use Spark to show this databases(schema)
        Dataset dataset1 = sparkSession.sql(SQL_SHOW_DATABASES);
        dataset1.show();
        List<Row> rows1 = dataset1.collectAsList();
        Assertions.assertEquals(
            1, rows1.stream().filter(row -> row.getString(0).equals(schemaName)).count());
      }

      @Override
      void checkShowSchemasAgain() {
        // Use Spark to show this databases(schema)
        Dataset dataset1 = sparkSession.sql(SQL_SHOW_DATABASES);
        dataset1.show();
        List<Row> rows1 = dataset1.collectAsList();
        Assertions.assertEquals(
            1, rows1.stream().filter(row -> row.getString(0).equals(schemaName)).count());
      }
    };
  }

  @Override
  TestGrantPrivilegesForMetalakeChecker getTestGrantPrivilegesForMetalakeChecker() {
    return new TestGrantPrivilegesForMetalakeChecker() {
      @Override
      void checkCreateSchema() {
        Assertions.assertThrows(AnalysisException.class, () -> sparkSession.sql(SQL_CREATE_SCHEMA));
      }
    };
  }

  @Override
  TestChangeOwnerChecker getTestChangeOwnerChecker() {
    return new TestChangeOwnerChecker() {
      @Override
      void checkCreateTable() {
        //  If the schema path is created by the user, the user can have the privilege
        //  to create the table. So we ignore the check
      }

      @Override
      void checkCreateSchema() {
        reset();
        Assertions.assertThrows(AnalysisException.class, () -> sparkSession.sql(SQL_CREATE_SCHEMA));
      }
    };
  }
}
