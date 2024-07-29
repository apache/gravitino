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
package org.apache.gravitino.integration.test.authorization.ranger;

import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.container.RangerContainer;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class RangerHiveIT extends RangerIT {
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private static Connection adminConnection;
  private static Connection anonymousConnection;
  private static final String adminUser = "gravitino";
  private static final String anonymousUser = "anonymous";

  @BeforeAll
  public static void setup() {
    RangerIT.setup();

    containerSuite.startHiveRangerContainer(
        new HashMap<>(
            ImmutableMap.of(
                HiveContainer.HIVE_RUNTIME_VERSION,
                HiveContainer.HIVE3,
                RangerContainer.DOCKER_ENV_RANGER_SERVER_URL,
                String.format(
                    "http://%s:%d",
                    containerSuite.getRangerContainer().getContainerIpAddress(),
                    RangerContainer.RANGER_SERVER_PORT),
                RangerContainer.DOCKER_ENV_RANGER_HIVE_REPOSITORY_NAME,
                RangerIT.RANGER_HIVE_REPO_NAME,
                RangerContainer.DOCKER_ENV_RANGER_HDFS_REPOSITORY_NAME,
                RangerIT.RANGER_HDFS_REPO_NAME,
                HiveContainer.HADOOP_USER_NAME,
                adminUser)));

    createRangerHdfsRepository(
        containerSuite.getHiveRangerContainer().getContainerIpAddress(), true);
    createRangerHiveRepository(
        containerSuite.getHiveRangerContainer().getContainerIpAddress(), true);
    allowAnyoneAccessHDFS();
    allowAnyoneAccessInformationSchema();

    // Create hive connection
    String url =
        String.format(
            "jdbc:hive2://%s:%d/default",
            containerSuite.getHiveRangerContainer().getContainerIpAddress(),
            HiveContainer.HIVE_SERVICE_PORT);
    try {
      Class.forName("org.apache.hive.jdbc.HiveDriver");
      adminConnection = DriverManager.getConnection(url, adminUser, "");
      anonymousConnection = DriverManager.getConnection(url, anonymousUser, "");
    } catch (ClassNotFoundException | SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /** Currently we only test Ranger Hive, So wo Allow anyone to visit HDFS */
  static void allowAnyoneAccessHDFS() {
    Map<String, RangerPolicy.RangerPolicyResource> policyResourceMap =
        ImmutableMap.of(RangerDefines.RESOURCE_PATH, new RangerPolicy.RangerPolicyResource("/*"));
    RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
    policyItem.setUsers(Arrays.asList(RangerDefines.CURRENT_USER));
    policyItem.setAccesses(
        Arrays.asList(
            new RangerPolicy.RangerPolicyItemAccess(RangerDefines.ACCESS_TYPE_HDFS_READ),
            new RangerPolicy.RangerPolicyItemAccess(RangerDefines.ACCESS_TYPE_HDFS_WRITE),
            new RangerPolicy.RangerPolicyItemAccess(RangerDefines.ACCESS_TYPE_HDFS_EXECUTE)));
    updateOrCreateRangerPolicy(
        RangerDefines.SERVICE_TYPE_HDFS,
        RANGER_HDFS_REPO_NAME,
        "allowAnyoneAccessHDFS",
        policyResourceMap,
        Collections.singletonList(policyItem));
  }

  /**
   * Hive must have this policy Allow anyone can access information schema to show `database`,
   * `tables` and `columns`
   */
  static void allowAnyoneAccessInformationSchema() {
    Map<String, RangerPolicy.RangerPolicyResource> policyResourceMap =
        ImmutableMap.of(
            RangerDefines.RESOURCE_DATABASE,
            new RangerPolicy.RangerPolicyResource("information_schema"),
            RangerDefines.RESOURCE_TABLE,
            new RangerPolicy.RangerPolicyResource("*"),
            RangerDefines.RESOURCE_COLUMN,
            new RangerPolicy.RangerPolicyResource("*"));
    RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
    policyItem.setGroups(Arrays.asList(RangerDefines.PUBLIC_GROUP));
    policyItem.setAccesses(
        Arrays.asList(
            new RangerPolicy.RangerPolicyItemAccess(RangerDefines.ACCESS_TYPE_HIVE_SELECT)));
    updateOrCreateRangerPolicy(
        RangerDefines.SERVICE_TYPE_HIVE,
        RANGER_HIVE_REPO_NAME,
        "allowAnyoneAccessInformationSchema",
        policyResourceMap,
        Collections.singletonList(policyItem));
  }

  @Test
  public void testCreateDatabase() throws Exception {
    String dbName = "db1";

    // Only allow admin user to operation database `db1`
    // Other users can't see the database `db1`
    Map<String, RangerPolicy.RangerPolicyResource> policyResourceMap =
        ImmutableMap.of(
            RangerDefines.RESOURCE_DATABASE, new RangerPolicy.RangerPolicyResource(dbName));
    RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();
    policyItem.setUsers(Arrays.asList(adminUser));
    policyItem.setAccesses(
        Arrays.asList(new RangerPolicy.RangerPolicyItemAccess(RangerDefines.ACCESS_TYPE_HIVE_ALL)));
    updateOrCreateRangerPolicy(
        RangerDefines.SERVICE_TYPE_HIVE,
        RANGER_HIVE_REPO_NAME,
        "testAllowShowDatabase",
        policyResourceMap,
        Collections.singletonList(policyItem));

    Statement adminStmt = adminConnection.createStatement();
    adminStmt.execute(String.format("CREATE DATABASE %s", dbName));
    String sql = "show databases";
    ResultSet adminRS = adminStmt.executeQuery(sql);
    List<String> adminDbs = new ArrayList<>();
    while (adminRS.next()) {
      adminDbs.add(adminRS.getString(1));
    }
    Assertions.assertTrue(adminDbs.contains(dbName));

    // Anonymous user can't see the database `db1`
    Statement anonymousStmt = anonymousConnection.createStatement();
    ResultSet anonymousRS = anonymousStmt.executeQuery(sql);
    List<String> anonymousDbs = new ArrayList<>();
    while (anonymousRS.next()) {
      anonymousDbs.add(anonymousRS.getString(1));
    }
    Assertions.assertFalse(anonymousDbs.contains(dbName));

    // Allow anonymous user to see the database `db1`
    policyItem.setUsers(Arrays.asList(adminUser, anonymousUser));
    policyItem.setAccesses(
        Arrays.asList(new RangerPolicy.RangerPolicyItemAccess(RangerDefines.ACCESS_TYPE_HIVE_ALL)));
    updateOrCreateRangerPolicy(
        RangerDefines.SERVICE_TYPE_HIVE,
        RANGER_HIVE_REPO_NAME,
        "testAllowShowDatabase",
        policyResourceMap,
        Collections.singletonList(policyItem));
    anonymousRS = anonymousStmt.executeQuery(sql);
    anonymousDbs.clear();
    while (anonymousRS.next()) {
      anonymousDbs.add(anonymousRS.getString(1));
    }
    Assertions.assertTrue(anonymousDbs.contains(dbName));
  }
}
