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
package org.apache.gravitino.authorization.ranger;

import static org.apache.gravitino.authorization.Privilege.Name.CREATE_SCHEMA;
import static org.apache.gravitino.authorization.Privilege.Name.CREATE_TABLE;
import static org.apache.gravitino.authorization.Privilege.Name.MODIFY_TABLE;
import static org.apache.gravitino.authorization.Privilege.Name.SELECT_TABLE;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;

public class AuthorizationConfigTest {
  @Test
  public void testNonstandardHiveProperties() {
    AuthorizationConfig authorizationConfig = new AuthorizationConfig();
    String propertyFilePath = "/authorization-defs/authorization-hive-nonstandard.properties";
    URL resourceUrl = AuthorizationConfigTest.class.getResource(propertyFilePath);
    try {
      if (resourceUrl != null) {
        Properties properties =
            authorizationConfig.loadPropertiesFromFile(new File(resourceUrl.getPath()));
        authorizationConfig.loadFromProperties(properties);
      }
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Failed to load authorization config from resource " + resourceUrl, e);
    }

    RangerHelper rangerHelper = new RangerHelper(authorizationConfig);
    Set<String> ownerPrivileges = rangerHelper.getOwnerPrivileges();
    Set<String> createSchemaPrivileges = rangerHelper.translatePrivilege(CREATE_SCHEMA);
    Set<String> createTablePrivileges = rangerHelper.translatePrivilege(CREATE_TABLE);
    Set<String> selectTablePrivileges = rangerHelper.translatePrivilege(SELECT_TABLE);
    Set<String> modifyTablePrivileges = rangerHelper.translatePrivilege(MODIFY_TABLE);

    Assertions.assertEquals(
        ownerPrivileges, ImmutableSet.of(RangerPrivilege.RangerHivePrivilege.ALL.toString()));
    Assertions.assertEquals(
        createSchemaPrivileges,
        ImmutableSet.of(RangerPrivilege.RangerHivePrivilege.CREATE.toString()));
    Assertions.assertEquals(
        createTablePrivileges,
        ImmutableSet.of(RangerPrivilege.RangerHivePrivilege.CREATE.toString()));
    Assertions.assertEquals(
        selectTablePrivileges,
        ImmutableSet.of(
            RangerPrivilege.RangerHivePrivilege.READ.toString(),
            RangerPrivilege.RangerHivePrivilege.SELECT.toString()));
    Assertions.assertEquals(
        modifyTablePrivileges,
        ImmutableSet.of(
            RangerPrivilege.RangerHivePrivilege.UPDATE.toString(),
            RangerPrivilege.RangerHivePrivilege.ALTER.toString(),
            RangerPrivilege.RangerHivePrivilege.WRITE.toString()));
  }
}
