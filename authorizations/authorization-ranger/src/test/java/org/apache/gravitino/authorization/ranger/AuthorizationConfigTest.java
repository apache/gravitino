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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import org.apache.gravitino.authorization.ranger.RangerPrivilege.RangerHivePrivilege;
import org.apache.gravitino.connector.AuthorizationPropertiesMeta;
import org.apache.gravitino.integration.test.container.RangerContainer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AuthorizationConfigTest {
  @Test
  public void testNonstandardHiveProperties() {
    RangerAuthorizationHivePlugin rangerAuthTestPlugin =
        new RangerAuthorizationHivePlugin(
            ImmutableMap.of(
                AuthorizationPropertiesMeta.RANGER_ADMIN_URL,
                "http://localhost:6080",
                AuthorizationPropertiesMeta.RANGER_AUTH_TYPE,
                RangerContainer.authType,
                AuthorizationPropertiesMeta.RANGER_USERNAME,
                RangerContainer.rangerUserName,
                AuthorizationPropertiesMeta.RANGER_PASSWORD,
                RangerContainer.rangerPassword,
                AuthorizationPropertiesMeta.RANGER_SERVICE_NAME,
                ""));
    Assertions.assertEquals(
        rangerAuthTestPlugin.getOwnerPrivileges(),
        Sets.newHashSet(RangerHivePrivilege.SERVICEADMIN));
    Assertions.assertEquals(
        rangerAuthTestPlugin.getPrivilegesMapping().get(CREATE_SCHEMA),
        Sets.newHashSet(RangerHivePrivilege.SERVICEADMIN));
    Assertions.assertEquals(
        rangerAuthTestPlugin.getPrivilegesMapping().get(CREATE_TABLE),
        Sets.newHashSet(RangerHivePrivilege.SERVICEADMIN));
    Assertions.assertEquals(
        rangerAuthTestPlugin.getPrivilegesMapping().get(SELECT_TABLE),
        Sets.newHashSet(RangerHivePrivilege.SERVICEADMIN));
    Assertions.assertEquals(
        rangerAuthTestPlugin.getPrivilegesMapping().get(MODIFY_TABLE),
        Sets.newHashSet(RangerHivePrivilege.SERVICEADMIN));

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
    rangerAuthTestPlugin.overrideAuthorizationConfig(authorizationConfig);
    Assertions.assertEquals(
        rangerAuthTestPlugin.getOwnerPrivileges(), Sets.newHashSet(RangerHivePrivilege.ALL));
    Assertions.assertEquals(
        rangerAuthTestPlugin.getPrivilegesMapping().get(CREATE_SCHEMA),
        Sets.newHashSet(RangerHivePrivilege.CREATE));
    Assertions.assertEquals(
        rangerAuthTestPlugin.getPrivilegesMapping().get(CREATE_TABLE),
        Sets.newHashSet(RangerHivePrivilege.CREATE));
    Assertions.assertEquals(
        rangerAuthTestPlugin.getPrivilegesMapping().get(SELECT_TABLE),
        Sets.newHashSet(RangerHivePrivilege.READ, RangerHivePrivilege.SELECT));
    Assertions.assertEquals(
        rangerAuthTestPlugin.getPrivilegesMapping().get(MODIFY_TABLE),
        Sets.newHashSet(
            RangerHivePrivilege.UPDATE, RangerHivePrivilege.ALTER, RangerHivePrivilege.WRITE));
  }
}
