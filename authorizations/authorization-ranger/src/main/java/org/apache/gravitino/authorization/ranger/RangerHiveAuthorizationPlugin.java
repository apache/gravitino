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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.authorization.Privilege;

/**
 * RangerHiveAuthorizationPlugin is a plugin for Apache Ranger to manage the Hive authorization of
 * the Apache Gravitino.
 */
public class RangerHiveAuthorizationPlugin extends RangerAuthorizationPlugin {
  public RangerHiveAuthorizationPlugin(String catalogProvider, Map<String, String> config) {
    super(catalogProvider, config);
  }

  /**
   * Ranger hive's privilege have `select`, `update`, `create`, `drop`, `alter`, `index`, `lock`,
   * `read`, `write`, `repladmin`, `serviceadmin`, `refresh` and `all`. Reference:
   * ranger/agents-common/src/main/resources/service-defs/ranger-servicedef-hive.json
   */
  @Override
  protected void initMapPrivileges() {
    mapPrivileges =
        ImmutableMap.<Privilege.Name, Set<String>>builder()
            .put(
                Privilege.Name.CREATE_SCHEMA,
                ImmutableSet.of(RangerDefines.ACCESS_TYPE_HIVE_CREATE))
            .put(
                Privilege.Name.CREATE_TABLE, ImmutableSet.of(RangerDefines.ACCESS_TYPE_HIVE_CREATE))
            .put(
                Privilege.Name.MODIFY_TABLE,
                ImmutableSet.of(
                    RangerDefines.ACCESS_TYPE_HIVE_UPDATE,
                    RangerDefines.ACCESS_TYPE_HIVE_ALTER,
                    RangerDefines.ACCESS_TYPE_HIVE_WRITE))
            .put(
                Privilege.Name.SELECT_TABLE,
                ImmutableSet.of(
                    RangerDefines.ACCESS_TYPE_HIVE_READ, RangerDefines.ACCESS_TYPE_HIVE_SELECT))
            .build();
  }

  @Override
  protected void initOwnerPrivileges() {
    ownerPrivileges = ImmutableSet.of(RangerDefines.ACCESS_TYPE_HIVE_ALL);
  }

  @Override
  protected void initPolicyFilterKeys() {
    policyFilterKeys =
        Arrays.asList(
            RangerDefines.SEARCH_FILTER_DATABASE,
            RangerDefines.SEARCH_FILTER_TABLE,
            RangerDefines.SEARCH_FILTER_COLUMN);
  }

  @Override
  protected void initPreciseFilterKeys() {
    policyPreciseFilterKeys =
        Arrays.asList(
            RangerDefines.RESOURCE_DATABASE,
            RangerDefines.RESOURCE_TABLE,
            RangerDefines.RESOURCE_COLUMN);
  }
}
