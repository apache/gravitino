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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.ranger.RangerPrivilege.RangerHivePrivilege;
import org.apache.gravitino.authorization.ranger.reference.RangerDefines.PolicyResource;

public class RangerAuthorizationHivePlugin extends RangerAuthorizationPlugin {
  public RangerAuthorizationHivePlugin(Map<String, String> config) {
    super(config);
  }

  /** Initialize the default mapping Gravitino privilege name to the Ranger privileges */
  @Override
  public void initializePrivilegesMappingConfig() {
    privilegesMapping.put(
        Privilege.Name.CREATE_SCHEMA, ImmutableSet.of(RangerHivePrivilege.CREATE));
    privilegesMapping.put(Privilege.Name.CREATE_TABLE, ImmutableSet.of(RangerHivePrivilege.CREATE));
    privilegesMapping.put(
        Privilege.Name.MODIFY_TABLE,
        ImmutableSet.of(
            RangerHivePrivilege.UPDATE, RangerHivePrivilege.ALTER, RangerHivePrivilege.WRITE));
    privilegesMapping.put(
        Privilege.Name.SELECT_TABLE,
        ImmutableSet.of(RangerHivePrivilege.READ, RangerHivePrivilege.SELECT));
  }

  /** Initialize the default owner privileges. */
  @Override
  public void initializeOwnerPrivilegesConfig() {
    ownerPrivileges.add(RangerHivePrivilege.ALL);
  }

  /** Initial Ranger policy resource defines. */
  @Override
  public void initializePolicyResourceDefinesConfig() {
    policyResourceDefines =
        ImmutableList.of(
            PolicyResource.DATABASE.toString(),
            PolicyResource.TABLE.toString(),
            PolicyResource.COLUMN.toString());
  }
}
