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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.authorization.Privilege;

/**
 * Ranger authorization use this configuration to mapping Gravitino privilege to the Ranger
 * privileges. We can use this configuration to support the different Ranger authorization
 * components, such as Hive, HDFS, HBase, etc.
 */
public abstract class RangerAuthorizationConfig {
  /** Mapping Gravitino privilege name to the Ranger privileges configuration. */
  protected Map<Privilege.Name, Set<RangerPrivilege>> privilegesMapping = new HashMap<>();
  /** The owner privileges, the owner can do anything on the metadata object configuration */
  protected Set<RangerPrivilege> ownerPrivileges = new HashSet<>();
  /**
   * Because Ranger doesn't support the precise search, Ranger will return the policy meets the
   * wildcard(*,?) conditions, If you use `db.table` condition to search policy, the Ranger will
   * match `db1.table1`, `db1.table2`, `db*.table*`, So we need to manually precisely filter this
   * research results. <br>
   * policyResourceDefines: The Ranger policy resource defines configuration. <br>
   */
  protected List<String> policyResourceDefines;

  /** Initialize the mapping Gravitino privilege name to the Ranger privileges configuration */
  abstract void initializePrivilegesMappingConfig();

  /** Initialize the owner privileges configuration. */
  abstract void initializeOwnerPrivilegesConfig();

  /** Initialize the policy resource defines configuration. */
  abstract List<String> initializePolicyResourceDefinesConfig();

  public final Map<Privilege.Name, Set<RangerPrivilege>> getPrivilegesMapping() {
    return privilegesMapping;
  }

  public final Set<RangerPrivilege> getOwnerPrivileges() {
    return ownerPrivileges;
  }

  public final List<String> getPolicyResourceDefines() {
    return policyResourceDefines;
  }
}
