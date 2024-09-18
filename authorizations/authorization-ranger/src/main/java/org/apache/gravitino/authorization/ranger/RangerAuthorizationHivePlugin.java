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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.ranger.RangerPrivilege.RangerHivePrivilege;
import org.apache.gravitino.authorization.ranger.reference.RangerDefines.PolicyResource;

public class RangerAuthorizationHivePlugin extends RangerAuthorizationPlugin {
  public RangerAuthorizationHivePlugin(Map<String, String> config) {
    super(config);
  }

  /** Set the default mapping Gravitino privilege name to the Ranger rule */
  public Map<Privilege.Name, Set<RangerPrivilege>> privilegesMappingRule() {
    return ImmutableMap.of(
        Privilege.Name.CREATE_SCHEMA,
        ImmutableSet.of(RangerHivePrivilege.CREATE),
        Privilege.Name.CREATE_TABLE,
        ImmutableSet.of(RangerHivePrivilege.CREATE),
        Privilege.Name.MODIFY_TABLE,
        ImmutableSet.of(
            RangerHivePrivilege.UPDATE, RangerHivePrivilege.ALTER, RangerHivePrivilege.WRITE),
        Privilege.Name.SELECT_TABLE,
        ImmutableSet.of(RangerHivePrivilege.READ, RangerHivePrivilege.SELECT));
  }

  /** Set the default owner rule. */
  public Set<RangerPrivilege> ownerMappingRule() {
    return ImmutableSet.of(RangerHivePrivilege.ALL);
  }

  /** Set Ranger policy resource rule. */
  public List<String> policyResourceDefinesRule() {
    return ImmutableList.of(
        PolicyResource.DATABASE.getName(),
        PolicyResource.TABLE.getName(),
        PolicyResource.COLUMN.getName());
  }
}
