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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.authorization.Privilege;

/**
 * Ranger authorization use this provider to mapping Gravitino privilege to the Ranger privileges.
 * We can use this it to support the different Ranger authorization components, such as Hive, HDFS,
 * HBase, etc.
 */
public interface RangerPrivilegesMappingProvider {
  /** Set the mapping Gravitino privilege name to the Ranger privileges rule. */
  Map<Privilege.Name, Set<RangerPrivilege>> privilegesMappingRule();

  /** Set the owner privileges rule. */
  Set<RangerPrivilege> ownerMappingRule();

  /** Set the policy resource defines rule. */
  List<String> policyResourceDefinesRule();
}
