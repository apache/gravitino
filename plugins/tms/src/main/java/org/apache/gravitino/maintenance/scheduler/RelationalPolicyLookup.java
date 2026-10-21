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
package org.apache.gravitino.maintenance.scheduler;

import java.util.Optional;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.PolicyMetaMapper;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.gravitino.storage.relational.po.PolicyPO;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NamespaceUtil;

/** Loads policies through the relational entity-store mappers. */
public final class RelationalPolicyLookup implements PolicyLookup {

  @Override
  public Optional<PolicyEntity> findById(long policyId) {
    PolicyPO policyPO =
        SessionUtils.getWithoutCommit(
            PolicyMetaMapper.class, mapper -> mapper.selectPolicyByPolicyId(policyId));
    if (policyPO == null) {
      return Optional.empty();
    }
    MetalakePO metalakePO =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class,
            mapper -> mapper.selectMetalakeMetaById(policyPO.getMetalakeId()));
    if (metalakePO == null) {
      return Optional.empty();
    }
    Namespace namespace = NamespaceUtil.ofPolicy(metalakePO.getMetalakeName());
    return Optional.of(POConverters.fromPolicyPO(policyPO, namespace));
  }
}
