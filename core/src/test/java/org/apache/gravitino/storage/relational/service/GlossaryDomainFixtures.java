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

package org.apache.gravitino.storage.relational.service;

import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.utils.NamespaceUtil;

/** Inserts domains for glossary tests without backing tags. */
public final class GlossaryDomainFixtures {
  private GlossaryDomainFixtures() {}

  /**
   * Inserts a domain, attaching it under the parent when one is given.
   *
   * @return The domain ID.
   */
  public static long insertDomain(
      String metalake, String name, Domain.Status status, @Nullable String parent, AuditInfo audit)
      throws Exception {
    DomainEntity domain =
        DomainEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(name)
            .withNamespace(NamespaceUtil.ofDomain(metalake))
            .withStatus(status)
            .withAuditInfo(audit)
            .build();
    DomainMetaService.getInstance().insertDomain(domain, false);
    if (parent != null) {
      DomainRelService.getInstance()
          .insertDomainRel(
              NameIdentifier.of(NamespaceUtil.ofDomain(metalake), name),
              NameIdentifier.of(NamespaceUtil.ofDomain(metalake), parent),
              audit);
    }
    return domain.id();
  }
}
