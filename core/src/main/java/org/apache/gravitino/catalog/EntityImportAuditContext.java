/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog;

import java.time.Instant;
import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.meta.AuditInfo;

/**
 * Supplies creator information when a protocol creates an external object and then imports it into
 * Gravitino. The import includes this information in its existing store write, without a separate
 * audit update. Ordinary reads do not supply a creator.
 *
 * <p>The context applies only to the specified identifier on the calling thread. Imports must run
 * synchronously; nested scopes restore their caller's context, including on failure.
 */
public final class EntityImportAuditContext {

  private static final ThreadLocal<EntityImportAuditContext> CURRENT = new ThreadLocal<>();

  private final NameIdentifier identifier;
  private final AuditInfo fallback;

  private EntityImportAuditContext(NameIdentifier identifier, String creator) {
    this.identifier = identifier;
    this.fallback = AuditInfo.builder().withCreator(creator).withCreateTime(Instant.now()).build();
  }

  /**
   * Runs an import with fallback creator information for the newly created object. Catalog-reported
   * creators and already imported entities are preserved.
   *
   * @param identifier the newly created object's identifier
   * @param creator the authenticated creator
   * @param action the synchronous import
   */
  public static void withCreator(NameIdentifier identifier, String creator, Runnable action) {
    EntityImportAuditContext previous = CURRENT.get();
    CURRENT.set(new EntityImportAuditContext(identifier, creator));
    try {
      action.run();
    } finally {
      if (previous == null) {
        CURRENT.remove();
      } else {
        CURRENT.set(previous);
      }
    }
  }

  /**
   * Copies catalog audit information for an entity's initial import, filling a missing creator from
   * the current scope. Existing modification information is preserved.
   *
   * @param identifier the entity being imported
   * @param audit the catalog-reported audit, if any
   * @return audit information to persist in the initial import
   */
  public static AuditInfo auditInfo(NameIdentifier identifier, @Nullable Audit audit) {
    AuditInfo reported =
        AuditInfo.builder()
            .withCreator(audit == null ? null : audit.creator())
            .withCreateTime(audit == null ? null : audit.createTime())
            .withLastModifier(audit == null ? null : audit.lastModifier())
            .withLastModifiedTime(audit == null ? null : audit.lastModifiedTime())
            .build();
    EntityImportAuditContext context = CURRENT.get();
    if (reported.creator() != null || context == null || !context.identifier.equals(identifier)) {
      return reported;
    }
    return AuditInfo.builder()
        .withCreator(context.fallback.creator())
        .withCreateTime(
            reported.createTime() == null ? context.fallback.createTime() : reported.createTime())
        .withLastModifier(reported.lastModifier())
        .withLastModifiedTime(reported.lastModifiedTime())
        .build();
  }
}
