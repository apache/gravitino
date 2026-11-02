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
import java.util.concurrent.CompletableFuture;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.meta.AuditInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests scoped audit information for entity imports. */
public class TestEntityImportAuditContext {

  private static final NameIdentifier IDENTIFIER =
      NameIdentifier.of("metalake", "catalog", "schema");

  /** Ordinary imports cannot claim the current reader as the creator. */
  @Test
  public void testOrdinaryImportHasNoFallback() {
    Assertions.assertNull(
        EntityImportAuditContext.auditInfo(IDENTIFIER, AuditInfo.EMPTY).creator());
    Assertions.assertNull(EntityImportAuditContext.auditInfo(IDENTIFIER, null).createTime());
  }

  /** Scoped imports fill missing creators and preserve modification information. */
  @Test
  public void testFallbackPreservesCatalogAudit() {
    AuditInfo modified =
        AuditInfo.builder().withLastModifier("bob").withLastModifiedTime(Instant.EPOCH).build();
    EntityImportAuditContext.withCreator(
        IDENTIFIER,
        "alice",
        () -> {
          AuditInfo audit = EntityImportAuditContext.auditInfo(IDENTIFIER, modified);
          Assertions.assertEquals("alice", audit.creator());
          Assertions.assertNotNull(audit.createTime());
          Assertions.assertEquals("bob", audit.lastModifier());
          Assertions.assertEquals(Instant.EPOCH, audit.lastModifiedTime());
          Assertions.assertEquals(
              audit.createTime(),
              EntityImportAuditContext.auditInfo(IDENTIFIER, null).createTime());

          AuditInfo reported =
              AuditInfo.builder().withCreator("carol").withCreateTime(Instant.EPOCH).build();
          AuditInfo preserved = EntityImportAuditContext.auditInfo(IDENTIFIER, reported);
          Assertions.assertEquals("carol", preserved.creator());
          Assertions.assertEquals(Instant.EPOCH, preserved.createTime());

          AuditInfo timeOnly = AuditInfo.builder().withCreateTime(Instant.EPOCH).build();
          Assertions.assertEquals(
              Instant.EPOCH, EntityImportAuditContext.auditInfo(IDENTIFIER, timeOnly).createTime());
        });
    Assertions.assertNull(
        EntityImportAuditContext.auditInfo(IDENTIFIER, AuditInfo.EMPTY).creator());
  }

  /** Other identifiers and threads cannot inherit a scoped creator. */
  @Test
  public void testScopeIsLimitedToIdentifierAndThread() {
    EntityImportAuditContext.withCreator(
        IDENTIFIER,
        "alice",
        () -> {
          NameIdentifier other = NameIdentifier.of("metalake", "catalog", "other");
          Assertions.assertNull(
              EntityImportAuditContext.auditInfo(other, AuditInfo.EMPTY).creator());
          Assertions.assertNull(
              CompletableFuture.supplyAsync(
                      () ->
                          EntityImportAuditContext.auditInfo(IDENTIFIER, AuditInfo.EMPTY).creator())
                  .join());
        });
  }

  /** Failed imports restore nested scopes and leave no state on a reused request thread. */
  @Test
  public void testScopeRestoredAfterFailure() {
    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            EntityImportAuditContext.withCreator(
                IDENTIFIER,
                "alice",
                () -> {
                  Assertions.assertThrows(
                      IllegalArgumentException.class,
                      () ->
                          EntityImportAuditContext.withCreator(
                              IDENTIFIER,
                              "bob",
                              () -> {
                                Assertions.assertEquals(
                                    "bob",
                                    EntityImportAuditContext.auditInfo(IDENTIFIER, null).creator());
                                throw new IllegalArgumentException("nested import failed");
                              }));
                  Assertions.assertEquals(
                      "alice", EntityImportAuditContext.auditInfo(IDENTIFIER, null).creator());
                  throw new IllegalStateException("import failed");
                }));
    Assertions.assertNull(EntityImportAuditContext.auditInfo(IDENTIFIER, null).creator());
  }
}
