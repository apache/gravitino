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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.meta.GlossaryTermEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.GlossaryTermDomainRelMapper;
import org.apache.gravitino.storage.relational.po.DomainPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

/** Exercises glossary term domain membership rows with real backend transactions. */
public class TestGlossaryTermDomainService extends TestJDBCBackend {
  private static final String LAKE = "glossary";
  private static final long LAKE_ID = 100L;
  private static final Namespace NS =
      Namespace.of(LAKE, Entity.SYSTEM_CATALOG_RESERVED_NAME, "glossary_term");
  private final GlossaryTermDomainService service = GlossaryTermDomainService.getInstance();
  private long sales;
  private long finance;

  /** Every live membership row points at a live term and a live domain of the same metalake. */
  @AfterEach
  public void checkInvariants() throws Exception {
    Assertions.assertFalse(SessionUtils.isInTransaction());
    GlossaryInvariants.assertDomainMembership();
  }

  @TestTemplate
  public void testAddListRemove() throws Exception {
    setup();
    run(() -> service.add(LAKE_ID, 101, sales));
    run(() -> service.add(LAKE_ID, 101, finance));
    run(() -> service.add(LAKE_ID, 102, sales));
    Assertions.assertEquals(
        Map.of(101L, List.of("Finance", "Sales"), 102L, List.of("Sales")),
        read(() -> service.domainNames(LAKE_ID, List.of(101L, 102L, 103L))));
    Assertions.assertEquals(
        Set.of(101L, 102L), read(() -> service.termIds(LAKE_ID, Set.of(sales))));
    run(() -> service.remove(101, sales));
    Assertions.assertEquals(Set.of(finance), read(() -> service.domainIds(101)));
  }

  @TestTemplate
  public void testRepeatedRemovalAdvancesPastRetainedGeneration() throws Exception {
    setup();
    long retained = Long.MAX_VALUE - 3;
    run(() -> service.add(LAKE_ID, 101, sales));
    SessionUtils.doWithCommit(
        GlossaryTermDomainRelMapper.class, mapper -> mapper.remove(101, sales, retained));
    for (int i = 1; i <= 2; i++) {
      run(() -> service.add(LAKE_ID, 101, sales));
      run(() -> service.remove(101, sales));
      Assertions.assertEquals(
          retained + i, scalar("SELECT MAX(deleted_at) FROM glossary_term_domain_rel"));
    }
    Assertions.assertTrue(read(() -> service.domainIds(101)).isEmpty());
    Assertions.assertEquals(
        3, scalar("SELECT COUNT(*) FROM glossary_term_domain_rel WHERE deleted_at > 0"));
  }

  @TestTemplate
  public void testTermDeleteAndMetalakeDropRetireRows() throws Exception {
    setup();
    run(() -> service.add(LAKE_ID, 101, finance));
    run(() -> service.add(LAKE_ID, 102, sales));
    Assertions.assertTrue(
        GlossaryTermMetaService.getInstance().delete(NameIdentifier.of(NS, "Term101")));
    Assertions.assertFalse(read(() -> service.hasTerms(finance)));
    Assertions.assertTrue(read(() -> service.hasTerms(sales)));
    run(() -> GlossaryTermMetaService.getInstance().dropMetalake(LAKE_ID));
    Assertions.assertFalse(read(() -> service.hasTerms(sales)));
    Assertions.assertEquals(
        0, scalar("SELECT COUNT(*) FROM glossary_term_domain_rel WHERE deleted_at = 0"));
    GlossaryTermMetaService.getInstance().collect(Long.MAX_VALUE, 1000);
    Assertions.assertEquals(0, scalar("SELECT COUNT(*) FROM glossary_term_domain_rel"));
  }

  @TestTemplate
  public void testLockDomainRejectsMissingAndDeleted() throws Exception {
    setup();
    DomainPO locked =
        read(() -> service.lockDomains(LAKE_ID, Set.of(), Set.of("Sales"), row -> {})).get(sales);
    Assertions.assertEquals(sales, locked.domainId());
    Assertions.assertThrows(
        NoSuchDomainException.class,
        () -> read(() -> service.lockDomains(LAKE_ID, Set.of(), Set.of("NoSuch"), row -> {})));
    Assertions.assertThrows(
        NoSuchDomainException.class,
        () -> read(() -> service.lockDomains(999L, Set.of(), Set.of("Sales"), row -> {})));
    Assertions.assertTrue(
        DomainMetaService.getInstance()
            .deleteDomain(NameIdentifier.of(NamespaceUtil.ofDomain(LAKE), "Finance")));
    Assertions.assertThrows(
        NoSuchDomainException.class,
        () -> read(() -> service.lockDomains(LAKE_ID, Set.of(), Set.of("Finance"), row -> {})));
  }

  @TestTemplate
  public void testFilterTermsAcrossDomainBatches() throws Exception {
    setup();
    run(() -> service.add(LAKE_ID, 102, sales));
    run(() -> service.add(LAKE_ID, 101, sales));
    run(() -> service.add(LAKE_ID, 101, finance));
    Set<Long> scope = new LinkedHashSet<>();
    scope.add(sales);
    for (long id = 1; id <= 500; id++) {
      scope.add(-id);
    }
    scope.add(finance);
    Assertions.assertEquals(
        List.of(101L, 102L), List.copyOf(read(() -> service.termIds(LAKE_ID, scope))));
    Assertions.assertTrue(read(() -> service.termIds(LAKE_ID, Set.of())).isEmpty());
  }

  private void setup() throws Exception {
    backend.insert(createBaseMakeLake(LAKE_ID, LAKE, AUDIT_INFO), false);
    for (long id = 101; id <= 103; id++) {
      backend.insert(
          GlossaryTermEntity.builder()
              .withId(id)
              .withName("Term" + id)
              .withNamespace(NS)
              .withAuditInfo(AUDIT_INFO)
              .build(),
          false);
    }
    sales = domain("Sales");
    finance = domain("Finance");
  }

  private long domain(String name) throws Exception {
    DomainEntity domain =
        DomainEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(name)
            .withNamespace(NamespaceUtil.ofDomain(LAKE))
            .withStatus(Domain.Status.PUBLISHED)
            .withAuditInfo(AUDIT_INFO)
            .build();
    DomainMetaService.getInstance().insertDomain(domain, false);
    return domain.id();
  }

  private static <T> T read(Supplier<T> work) {
    return SessionUtils.doWithCommitAndFetchResult(
        GlossaryTermDomainRelMapper.class, ignored -> work.get());
  }

  private static void run(Runnable work) {
    read(
        () -> {
          work.run();
          return null;
        });
  }

  private long scalar(String sql) throws Exception {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
        Connection connection = session.getConnection();
        Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(sql)) {
      Assertions.assertTrue(result.next());
      return result.getLong(1);
    }
  }
}
