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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.DomainRelMapper;
import org.apache.gravitino.storage.relational.po.DomainRelPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

public class TestDomainRelService extends TestJDBCBackend {

  private static final String METALAKE_NAME = "metalake_for_domain_rel_service_test";

  private final DomainMetaService domainMetaService = DomainMetaService.getInstance();

  private final DomainRelService domainRelService = DomainRelService.getInstance();

  @TestTemplate
  public void testInsertDomainRelAndResolveParent() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity finance = insertDomain(METALAKE_NAME, "Finance");
    DomainEntity trading = insertDomain(METALAKE_NAME, "Trading");
    DomainEntity tax = insertDomain(METALAKE_NAME, "Tax");
    DomainEntity markets = insertDomain(METALAKE_NAME, "Markets");

    Assertions.assertFalse(domainRelService.getParentDomainName(trading.id()).isPresent());
    Assertions.assertTrue(domainRelService.listSubDomains(finance.nameIdentifier()).isEmpty());

    domainRelService.insertDomainRel(
        trading.nameIdentifier(), finance.nameIdentifier(), AUDIT_INFO);
    domainRelService.insertDomainRel(tax.nameIdentifier(), finance.nameIdentifier(), AUDIT_INFO);

    Assertions.assertEquals("Finance", domainRelService.getParentDomainName(trading.id()).get());
    Assertions.assertFalse(domainRelService.getParentDomainName(finance.id()).isPresent());
    Assertions.assertEquals(
        ImmutableMap.of(trading.id(), "Finance", tax.id(), "Finance"),
        domainRelService.batchGetParentDomainNames(
            Lists.newArrayList(finance.id(), trading.id(), tax.id(), markets.id())));
    Assertions.assertTrue(
        domainRelService.batchGetParentDomainNames(Lists.newArrayList()).isEmpty());

    DomainEntity tradingWithParent = trading.toBuilder().withParent("Finance").build();
    DomainEntity taxWithParent = tax.toBuilder().withParent("Finance").build();
    List<DomainEntity> subDomains = domainRelService.listSubDomains(finance.nameIdentifier());
    Assertions.assertEquals(2, subDomains.size());
    Assertions.assertTrue(subDomains.contains(tradingWithParent));
    Assertions.assertTrue(subDomains.contains(taxWithParent));
    Assertions.assertTrue(domainRelService.listSubDomains(trading.nameIdentifier()).isEmpty());

    DomainRelPO domainRelPO = selectDomainRel(trading.id());
    Assertions.assertEquals(finance.id(), domainRelPO.parentId());
    Assertions.assertEquals(selectMetalakeId(trading), domainRelPO.metalakeId());
    Assertions.assertEquals(1L, domainRelPO.currentVersion());
    Assertions.assertEquals(
        AUDIT_INFO, JsonUtils.anyFieldMapper().readValue(domainRelPO.auditInfo(), AuditInfo.class));
  }

  @TestTemplate
  public void testDomainReadsFillParent() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity finance = insertDomain(METALAKE_NAME, "Finance");
    DomainEntity trading = insertDomain(METALAKE_NAME, "Trading");
    domainRelService.insertDomainRel(
        trading.nameIdentifier(), finance.nameIdentifier(), AUDIT_INFO);
    DomainEntity tradingWithParent = trading.toBuilder().withParent("Finance").build();

    Assertions.assertEquals(
        tradingWithParent, domainMetaService.getDomainByIdentifier(trading.nameIdentifier()));
    Assertions.assertEquals(
        tradingWithParent, backend.get(trading.nameIdentifier(), Entity.EntityType.DOMAIN));
    Assertions.assertEquals(
        finance, domainMetaService.getDomainByIdentifier(finance.nameIdentifier()));

    List<DomainEntity> domains =
        domainMetaService.listDomainsByNamespace(NamespaceUtil.ofDomain(METALAKE_NAME));
    Assertions.assertEquals(2, domains.size());
    Assertions.assertTrue(domains.contains(finance));
    Assertions.assertTrue(domains.contains(tradingWithParent));
    Assertions.assertEquals(
        Lists.newArrayList(tradingWithParent),
        domainMetaService.batchGetDomainByIdentifier(Lists.newArrayList(trading.nameIdentifier())));

    // An update keeps the parent, which only the relation changes.
    DomainEntity updated =
        domainMetaService.updateDomain(
            trading.nameIdentifier(),
            entity -> ((DomainEntity) entity).toBuilder().withComment("updated").build());
    Assertions.assertEquals("Finance", updated.parent().orElse(null));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            domainMetaService.updateDomain(
                trading.nameIdentifier(),
                entity -> ((DomainEntity) entity).toBuilder().withParent(null).build()));
    Assertions.assertEquals(
        "Finance",
        domainMetaService.getDomainByIdentifier(trading.nameIdentifier()).parent().orElse(null));

    // The edge refers to the parent by ID, so a renamed parent is resolved by its new name.
    domainMetaService.updateDomain(
        finance.nameIdentifier(),
        entity -> ((DomainEntity) entity).toBuilder().withName("Finance/Global").build());
    Assertions.assertEquals(
        "Finance/Global",
        domainMetaService.getDomainByIdentifier(trading.nameIdentifier()).parent().orElse(null));
  }

  @TestTemplate
  public void testInsertDomainRelIsRejected() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    String anotherMetalakeName = METALAKE_NAME + "_another";
    createAndInsertMakeLake(anotherMetalakeName);
    DomainEntity finance = insertDomain(METALAKE_NAME, "Finance");
    DomainEntity trading = insertDomain(METALAKE_NAME, "Trading");
    DomainEntity markets = insertDomain(METALAKE_NAME, "Markets");
    DomainEntity elsewhere = insertDomain(anotherMetalakeName, "Elsewhere");
    NameIdentifier missing = NameIdentifierUtil.ofDomain(METALAKE_NAME, "missing");

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            domainRelService.insertDomainRel(
                finance.nameIdentifier(), finance.nameIdentifier(), AUDIT_INFO));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            domainRelService.insertDomainRel(
                elsewhere.nameIdentifier(), finance.nameIdentifier(), AUDIT_INFO));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> domainRelService.insertDomainRel(missing, finance.nameIdentifier(), AUDIT_INFO));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> domainRelService.insertDomainRel(trading.nameIdentifier(), missing, AUDIT_INFO));
    Assertions.assertThrows(
        NoSuchEntityException.class, () -> domainRelService.listSubDomains(missing));

    domainRelService.insertDomainRel(
        trading.nameIdentifier(), finance.nameIdentifier(), AUDIT_INFO);
    // A domain has at most one parent; moving it is a re-point.
    Assertions.assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            domainRelService.insertDomainRel(
                trading.nameIdentifier(), markets.nameIdentifier(), AUDIT_INFO));
    Assertions.assertEquals(finance.id(), selectDomainRel(trading.id()).parentId());

    // The unique key keeps one active edge per domain even without the check.
    DomainRelPO secondEdge =
        DomainRelPO.initializeDomainRelPO(
            selectMetalakeId(trading), markets.id(), trading.id(), AUDIT_INFO);
    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            SessionUtils.doWithCommit(
                DomainRelMapper.class, mapper -> mapper.insertDomainRel(secondEdge)));
  }

  @TestTemplate
  public void testUpdateDomainRel() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity finance = insertDomain(METALAKE_NAME, "Finance");
    DomainEntity markets = insertDomain(METALAKE_NAME, "Markets");
    DomainEntity trading = insertDomain(METALAKE_NAME, "Trading");

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            domainRelService.updateDomainRel(
                trading.nameIdentifier(), markets.nameIdentifier(), AUDIT_INFO));

    domainRelService.insertDomainRel(
        trading.nameIdentifier(), finance.nameIdentifier(), AUDIT_INFO);
    Instant modifiedTime = Instant.now();
    AuditInfo modifiedAuditInfo =
        AuditInfo.builder().withLastModifier("mover").withLastModifiedTime(modifiedTime).build();
    domainRelService.updateDomainRel(
        trading.nameIdentifier(), markets.nameIdentifier(), modifiedAuditInfo);

    Assertions.assertEquals("Markets", domainRelService.getParentDomainName(trading.id()).get());
    Assertions.assertTrue(domainRelService.listSubDomains(finance.nameIdentifier()).isEmpty());
    Assertions.assertEquals(
        Lists.newArrayList(trading.toBuilder().withParent("Markets").build()),
        domainRelService.listSubDomains(markets.nameIdentifier()));

    DomainRelPO domainRelPO = selectDomainRel(trading.id());
    Assertions.assertEquals(2L, domainRelPO.currentVersion());
    AuditInfo storedAuditInfo =
        JsonUtils.anyFieldMapper().readValue(domainRelPO.auditInfo(), AuditInfo.class);
    Assertions.assertEquals(AUDIT_INFO.creator(), storedAuditInfo.creator());
    Assertions.assertEquals("mover", storedAuditInfo.lastModifier());
    Assertions.assertEquals(modifiedTime, storedAuditInfo.lastModifiedTime());
    Assertions.assertEquals(1, countDomainRelRows(trading.id()));

    // Re-pointing to the current parent changes nothing.
    domainRelService.updateDomainRel(
        trading.nameIdentifier(), markets.nameIdentifier(), modifiedAuditInfo);
    Assertions.assertEquals(2L, selectDomainRel(trading.id()).currentVersion());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            domainRelService.updateDomainRel(
                trading.nameIdentifier(), trading.nameIdentifier(), AUDIT_INFO));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            domainRelService.updateDomainRel(
                trading.nameIdentifier(),
                NameIdentifierUtil.ofDomain(METALAKE_NAME, "missing"),
                AUDIT_INFO));
  }

  @TestTemplate
  public void testDeleteDomainRel() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity finance = insertDomain(METALAKE_NAME, "Finance");
    DomainEntity trading = insertDomain(METALAKE_NAME, "Trading");

    Assertions.assertFalse(domainRelService.deleteDomainRel(trading.nameIdentifier()));
    domainRelService.insertDomainRel(
        trading.nameIdentifier(), finance.nameIdentifier(), AUDIT_INFO);
    Assertions.assertTrue(domainRelService.deleteDomainRel(trading.nameIdentifier()));
    Assertions.assertFalse(domainRelService.deleteDomainRel(trading.nameIdentifier()));
    Assertions.assertFalse(domainRelService.getParentDomainName(trading.id()).isPresent());
    Assertions.assertTrue(domainRelService.listSubDomains(finance.nameIdentifier()).isEmpty());
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            domainRelService.deleteDomainRel(
                NameIdentifierUtil.ofDomain(METALAKE_NAME, "missing")));

    // A detached domain can be attached again.
    domainRelService.insertDomainRel(
        trading.nameIdentifier(), finance.nameIdentifier(), AUDIT_INFO);
    Assertions.assertEquals("Finance", domainRelService.getParentDomainName(trading.id()).get());
  }

  @TestTemplate
  public void testDeleteDomainWithSubDomains() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity finance = insertDomain(METALAKE_NAME, "Finance");
    DomainEntity trading = insertDomain(METALAKE_NAME, "Trading");
    domainRelService.insertDomainRel(
        trading.nameIdentifier(), finance.nameIdentifier(), AUDIT_INFO);

    Assertions.assertThrows(
        NonEmptyEntityException.class,
        () -> domainMetaService.deleteDomain(finance.nameIdentifier()));
    Assertions.assertThrows(
        NonEmptyEntityException.class,
        () -> backend.delete(finance.nameIdentifier(), Entity.EntityType.DOMAIN, true));
    Assertions.assertTrue(backend.exists(finance.nameIdentifier(), Entity.EntityType.DOMAIN));
    Assertions.assertEquals("Finance", domainRelService.getParentDomainName(trading.id()).get());

    // Deleting the sub-domain removes its own edge, after which the parent can be deleted.
    Assertions.assertTrue(domainMetaService.deleteDomain(trading.nameIdentifier()));
    Assertions.assertNull(selectDomainRel(trading.id()));
    Assertions.assertEquals(1, countDomainRelRows(trading.id()));
    Assertions.assertTrue(domainMetaService.deleteDomain(finance.nameIdentifier()));
  }

  @TestTemplate
  public void testDomainRelWithMetalakeDeleteAndGarbageCollection() throws IOException {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);
    String anotherMetalakeName = METALAKE_NAME + "_another";
    createAndInsertMakeLake(anotherMetalakeName);
    DomainEntity finance = insertDomain(METALAKE_NAME, "Finance");
    DomainEntity trading = insertDomain(METALAKE_NAME, "Trading");
    DomainEntity anotherFinance = insertDomain(anotherMetalakeName, "Finance");
    DomainEntity anotherTrading = insertDomain(anotherMetalakeName, "Trading");
    domainRelService.insertDomainRel(
        trading.nameIdentifier(), finance.nameIdentifier(), AUDIT_INFO);
    domainRelService.insertDomainRel(
        anotherTrading.nameIdentifier(), anotherFinance.nameIdentifier(), AUDIT_INFO);

    Assertions.assertTrue(
        MetalakeMetaService.getInstance().deleteMetalake(metalake.nameIdentifier(), true));
    Assertions.assertNull(selectDomainRel(trading.id()));
    Assertions.assertEquals(1, countDomainRelRows(trading.id()));
    Assertions.assertEquals(
        "Finance", domainRelService.getParentDomainName(anotherTrading.id()).get());

    Assertions.assertTrue(
        backend.hardDeleteLegacyData(Entity.EntityType.DOMAIN, Instant.now().toEpochMilli() + 1000)
            >= 3);
    Assertions.assertEquals(0, countDomainRelRows(trading.id()));
    Assertions.assertEquals(1, countDomainRelRows(anotherTrading.id()));
    Assertions.assertEquals(
        "Finance", domainRelService.getParentDomainName(anotherTrading.id()).get());
  }

  @TestTemplate
  public void testDomainRelWithNonCascadeMetalakeDelete() throws IOException {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity finance = insertDomain(METALAKE_NAME, "Finance");
    DomainEntity trading = insertDomain(METALAKE_NAME, "Trading");
    domainRelService.insertDomainRel(
        trading.nameIdentifier(), finance.nameIdentifier(), AUDIT_INFO);

    Assertions.assertTrue(
        MetalakeMetaService.getInstance().deleteMetalake(metalake.nameIdentifier(), false));
    Assertions.assertNull(selectDomainRel(trading.id()));
    Assertions.assertEquals(1, countDomainRelRows(trading.id()));
  }

  @TestTemplate
  public void testHierarchyChangeRejectsCycle() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity finance = insertDomain(METALAKE_NAME, "Finance");
    DomainEntity trading = insertDomain(METALAKE_NAME, "Trading");
    DomainEntity equities = insertDomain(METALAKE_NAME, "Equities");
    DomainEntity markets = insertDomain(METALAKE_NAME, "Markets");
    // Finance <- Trading <- Equities, and Markets on its own.
    insertDomainRel(trading, finance);
    insertDomainRel(equities, trading);

    // Attaching a domain under its own sub-domain, at any depth, is refused.
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> insertDomainRel(finance, equities));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> insertDomainRel(finance, trading));
    Assertions.assertFalse(domainRelService.getParentDomainName(finance.id()).isPresent());

    // So is re-pointing one.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            domainRelService.updateDomainRel(
                trading.nameIdentifier(), equities.nameIdentifier(), AUDIT_INFO));
    Assertions.assertEquals("Finance", domainRelService.getParentDomainName(trading.id()).get());

    // Moves that keep the hierarchy acyclic are allowed.
    insertDomainRel(finance, markets);
    domainRelService.updateDomainRel(
        equities.nameIdentifier(), finance.nameIdentifier(), AUDIT_INFO);
    Assertions.assertEquals("Markets", domainRelService.getParentDomainName(finance.id()).get());
    Assertions.assertEquals("Finance", domainRelService.getParentDomainName(equities.id()).get());
  }

  @TestTemplate
  public void testConcurrentAttachesCannotFormCycle() throws Exception {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity finance = insertDomain(METALAKE_NAME, "Finance");
    DomainEntity trading = insertDomain(METALAKE_NAME, "Trading");
    DomainEntity markets = insertDomain(METALAKE_NAME, "Markets");
    DomainEntity equities = insertDomain(METALAKE_NAME, "Equities");
    // Finance <- Trading and Markets <- Equities. Attaching Finance under Equities and Markets
    // under Trading touch disjoint domains, yet together they would close a cycle.
    insertDomainRel(trading, finance);
    insertDomainRel(equities, markets);

    CountDownLatch firstWritten = new CountDownLatch(1);
    CountDownLatch allowFirstCommit = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<Throwable> firstResult =
        executor.submit(
            () -> {
              try {
                SessionUtils.doMultipleWithCommit(
                    () -> insertDomainRel(finance, equities),
                    () -> {
                      firstWritten.countDown();
                      await(allowFirstCommit);
                    });
                return null;
              } catch (Throwable throwable) {
                return throwable;
              }
            });

    try {
      Assertions.assertTrue(firstWritten.await(30, TimeUnit.SECONDS));
      Future<Throwable> secondResult =
          executor.submit(
              () -> {
                try {
                  insertDomainRel(markets, trading);
                  return null;
                } catch (Throwable throwable) {
                  return throwable;
                }
              });
      // The first change holds the metalake, so the second waits and then sees its edge.
      Assertions.assertThrows(
          TimeoutException.class, () -> secondResult.get(500, TimeUnit.MILLISECONDS));

      allowFirstCommit.countDown();
      Assertions.assertNull(firstResult.get(30, TimeUnit.SECONDS));
      Throwable secondFailure = secondResult.get(30, TimeUnit.SECONDS);
      Assertions.assertTrue(
          secondFailure instanceof IllegalArgumentException, String.valueOf(secondFailure));
    } finally {
      allowFirstCommit.countDown();
      executor.shutdownNow();
    }

    Assertions.assertEquals("Equities", domainRelService.getParentDomainName(finance.id()).get());
    Assertions.assertFalse(domainRelService.getParentDomainName(markets.id()).isPresent());
  }

  @TestTemplate
  public void testDeleteParentWaitsForConcurrentAttach() throws Exception {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity finance = insertDomain(METALAKE_NAME, "Finance");
    DomainEntity trading = insertDomain(METALAKE_NAME, "Trading");

    CountDownLatch attachWritten = new CountDownLatch(1);
    CountDownLatch allowAttachCommit = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<Throwable> attachResult =
        executor.submit(
            () -> {
              try {
                SessionUtils.doMultipleWithCommit(
                    () -> insertDomainRel(trading, finance),
                    () -> {
                      attachWritten.countDown();
                      await(allowAttachCommit);
                    });
                return null;
              } catch (Throwable throwable) {
                return throwable;
              }
            });

    try {
      Assertions.assertTrue(attachWritten.await(30, TimeUnit.SECONDS));
      Future<Throwable> deleteResult =
          executor.submit(
              () -> {
                try {
                  domainMetaService.deleteDomain(finance.nameIdentifier());
                  return null;
                } catch (Throwable throwable) {
                  return throwable;
                }
              });
      // The attach holds the parent row, so the delete waits for it instead of missing the edge.
      Assertions.assertThrows(
          TimeoutException.class, () -> deleteResult.get(500, TimeUnit.MILLISECONDS));

      allowAttachCommit.countDown();
      Assertions.assertNull(attachResult.get(30, TimeUnit.SECONDS));
      Throwable deleteFailure = deleteResult.get(30, TimeUnit.SECONDS);
      Assertions.assertTrue(
          deleteFailure instanceof NonEmptyEntityException, String.valueOf(deleteFailure));
    } finally {
      allowAttachCommit.countDown();
      executor.shutdownNow();
    }

    Assertions.assertTrue(backend.exists(finance.nameIdentifier(), Entity.EntityType.DOMAIN));
    Assertions.assertEquals("Finance", domainRelService.getParentDomainName(trading.id()).get());
  }

  @TestTemplate
  public void testAttachWaitsForConcurrentParentDelete() throws Exception {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity finance = insertDomain(METALAKE_NAME, "Finance");
    DomainEntity trading = insertDomain(METALAKE_NAME, "Trading");

    CountDownLatch deleteWritten = new CountDownLatch(1);
    CountDownLatch allowDeleteCommit = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<Throwable> deleteResult =
        executor.submit(
            () -> {
              try {
                SessionUtils.doMultipleWithCommit(
                    () ->
                        Assertions.assertTrue(
                            domainMetaService.deleteDomain(finance.nameIdentifier())),
                    () -> {
                      deleteWritten.countDown();
                      await(allowDeleteCommit);
                    });
                return null;
              } catch (Throwable throwable) {
                return throwable;
              }
            });

    try {
      Assertions.assertTrue(deleteWritten.await(30, TimeUnit.SECONDS));
      Future<Throwable> attachResult =
          executor.submit(
              () -> {
                try {
                  insertDomainRel(trading, finance);
                  return null;
                } catch (Throwable throwable) {
                  return throwable;
                }
              });
      // The delete holds the parent row, so the attach waits and then sees the parent is gone.
      Assertions.assertThrows(
          TimeoutException.class, () -> attachResult.get(500, TimeUnit.MILLISECONDS));

      allowDeleteCommit.countDown();
      Assertions.assertNull(deleteResult.get(30, TimeUnit.SECONDS));
      Throwable attachFailure = attachResult.get(30, TimeUnit.SECONDS);
      Assertions.assertTrue(
          attachFailure instanceof NoSuchEntityException, String.valueOf(attachFailure));
    } finally {
      allowDeleteCommit.countDown();
      executor.shutdownNow();
    }

    Assertions.assertFalse(backend.exists(finance.nameIdentifier(), Entity.EntityType.DOMAIN));
    Assertions.assertNull(selectDomainRel(trading.id()));
    Assertions.assertEquals(0, countDomainRelRows(trading.id()));
  }

  private DomainEntity insertDomain(String metalake, String name) throws IOException {
    Map<String, String> props = ImmutableMap.of("k1", "v1");
    DomainEntity domain =
        DomainEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(name)
            .withNamespace(NamespaceUtil.ofDomain(metalake))
            .withProperties(props)
            .withStatus(Domain.Status.DRAFT)
            .withAuditInfo(AUDIT_INFO)
            .build();
    domainMetaService.insertDomain(domain, false);
    return domain;
  }

  private void insertDomainRel(DomainEntity child, DomainEntity parent) {
    try {
      domainRelService.insertDomainRel(child.nameIdentifier(), parent.nameIdentifier(), AUDIT_INFO);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      Assertions.assertTrue(latch.await(30, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private DomainRelPO selectDomainRel(long childId) {
    return SessionUtils.getWithoutCommit(
        DomainRelMapper.class, mapper -> mapper.selectDomainRelByChildId(childId));
  }

  private long selectMetalakeId(DomainEntity domain) {
    return MetalakeMetaService.getInstance().getMetalakeIdByName(domain.namespace().level(0));
  }

  private int countDomainRelRows(long childId) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format("SELECT count(*) FROM domain_rel WHERE child_id = %d", childId))) {
      if (rs.next()) {
        return rs.getInt(1);
      }
      throw new RuntimeException("Doesn't contain data");
    } catch (SQLException se) {
      throw new RuntimeException("SQL execution failed", se);
    }
  }
}
