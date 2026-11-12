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

import static org.mockito.ArgumentMatchers.any;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsDomainOperations.LockedDomainState;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.DomainMetaMapper;
import org.apache.gravitino.storage.relational.mapper.DomainRelMapper;
import org.apache.gravitino.storage.relational.po.DomainPO;
import org.apache.gravitino.storage.relational.po.DomainRelPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.Mockito;

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
        entity -> ((DomainEntity) entity).toBuilder().withName("Finance_Global").build());
    Assertions.assertEquals(
        "Finance_Global",
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

  @TestTemplate
  public void testCaughtNonEmptyDeleteDoesNotMutateParent() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity parent = insertDomain(METALAKE_NAME, "Parent");
    DomainEntity child = insertDomain(METALAKE_NAME, "Child");
    insertDomainRel(child, parent);
    SessionUtils.doMultipleWithCommit(
        () ->
            Assertions.assertThrows(
                NonEmptyEntityException.class,
                () -> domainMetaService.deleteDomain(parent.nameIdentifier())),
        () -> {
          try {
            domainMetaService.updateDomain(
                child.nameIdentifier(),
                entity -> ((DomainEntity) entity).toBuilder().withComment("changed").build());
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
    Assertions.assertTrue(backend.exists(parent.nameIdentifier(), Entity.EntityType.DOMAIN));
    Assertions.assertEquals(
        "Parent", domainRelService.getParentDomainName(child.id()).orElse(null));
    Assertions.assertEquals(
        "changed", domainMetaService.getDomainByIdentifier(child.nameIdentifier()).comment());
  }

  @TestTemplate
  public void testRepeatedDetachWithinTransaction() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity parent = insertDomain(METALAKE_NAME, "Parent");
    DomainEntity child = insertDomain(METALAKE_NAME, "Child");
    SessionUtils.doMultipleWithCommit(
        () -> {
          try {
            for (int i = 0; i < 3; i++) {
              insertDomainRel(child, parent);
              Assertions.assertFalse(
                  domainRelService
                      .removeDomainParentAndGet(child.nameIdentifier(), state -> {})
                      .parent()
                      .isPresent());
            }
            insertDomainRel(child, parent);
            Assertions.assertTrue(domainMetaService.deleteDomain(child.nameIdentifier()));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
    Assertions.assertEquals(4, countDomainRelRows(child.id()));
    Assertions.assertNull(selectDomainRel(child.id()));
  }

  @TestTemplate
  public void testLockedUpdateReadsPastCachedParent() throws Exception {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity first = insertDomain(METALAKE_NAME, "First");
    DomainEntity second = insertDomain(METALAKE_NAME, "Second");
    DomainEntity child = insertDomain(METALAKE_NAME, "Child");
    insertDomainRel(child, first);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    SessionUtils.beginTransaction();
    try {
      Assertions.assertEquals(
          "First",
          domainMetaService.getDomainByIdentifier(child.nameIdentifier()).parent().orElse(null));
      Assertions.assertEquals(
          "First", domainRelService.getParentDomainName(child.id()).orElse(null));
      executor
          .submit(
              () -> {
                try {
                  domainRelService.updateDomainRel(
                      child.nameIdentifier(), second.nameIdentifier(), AUDIT_INFO);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              })
          .get(30, TimeUnit.SECONDS);
      Assertions.assertEquals(
          "Second", domainRelService.getParentDomainName(child.id()).orElse(null));
      domainMetaService.updateDomain(
          child.nameIdentifier(),
          entity -> {
            DomainEntity domain = (DomainEntity) entity;
            Assertions.assertEquals("Second", domain.parent().orElse(null));
            return entity;
          });
      SessionUtils.commitTransaction();
    } finally {
      if (SessionUtils.isInTransaction()) {
        SessionUtils.rollbackTransaction();
      }
      executor.shutdownNow();
    }
  }

  @TestTemplate
  public void testResponseReadFailureRollsBackUpdate() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity child = insertDomain(METALAKE_NAME, "Child");
    installReadProbe();
    AtomicBoolean written = new AtomicBoolean();
    ReadProbe.AFTER_WRITE.set(
        id -> {
          if (id.endsWith(".updateDomainMeta")) {
            written.set(true);
          }
        });
    ReadProbe.BEFORE_READ.set(
        id -> {
          if (written.get()) {
            throw new IllegalStateException("response read failed");
          }
        });
    try {
      Assertions.assertThrows(
          PersistenceException.class,
          () ->
              domainMetaService.updateDomain(
                  child.nameIdentifier(),
                  entity ->
                      ((DomainEntity) entity).toBuilder().withComment("must roll back").build()));
    } finally {
      ReadProbe.clear();
    }
    Assertions.assertTrue(written.get());
    Assertions.assertNull(
        domainMetaService.getDomainByIdentifier(child.nameIdentifier()).comment());
  }

  @TestTemplate
  public void testReadPathsKeepMetadataAndParentTogether() throws Exception {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity first = insertDomain(METALAKE_NAME, "First");
    DomainEntity second = insertDomain(METALAKE_NAME, "Second");
    DomainEntity child = insertDomain(METALAKE_NAME, "Child");
    insertDomainRel(child, first);
    installReadProbe();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      for (String path : new String[] {"get", "list", "batch", "subdomains"}) {
        domainRelService.updateDomainRel(
            child.nameIdentifier(), first.nameIdentifier(), AUDIT_INFO);
        domainMetaService.updateDomain(first.nameIdentifier(), entity -> entity);
        domainMetaService.updateDomain(
            child.nameIdentifier(),
            entity -> ((DomainEntity) entity).toBuilder().withComment("old").build());
        AtomicBoolean triggered = new AtomicBoolean();
        String method =
            path.equals("get")
                ? "selectDomainMetaByMetalakeAndName"
                : path.equals("list")
                    ? "listDomainPOsByMetalake"
                    : path.equals("batch")
                        ? "listDomainPOsByMetalakeAndDomainNames"
                        : "listDomainPOsByParentIdentifier";
        ReadProbe.AFTER_READ.set(
            id -> {
              if (id.endsWith("." + method) && triggered.compareAndSet(false, true)) {
                try {
                  executor
                      .submit(
                          () ->
                              SessionUtils.doMultipleWithCommit(
                                  () -> {
                                    try {
                                      domainMetaService.updateDomain(
                                          child.nameIdentifier(),
                                          entity ->
                                              ((DomainEntity) entity)
                                                  .toBuilder().withComment("new").build());
                                      domainRelService.updateDomainRel(
                                          child.nameIdentifier(),
                                          second.nameIdentifier(),
                                          AUDIT_INFO);
                                    } catch (IOException e) {
                                      throw new RuntimeException(e);
                                    }
                                  }))
                      .get(30, TimeUnit.SECONDS);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
            });
        DomainEntity read;
        try {
          if (path.equals("get")) {
            read = domainMetaService.getDomainByIdentifier(child.nameIdentifier());
          } else {
            List<DomainEntity> domains =
                path.equals("list")
                    ? domainMetaService.listDomainsByNamespace(child.namespace())
                    : path.equals("batch")
                        ? domainMetaService.batchGetDomainByIdentifier(
                            Collections.singletonList(child.nameIdentifier()))
                        : domainRelService.listSubDomains(first.nameIdentifier());
            read = domains.stream().filter(d -> d.id().equals(child.id())).findFirst().get();
          }
        } finally {
          ReadProbe.clear();
        }
        Assertions.assertTrue(triggered.get(), path);
        Assertions.assertEquals("old", read.comment(), path);
        Assertions.assertEquals("First", read.parent().orElse(null), path);
      }
    } finally {
      ReadProbe.clear();
      executor.shutdownNow();
    }
  }

  @TestTemplate
  public void testSubDomainReadUsesCurrentParentName() throws Exception {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity parent = insertDomain(METALAKE_NAME, "Parent");
    DomainEntity child = insertDomain(METALAKE_NAME, "Child");
    insertDomainRel(child, parent);
    installReadProbe();
    AtomicBoolean triggered = new AtomicBoolean();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    ReadProbe.AFTER_READ.set(
        id -> {
          if (id.endsWith(".listDomainPOsByParentIdentifier")
              && triggered.compareAndSet(false, true)) {
            try {
              executor
                  .submit(
                      () ->
                          SessionUtils.doMultipleWithCommit(
                              () -> {
                                try {
                                  domainMetaService.updateDomain(
                                      parent.nameIdentifier(),
                                      entity ->
                                          ((DomainEntity) entity)
                                              .toBuilder().withName("Renamed").build());
                                  domainMetaService.updateDomain(
                                      child.nameIdentifier(),
                                      entity ->
                                          ((DomainEntity) entity)
                                              .toBuilder().withComment("new").build());
                                } catch (IOException e) {
                                  throw new RuntimeException(e);
                                }
                              }))
                  .get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        });
    try {
      DomainEntity read = domainRelService.listSubDomains(parent.nameIdentifier()).get(0);
      Assertions.assertTrue(triggered.get());
      Assertions.assertNull(read.comment());
      Assertions.assertEquals("Parent", read.parent().orElse(null));
    } finally {
      ReadProbe.clear();
      executor.shutdownNow();
    }
  }

  @TestTemplate
  public void testRepeatedDetachThenMetalakeDelete() throws IOException {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity parent = insertDomain(METALAKE_NAME, "Parent");
    DomainEntity child = insertDomain(METALAKE_NAME, "Child");
    SessionUtils.doMultipleWithCommit(
        () -> {
          insertDomainRel(child, parent);
          try {
            Assertions.assertFalse(
                domainRelService
                    .removeDomainParentAndGet(child.nameIdentifier(), state -> {})
                    .parent()
                    .isPresent());
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          insertDomainRel(child, parent);
          Assertions.assertTrue(
              MetalakeMetaService.getInstance().deleteMetalake(metalake.nameIdentifier(), false));
        });
    Assertions.assertEquals(2, countDomainRelRows(child.id()));
    Assertions.assertNull(selectDomainRel(child.id()));
  }

  @TestTemplate
  public void testSubDomainsResolveRecreatedParentInOneStatement() throws Exception {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity parent = insertDomain(METALAKE_NAME, "Parent");
    DomainEntity child = insertDomain(METALAKE_NAME, "Child");
    insertDomainRel(child, parent);
    installReadProbe();
    AtomicBoolean triggered = new AtomicBoolean();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    ReadProbe.BEFORE_READ.set(
        id -> {
          // Replace the parent after any preliminary lookup but before the children query.
          // A two-statement implementation would keep the original parent's ID.
          if (id.endsWith(".listDomainPOsByParentIdentifier")
              && triggered.compareAndSet(false, true)) {
            try {
              executor
                  .submit(
                      () ->
                          SessionUtils.doMultipleWithCommit(
                              () -> {
                                Assertions.assertTrue(
                                    domainMetaService.deleteDomain(child.nameIdentifier()));
                                Assertions.assertTrue(
                                    domainMetaService.deleteDomain(parent.nameIdentifier()));
                                try {
                                  DomainEntity replacement = insertDomain(METALAKE_NAME, "Parent");
                                  DomainEntity replacementChild =
                                      insertDomain(METALAKE_NAME, "ReplacementChild");
                                  insertDomainRel(replacementChild, replacement);
                                } catch (IOException e) {
                                  throw new RuntimeException(e);
                                }
                              }))
                  .get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        });
    try {
      List<DomainEntity> children = domainRelService.listSubDomains(parent.nameIdentifier());
      Assertions.assertTrue(triggered.get());
      Assertions.assertEquals(1, children.size());
      Assertions.assertEquals("ReplacementChild", children.get(0).name());
      Assertions.assertEquals("Parent", children.get(0).parent().orElse(null));
    } finally {
      ReadProbe.clear();
      executor.shutdownNow();
    }
  }

  @TestTemplate
  public void testInsertDomainWithCheck() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity finance = newDomain(METALAKE_NAME, "Finance", null);
    List<LockedDomainState> states = new ArrayList<>();

    DomainEntity inserted = insertDomainWithCheck(finance, states::add);
    Assertions.assertEquals(
        inserted, domainMetaService.getDomainByIdentifier(finance.nameIdentifier()));
    Assertions.assertEquals(1, states.size());
    Assertions.assertEquals(inserted, states.get(0).domain());
    Assertions.assertFalse(states.get(0).parent().isPresent());
    Assertions.assertTrue(states.get(0).subDomains().isEmpty());

    // A domain with a parent is inserted together with its edge, and the check sees the parent.
    DomainEntity trading = newDomain(METALAKE_NAME, "Trading", "Finance");
    states.clear();
    inserted = insertDomainWithCheck(trading, states::add);
    Assertions.assertEquals(
        inserted, domainMetaService.getDomainByIdentifier(trading.nameIdentifier()));
    Assertions.assertEquals(finance.id(), selectDomainRel(trading.id()).parentId());
    LockedDomainState state = states.get(0);
    Assertions.assertEquals(finance.id(), state.parent().get().id());
    Assertions.assertFalse(state.parent().get().parent().isPresent());

    // The check sees that the parent is itself a sub-domain.
    DomainEntity desk = newDomain(METALAKE_NAME, "Desk", "Trading");
    states.clear();
    insertDomainWithCheck(desk, states::add);
    Assertions.assertEquals("Finance", states.get(0).parent().get().parent().get());

    // A failing check inserts neither the domain nor its edge.
    DomainEntity rejected = newDomain(METALAKE_NAME, "Rejected", "Finance");
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            insertDomainWithCheck(
                rejected,
                s -> {
                  throw new IllegalArgumentException("rejected");
                }));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> domainMetaService.getDomainByIdentifier(rejected.nameIdentifier()));
    Assertions.assertEquals(0, countDomainRelRows(rejected.id()));

    DomainEntity orphan = newDomain(METALAKE_NAME, "Orphan", "missing");
    Assertions.assertThrows(
        NoSuchEntityException.class, () -> insertDomainWithCheck(orphan, s -> {}));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> domainMetaService.getDomainByIdentifier(orphan.nameIdentifier()));

    Assertions.assertThrows(
        EntityAlreadyExistsException.class,
        () -> insertDomainWithCheck(newDomain(METALAKE_NAME, "Finance", null), s -> {}));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> insertDomainWithCheck(newDomain("missing_metalake", "X", null), s -> {}));
  }

  @TestTemplate
  public void testInsertDomainWithCheckRejectsOuterTransaction() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity finance = insertDomain(METALAKE_NAME, "Finance");
    for (String parent : new String[] {null, finance.name()}) {
      DomainEntity rejected = newDomain(METALAKE_NAME, "Rejected", parent);
      DomainEntity accepted = newDomain(METALAKE_NAME, "Accepted" + rejected.id(), null);
      SessionUtils.doMultipleWithCommit(
          () -> {
            Assertions.assertThrows(
                IllegalStateException.class,
                () ->
                    insertDomainWithCheck(
                        rejected,
                        state -> {
                          throw new IllegalArgumentException("rejected");
                        }));
            // Catching the rejection must not leave a domain for the outer commit to persist.
            // A separate write confirms that the outer transaction still commits normally.
            try {
              domainMetaService.insertDomain(accepted, false);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });
      Assertions.assertEquals(
          accepted, domainMetaService.getDomainByIdentifier(accepted.nameIdentifier()));
      Assertions.assertThrows(
          NoSuchEntityException.class,
          () -> domainMetaService.getDomainByIdentifier(rejected.nameIdentifier()));
      Assertions.assertEquals(0, countDomainRelRows(rejected.id()));
    }
  }

  @TestTemplate
  public void testSetDomainParent() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    String anotherMetalakeName = METALAKE_NAME + "_another";
    createAndInsertMakeLake(anotherMetalakeName);
    DomainEntity finance = insertDomain(METALAKE_NAME, "Finance");
    DomainEntity markets = insertDomain(METALAKE_NAME, "Markets");
    DomainEntity trading = insertDomain(METALAKE_NAME, "Trading");
    DomainEntity desk = insertDomain(METALAKE_NAME, "Desk");
    DomainEntity elsewhere = insertDomain(anotherMetalakeName, "Elsewhere");
    insertDomainRel(desk, trading);
    List<LockedDomainState> states = new ArrayList<>();

    // Attaching a top-level domain inserts its edge; the check sees the locked domains.
    DomainEntity attached =
        domainRelService.setDomainParentAndGet(
            trading.nameIdentifier(), finance.nameIdentifier(), AUDIT_INFO, states::add);
    Assertions.assertEquals(trading.toBuilder().withParent("Finance").build(), attached);
    LockedDomainState state = states.get(0);
    Assertions.assertEquals(trading.id(), state.domain().id());
    Assertions.assertFalse(state.domain().parent().isPresent());
    Assertions.assertEquals(finance.id(), state.parent().get().id());
    Assertions.assertFalse(state.parent().get().parent().isPresent());
    Assertions.assertEquals(
        Lists.newArrayList(desk.id()),
        state.subDomains().stream().map(DomainEntity::id).collect(Collectors.toList()));
    Assertions.assertEquals(1L, selectDomainRel(trading.id()).currentVersion());

    // Moving it re-points the edge; the check sees the current parent.
    states.clear();
    Instant modifiedTime = Instant.now();
    AuditInfo modifiedAuditInfo =
        AuditInfo.builder()
            .withCreator("mover")
            .withCreateTime(modifiedTime)
            .withLastModifier("mover")
            .withLastModifiedTime(modifiedTime)
            .build();
    DomainEntity moved =
        domainRelService.setDomainParentAndGet(
            trading.nameIdentifier(), markets.nameIdentifier(), modifiedAuditInfo, states::add);
    Assertions.assertEquals("Markets", moved.parent().get());
    Assertions.assertEquals("Finance", states.get(0).domain().parent().get());
    Assertions.assertEquals(markets.id(), states.get(0).parent().get().id());
    DomainRelPO domainRelPO = selectDomainRel(trading.id());
    Assertions.assertEquals(2L, domainRelPO.currentVersion());
    AuditInfo storedAuditInfo =
        JsonUtils.anyFieldMapper().readValue(domainRelPO.auditInfo(), AuditInfo.class);
    Assertions.assertEquals(AUDIT_INFO.creator(), storedAuditInfo.creator());
    Assertions.assertEquals("mover", storedAuditInfo.lastModifier());

    // The check also sees that the new parent is itself a sub-domain.
    states.clear();
    domainRelService.setDomainParentAndGet(
        finance.nameIdentifier(), desk.nameIdentifier(), AUDIT_INFO, states::add);
    Assertions.assertEquals("Trading", states.get(0).parent().get().parent().get());
    domainRelService.removeDomainParentAndGet(finance.nameIdentifier(), s -> {});

    // Setting the current parent changes nothing.
    domainRelService.setDomainParentAndGet(
        trading.nameIdentifier(), markets.nameIdentifier(), modifiedAuditInfo, s -> {});
    Assertions.assertEquals(2L, selectDomainRel(trading.id()).currentVersion());

    // A failing check changes nothing.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            domainRelService.setDomainParentAndGet(
                trading.nameIdentifier(),
                finance.nameIdentifier(),
                AUDIT_INFO,
                s -> {
                  throw new IllegalArgumentException("rejected");
                }));
    Assertions.assertEquals("Markets", domainRelService.getParentDomainName(trading.id()).get());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            domainRelService.setDomainParentAndGet(
                trading.nameIdentifier(), trading.nameIdentifier(), AUDIT_INFO, s -> {}));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            domainRelService.setDomainParentAndGet(
                elsewhere.nameIdentifier(), finance.nameIdentifier(), AUDIT_INFO, s -> {}));
    // On MySQL, whose collation ignores trailing spaces, both names resolve to the same row, which
    // is refused as its own parent; elsewhere the second name does not exist. No edge is written.
    RuntimeException selfParent =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                domainRelService.setDomainParentAndGet(
                    finance.nameIdentifier(),
                    NameIdentifier.of(NamespaceUtil.ofDomain(METALAKE_NAME), "Finance "),
                    AUDIT_INFO,
                    s -> {}));
    Assertions.assertTrue(
        selfParent instanceof IllegalArgumentException
            || selfParent instanceof NoSuchEntityException,
        String.valueOf(selfParent));
    Assertions.assertFalse(domainRelService.getParentDomainName(finance.id()).isPresent());

    NameIdentifier missing = NameIdentifierUtil.ofDomain(METALAKE_NAME, "missing");
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            domainRelService.setDomainParentAndGet(
                missing, finance.nameIdentifier(), AUDIT_INFO, s -> {}));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            domainRelService.setDomainParentAndGet(
                trading.nameIdentifier(), missing, AUDIT_INFO, s -> {}));
  }

  @TestTemplate
  public void testRemoveDomainParent() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity finance = insertDomain(METALAKE_NAME, "Finance");
    DomainEntity trading = insertDomain(METALAKE_NAME, "Trading");
    insertDomainRel(trading, finance);
    List<LockedDomainState> states = new ArrayList<>();

    // A failing check keeps the edge.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            domainRelService.removeDomainParentAndGet(
                trading.nameIdentifier(),
                s -> {
                  throw new IllegalArgumentException("rejected");
                }));
    Assertions.assertEquals("Finance", domainRelService.getParentDomainName(trading.id()).get());

    // The check sees the parent the edge is removed from.
    Assertions.assertEquals(
        trading, domainRelService.removeDomainParentAndGet(trading.nameIdentifier(), states::add));
    Assertions.assertEquals("Finance", states.get(0).domain().parent().get());
    Assertions.assertFalse(states.get(0).parent().isPresent());
    Assertions.assertTrue(states.get(0).subDomains().isEmpty());
    Assertions.assertFalse(domainRelService.getParentDomainName(trading.id()).isPresent());
    Assertions.assertTrue(domainRelService.listSubDomains(finance.nameIdentifier()).isEmpty());

    // Removing the parent of a top-level domain changes nothing.
    states.clear();
    Assertions.assertEquals(
        trading, domainRelService.removeDomainParentAndGet(trading.nameIdentifier(), states::add));
    Assertions.assertFalse(states.get(0).domain().parent().isPresent());
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            domainRelService.removeDomainParentAndGet(
                NameIdentifierUtil.ofDomain(METALAKE_NAME, "missing"), s -> {}));

    // A detached domain can be attached again.
    insertDomainRel(trading, finance);
    Assertions.assertEquals("Finance", domainRelService.getParentDomainName(trading.id()).get());
  }

  @TestTemplate
  public void testUpdateDomainStatus() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity finance = insertDomain(METALAKE_NAME, "Finance");
    DomainEntity trading = insertDomain(METALAKE_NAME, "Trading");
    insertDomainRel(trading, finance);
    List<LockedDomainState> states = new ArrayList<>();
    Instant modifiedTime = Instant.now();
    AuditInfo modifiedAuditInfo =
        AuditInfo.builder()
            .withLastModifier("publisher")
            .withLastModifiedTime(modifiedTime)
            .build();

    // A top-level domain has no parent; the check sees its sub-domains.
    DomainEntity published =
        domainRelService.updateDomainStatusAndGet(
            finance.nameIdentifier(), Domain.Status.PUBLISHED, modifiedAuditInfo, states::add);
    Assertions.assertEquals(Domain.Status.PUBLISHED, published.status());
    Assertions.assertEquals(AUDIT_INFO.creator(), published.auditInfo().creator());
    Assertions.assertEquals("publisher", published.auditInfo().lastModifier());
    Assertions.assertEquals(modifiedTime, published.auditInfo().lastModifiedTime());
    Assertions.assertEquals(
        published, domainMetaService.getDomainByIdentifier(finance.nameIdentifier()));
    Assertions.assertEquals(Domain.Status.DRAFT, states.get(0).domain().status());
    Assertions.assertFalse(states.get(0).parent().isPresent());
    Assertions.assertEquals(trading.id(), states.get(0).subDomains().get(0).id());
    Assertions.assertEquals(Domain.Status.DRAFT, states.get(0).subDomains().get(0).status());

    // A sub-domain's check sees its parent.
    states.clear();
    DomainEntity publishedTrading =
        domainRelService.updateDomainStatusAndGet(
            trading.nameIdentifier(), Domain.Status.PUBLISHED, modifiedAuditInfo, states::add);
    Assertions.assertEquals("Finance", publishedTrading.parent().get());
    Assertions.assertEquals("Finance", states.get(0).domain().parent().get());
    Assertions.assertEquals(Domain.Status.PUBLISHED, states.get(0).parent().get().status());

    // Setting the current status writes nothing.
    long version = selectDomainVersion(trading.id());
    domainRelService.updateDomainStatusAndGet(
        trading.nameIdentifier(), Domain.Status.PUBLISHED, modifiedAuditInfo, s -> {});
    Assertions.assertEquals(version, selectDomainVersion(trading.id()));

    // A failing check changes nothing.
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            domainRelService.updateDomainStatusAndGet(
                finance.nameIdentifier(),
                Domain.Status.DRAFT,
                modifiedAuditInfo,
                s -> {
                  throw new IllegalArgumentException("rejected");
                }));
    Assertions.assertEquals(
        Domain.Status.PUBLISHED,
        domainMetaService.getDomainByIdentifier(finance.nameIdentifier()).status());

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            domainRelService.updateDomainStatusAndGet(
                NameIdentifierUtil.ofDomain(METALAKE_NAME, "missing"),
                Domain.Status.PUBLISHED,
                modifiedAuditInfo,
                s -> {}));
  }

  @TestTemplate
  public void testUpdateDomainStatusRetriesWhenParentChanges() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity finance = insertDomain(METALAKE_NAME, "Finance");
    DomainEntity markets = insertDomain(METALAKE_NAME, "Markets");
    DomainEntity trading = insertDomain(METALAKE_NAME, "Trading");
    insertDomainRel(trading, finance);
    NameIdentifier tradingIdent = trading.nameIdentifier();
    DomainPO observedPO = selectDomainPO(trading.id());
    DomainRelPO observedRelPO = selectDomainRel(trading.id());

    // The domain is moved after its parent was read: the attempt writes nothing.
    moveDomainRel(trading, markets);
    Assertions.assertFalse(
        domainRelService
            .tryUpdateDomainStatusOnce(
                tradingIdent,
                Domain.Status.PUBLISHED,
                AUDIT_INFO,
                s -> Assertions.fail("the check must not run"),
                observedPO,
                observedRelPO)
            .isPresent());
    Assertions.assertEquals(
        Domain.Status.DRAFT, domainMetaService.getDomainByIdentifier(tradingIdent).status());

    // The domain is detached after its parent was read.
    domainRelService.removeDomainParentAndGet(tradingIdent, s -> {});
    Assertions.assertFalse(
        domainRelService
            .tryUpdateDomainStatusOnce(
                tradingIdent,
                Domain.Status.PUBLISHED,
                AUDIT_INFO,
                s -> {},
                observedPO,
                observedRelPO)
            .isPresent());

    // The domain is attached after it was read as top-level.
    insertDomainRel(trading, markets);
    Assertions.assertFalse(
        domainRelService
            .tryUpdateDomainStatusOnce(
                tradingIdent, Domain.Status.PUBLISHED, AUDIT_INFO, s -> {}, observedPO, null)
            .isPresent());

    // A fresh read locks the current parent, which the check sees.
    List<LockedDomainState> states = new ArrayList<>();
    DomainEntity published =
        domainRelService.updateDomainStatusAndGet(
            tradingIdent, Domain.Status.PUBLISHED, AUDIT_INFO, states::add);
    Assertions.assertEquals(Domain.Status.PUBLISHED, published.status());
    Assertions.assertEquals(markets.id(), states.get(0).parent().get().id());
  }

  @TestTemplate
  public void testUpdateDomainStatusWhenObservedParentIsGone() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity finance = insertDomain(METALAKE_NAME, "Finance");
    DomainEntity trading = insertDomain(METALAKE_NAME, "Trading");
    insertDomainRel(trading, finance);
    DomainPO observedPO = selectDomainPO(trading.id());
    DomainRelPO observedRelPO = selectDomainRel(trading.id());

    // The domain is detached and its old parent deleted after the parent was read.
    domainRelService.removeDomainParentAndGet(trading.nameIdentifier(), s -> {});
    Assertions.assertTrue(domainMetaService.deleteDomain(finance.nameIdentifier()));
    Assertions.assertFalse(
        domainRelService
            .tryUpdateDomainStatusOnce(
                trading.nameIdentifier(),
                Domain.Status.PUBLISHED,
                AUDIT_INFO,
                s -> Assertions.fail("the check must not run"),
                observedPO,
                observedRelPO)
            .isPresent());
    Assertions.assertEquals(
        Domain.Status.DRAFT,
        domainMetaService.getDomainByIdentifier(trading.nameIdentifier()).status());
  }

  @TestTemplate
  public void testUpdateDomainStatusGivesUpAfterRetries() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity finance = insertDomain(METALAKE_NAME, "Finance");
    DomainRelService service = Mockito.spy(domainRelService);
    // Every attempt finds that the parent changed.
    Mockito.doReturn(Optional.empty())
        .when(service)
        .tryUpdateDomainStatusOnce(any(), any(), any(), any(), any(), any());

    Assertions.assertThrows(
        OptimisticLockException.class,
        () ->
            service.updateDomainStatusAndGet(
                finance.nameIdentifier(), Domain.Status.PUBLISHED, AUDIT_INFO, s -> {}));
    Mockito.verify(service, Mockito.times(3))
        .tryUpdateDomainStatusOnce(any(), any(), any(), any(), any(), any());
  }

  @TestTemplate
  public void testUpdateDomainStatusRejectsOuterTransaction() throws IOException {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity finance = insertDomain(METALAKE_NAME, "Finance");

    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            SessionUtils.doMultipleWithCommit(
                () -> {
                  try {
                    domainRelService.updateDomainStatusAndGet(
                        finance.nameIdentifier(), Domain.Status.PUBLISHED, AUDIT_INFO, s -> {});
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                }));
    Assertions.assertEquals(
        Domain.Status.DRAFT,
        domainMetaService.getDomainByIdentifier(finance.nameIdentifier()).status());
  }

  @TestTemplate
  public void testLockedOperationsWaitForDomainRows() throws Exception {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity finance = insertDomain(METALAKE_NAME, "Finance");
    DomainEntity markets = insertDomain(METALAKE_NAME, "Markets");
    DomainEntity trading = insertDomain(METALAKE_NAME, "Trading");
    DomainEntity desk = insertDomain(METALAKE_NAME, "Desk");
    insertDomainRel(trading, finance);

    // Only the domain row is held, not the metalake row, so each wait is on that domain row.
    assertWaitsForDomainRow(
        finance.id(),
        () ->
            domainRelService.updateDomainStatusAndGet(
                trading.nameIdentifier(), Domain.Status.PUBLISHED, AUDIT_INFO, s -> {}));
    assertWaitsForDomainRow(
        finance.id(),
        () -> {
          insertDomainWithCheck(newDomain(METALAKE_NAME, "Tax", "Finance"), s -> {});
          return null;
        });
    assertWaitsForDomainRow(
        markets.id(),
        () ->
            domainRelService.setDomainParentAndGet(
                desk.nameIdentifier(), markets.nameIdentifier(), AUDIT_INFO, s -> {}));
    assertWaitsForDomainRow(
        desk.id(),
        () ->
            domainRelService.setDomainParentAndGet(
                desk.nameIdentifier(), finance.nameIdentifier(), AUDIT_INFO, s -> {}));

    assertWaitsForDomainRow(
        desk.id(), () -> domainRelService.removeDomainParentAndGet(desk.nameIdentifier(), s -> {}));
    assertWaitsForDomainRow(
        markets.id(),
        () ->
            domainRelService.updateDomainStatusAndGet(
                markets.nameIdentifier(), Domain.Status.PUBLISHED, AUDIT_INFO, s -> {}));

    Assertions.assertEquals(
        Domain.Status.PUBLISHED,
        domainMetaService.getDomainByIdentifier(trading.nameIdentifier()).status());
    Assertions.assertEquals(
        Domain.Status.PUBLISHED,
        domainMetaService.getDomainByIdentifier(markets.nameIdentifier()).status());
    Assertions.assertFalse(domainRelService.getParentDomainName(desk.id()).isPresent());
  }

  @TestTemplate
  public void testLockedReadsBypassTheSessionCache() throws Exception {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity finance = insertDomain(METALAKE_NAME, "Finance");
    DomainEntity markets = insertDomain(METALAKE_NAME, "Markets");
    DomainEntity trading = insertDomain(METALAKE_NAME, "Trading");
    ExecutorService executor = Executors.newSingleThreadExecutor();

    try {
      SessionUtils.doMultipleWithCommit(
          () -> {
            // An outer transaction reads the edge before another transaction attaches the domain;
            // the empty result is now in this session's cache.
            Assertions.assertNull(selectDomainRel(trading.id()));
            try {
              executor.submit(() -> insertDomainRel(trading, finance)).get(30, TimeUnit.SECONDS);
              // Under its locks, the change reads the committed edge and re-points it, instead of
              // inserting a second edge from the cached empty read.
              DomainEntity moved =
                  domainRelService.setDomainParentAndGet(
                      trading.nameIdentifier(), markets.nameIdentifier(), AUDIT_INFO, s -> {});
              Assertions.assertEquals("Markets", moved.parent().get());
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
    } finally {
      executor.shutdownNow();
    }
    Assertions.assertEquals("Markets", domainRelService.getParentDomainName(trading.id()).get());
    Assertions.assertEquals(1, countDomainRelRows(trading.id()));
  }

  // On H2 the metalake row is locked exclusively, so there the publish waits on the metalake row;
  // testLockedOperationsWaitForDomainRows checks the parent row lock on every database.
  @TestTemplate
  public void testPublishWaitsForConcurrentParentUnpublish() throws Exception {
    createAndInsertMakeLake(METALAKE_NAME);
    DomainEntity finance = insertDomain(METALAKE_NAME, "Finance");
    DomainEntity trading = insertDomain(METALAKE_NAME, "Trading");
    insertDomainRel(trading, finance);
    domainRelService.updateDomainStatusAndGet(
        finance.nameIdentifier(), Domain.Status.PUBLISHED, AUDIT_INFO, s -> {});

    CountDownLatch unpublishChecked = new CountDownLatch(1);
    CountDownLatch allowUnpublishCommit = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<Throwable> unpublishResult =
        executor.submit(
            () -> {
              try {
                domainRelService.updateDomainStatusAndGet(
                    finance.nameIdentifier(),
                    Domain.Status.DRAFT,
                    AUDIT_INFO,
                    s -> {
                      unpublishChecked.countDown();
                      await(allowUnpublishCommit);
                    });
                return null;
              } catch (Throwable throwable) {
                return throwable;
              }
            });

    try {
      Assertions.assertTrue(unpublishChecked.await(30, TimeUnit.SECONDS));
      Future<Throwable> publishResult =
          executor.submit(
              () -> {
                try {
                  domainRelService.updateDomainStatusAndGet(
                      trading.nameIdentifier(),
                      Domain.Status.PUBLISHED,
                      AUDIT_INFO,
                      s ->
                          Preconditions.checkArgument(
                              s.parent().get().status() == Domain.Status.PUBLISHED,
                              "The parent is not published"));
                  return null;
                } catch (Throwable throwable) {
                  return throwable;
                }
              });
      // The unpublish holds the parent row, so the publish waits and then sees the draft parent.
      Assertions.assertThrows(
          TimeoutException.class, () -> publishResult.get(500, TimeUnit.MILLISECONDS));

      allowUnpublishCommit.countDown();
      Assertions.assertNull(unpublishResult.get(30, TimeUnit.SECONDS));
      Throwable publishFailure = publishResult.get(30, TimeUnit.SECONDS);
      Assertions.assertTrue(
          publishFailure instanceof IllegalArgumentException, String.valueOf(publishFailure));
    } finally {
      allowUnpublishCommit.countDown();
      executor.shutdownNow();
    }

    Assertions.assertEquals(
        Domain.Status.DRAFT,
        domainMetaService.getDomainByIdentifier(trading.nameIdentifier()).status());
  }

  // Holds the row of a domain in another transaction, checks that the operation waits for it,
  // and lets the operation finish once the row is released.
  private void assertWaitsForDomainRow(long domainId, Callable<?> operation) throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (SqlSession holder =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(false)) {
      try (Statement statement = holder.getConnection().createStatement();
          ResultSet rs =
              statement.executeQuery(
                  String.format(
                      "SELECT domain_id FROM domain_meta WHERE domain_id = %d FOR UPDATE",
                      domainId))) {
        Assertions.assertTrue(rs.next());
      }
      Future<?> result = executor.submit(operation);
      Assertions.assertThrows(TimeoutException.class, () -> result.get(500, TimeUnit.MILLISECONDS));
      holder.rollback(true);
      result.get(30, TimeUnit.SECONDS);
    } finally {
      executor.shutdownNow();
    }
  }

  private void installReadProbe() {
    var configuration =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().getConfiguration();
    if (configuration.getInterceptors().stream().noneMatch(i -> i instanceof ReadProbe)) {
      configuration.addInterceptor(new ReadProbe());
    }
  }

  /** Injects controlled interleavings and failures without changing production mapper code. */
  @Intercepts({
    @Signature(
        type = Executor.class,
        method = "query",
        args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class}),
    @Signature(
        type = Executor.class,
        method = "update",
        args = {MappedStatement.class, Object.class})
  })
  public static class ReadProbe implements Interceptor {
    private static final ThreadLocal<Consumer<String>> BEFORE_READ = new ThreadLocal<>();
    private static final ThreadLocal<Consumer<String>> AFTER_READ = new ThreadLocal<>();
    private static final ThreadLocal<Consumer<String>> AFTER_WRITE = new ThreadLocal<>();

    /** Creates an inactive probe. */
    public ReadProbe() {}

    /** {@inheritDoc} */
    @Override
    public Object intercept(Invocation invocation) throws Throwable {
      String id = ((MappedStatement) invocation.getArgs()[0]).getId();
      boolean read = invocation.getMethod().getName().equals("query");
      if (read && BEFORE_READ.get() != null) {
        BEFORE_READ.get().accept(id);
      }
      Object result = invocation.proceed();
      Consumer<String> after = read ? AFTER_READ.get() : AFTER_WRITE.get();
      if (after != null) {
        after.accept(id);
      }
      return result;
    }

    private static void clear() {
      BEFORE_READ.remove();
      AFTER_READ.remove();
      AFTER_WRITE.remove();
    }
  }

  private DomainEntity newDomain(String metalake, String name, String parent) {
    return DomainEntity.builder()
        .withId(RandomIdGenerator.INSTANCE.nextId())
        .withName(name)
        .withNamespace(NamespaceUtil.ofDomain(metalake))
        .withProperties(ImmutableMap.of("k1", "v1"))
        .withStatus(Domain.Status.DRAFT)
        .withParent(parent)
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  // Inserts a domain with a new backing tag named after its ID, and returns the domain as the check
  // saw it, with its backing tag set.
  private DomainEntity insertDomainWithCheck(DomainEntity domain, Consumer<LockedDomainState> check)
      throws IOException {
    TagEntity backingTag =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag_" + domain.id())
            .withNamespace(NamespaceUtil.ofTag(domain.namespace().level(0)))
            .withAllowedValues(new String[0])
            .withAuditInfo(AUDIT_INFO)
            .build();
    DomainEntity[] checked = new DomainEntity[1];
    domainMetaService.insertDomainWithNewBackingTag(
        domain,
        backingTag,
        state -> {
          check.accept(state);
          checked[0] = state.domain();
        });
    return checked[0];
  }

  private void moveDomainRel(DomainEntity child, DomainEntity newParent) throws IOException {
    domainRelService.updateDomainRel(
        child.nameIdentifier(), newParent.nameIdentifier(), AUDIT_INFO);
  }

  private DomainPO selectDomainPO(long domainId) {
    return SessionUtils.getWithoutCommit(
        DomainMetaMapper.class, mapper -> mapper.selectDomainMetaById(domainId));
  }

  private long selectDomainVersion(long domainId) {
    return selectDomainPO(domainId).currentVersion();
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
