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
package org.apache.gravitino.stats;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.UnmodifiableStatisticException;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.StatisticEntity;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.Executable;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatisticManager {

  private static final Logger LOG = LoggerFactory.getLogger(StatisticManager.class);

  private final EntityStore store;

  private final IdGenerator idGenerator;

  public StatisticManager(EntityStore store, IdGenerator idGenerator) {
    this.store = store;
    this.idGenerator = idGenerator;
  }

  public List<Statistic> listStatistics(String metalake, MetadataObject metadataObject) {
    try {
      NameIdentifier identifier = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
      Entity.EntityType type = StatisticEntity.getStatisticType(metadataObject.type());
      return TreeLockUtils.doWithTreeLock(
          identifier,
          LockType.READ,
          () ->
              store.list(Namespace.fromString(identifier.toString()), StatisticEntity.class, type)
                  .stream()
                  .map(
                      entity -> {
                        String name = entity.name();
                        StatisticValue<?> value = entity.value();
                        return new CustomStatistic(name, value, entity.auditInfo());
                      })
                  .collect(Collectors.toList()));
    } catch (NoSuchEntityException nse) {
      LOG.warn(
          "Failed to list statistics for metadata object {} in the metalake {}: {}",
          metadataObject.fullName(),
          metalake,
          nse.getMessage());
      throw new NoSuchMetadataObjectException(
          "The metadata object %s in the metalake %s isn't found",
          metadataObject.fullName(), metalake);
    } catch (IOException ioe) {
      LOG.error(
          "Failed to list statistics for metadata object {} in the metalake {} : {}",
          metadataObject.fullName(),
          metalake,
          ioe.getMessage());
      throw new RuntimeException(ioe);
    }
  }

  public void updateStatistics(
      String metalake, MetadataObject metadataObject, Map<String, StatisticValue<?>> statistics) {
    try {
      NameIdentifier identifier = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
      List<StatisticEntity> statisticEntities = Lists.newArrayList();
      for (Map.Entry<String, StatisticValue<?>> entry : statistics.entrySet()) {
        String name = entry.getKey();
        StatisticValue<?> value = entry.getValue();

        StatisticEntity statistic =
            StatisticEntity.builder(StatisticEntity.getStatisticType(metadataObject.type()))
                .withId(idGenerator.nextId())
                .withName(name)
                .withValue(value)
                .withNamespace(Namespace.fromString(identifier.toString()))
                .withAuditInfo(
                    AuditInfo.builder()
                        .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                        .withCreateTime(Instant.now())
                        .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                        .withLastModifiedTime(Instant.now())
                        .build())
                .build();
        statisticEntities.add(statistic);
      }
      TreeLockUtils.doWithTreeLock(
          identifier,
          LockType.WRITE,
          (Executable<Void, IOException>)
              () -> {
                store.batchPut(statisticEntities, true);
                return null;
              });

    } catch (NoSuchEntityException nse) {
      LOG.warn(
          "Failed to update statistics for metadata object {} in the metalake {}: {}",
          metadataObject.fullName(),
          metalake,
          nse.getMessage());
      throw new NoSuchMetadataObjectException(
          "The metadata object %s in the metalake %s isn't found",
          metadataObject.fullName(), metalake);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  public boolean dropStatistics(
      String metalake, MetadataObject metadataObject, List<String> statistics)
      throws UnmodifiableStatisticException {
    try {
      NameIdentifier identifier = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
      Entity.EntityType type = StatisticEntity.getStatisticType(metadataObject.type());
      List<Pair<NameIdentifier, Entity.EntityType>> idents = Lists.newArrayList();

      for (String statistic : statistics) {
        Pair<NameIdentifier, Entity.EntityType> pair =
            Pair.of(NameIdentifierUtil.ofStatistic(identifier, statistic), type);
        idents.add(pair);
      }
      int deleteCount =
          TreeLockUtils.doWithTreeLock(
              identifier, LockType.WRITE, () -> store.batchDelete(idents, true));
      // If deleteCount is 0, it means that the statistics were not found.
      return deleteCount != 0;
    } catch (NoSuchEntityException nse) {
      LOG.warn(
          "Failed to drop statistics for metadata object {} in the metalake {} : {}",
          metadataObject.fullName(),
          metalake,
          nse.getMessage());
      throw new NoSuchMetadataObjectException(
          "The metadata object %s in the metalake %s isn't found",
          metadataObject.fullName(), metalake);
    } catch (IOException ioe) {
      LOG.error(
          "Failed to drop statistics for metadata object {} in the metalake {} : {}",
          metadataObject.fullName(),
          metalake,
          ioe.getMessage());
      throw new RuntimeException(ioe);
    }
  }

  @VisibleForTesting
  public static class CustomStatistic implements Statistic {

    private final String name;
    private final StatisticValue<?> value;
    private final Audit auditInfo;

    public CustomStatistic(String name, StatisticValue<?> value, Audit auditInfo) {
      this.name = name;
      this.value = value;
      this.auditInfo = auditInfo;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Optional<StatisticValue<?>> value() {
      return Optional.of(value);
    }

    @Override
    public boolean reserved() {
      return false;
    }

    @Override
    public boolean modifiable() {
      return true;
    }

    @Override
    public Audit auditInfo() {
      return auditInfo;
    }
  }
}
