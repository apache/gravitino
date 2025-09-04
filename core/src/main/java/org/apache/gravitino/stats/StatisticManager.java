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
import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
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
import org.apache.gravitino.stats.storage.MetadataObjectStatisticsDrop;
import org.apache.gravitino.stats.storage.MetadataObjectStatisticsUpdate;
import org.apache.gravitino.stats.storage.PartitionStatisticStorage;
import org.apache.gravitino.stats.storage.PartitionStatisticStorageFactory;
import org.apache.gravitino.stats.storage.PersistedPartitionStatistics;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.Executable;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatisticManager implements Closeable {

  private static final String OPTIONS_PREFIX = "gravitino.stats.partition.storageOption.";
  private static final Logger LOG = LoggerFactory.getLogger(StatisticManager.class);

  private final EntityStore store;

  private final IdGenerator idGenerator;
  private final PartitionStatisticStorage partitionStorage;

  public StatisticManager(EntityStore store, IdGenerator idGenerator, Config config) {
    this.store = store;
    this.idGenerator = idGenerator;
    String className = config.get(Configs.PARTITION_STATS_STORAGE_FACTORY_CLASS);
    Map<String, String> options = config.getConfigsWithPrefix(OPTIONS_PREFIX);
    try {
      PartitionStatisticStorageFactory factory =
          (PartitionStatisticStorageFactory)
              Class.forName(className).getDeclaredConstructor().newInstance();
      this.partitionStorage = factory.create(options);
    } catch (Exception e) {
      LOG.error(
          "Failed to create and initialize partition statistics storage factory by name {}.",
          className,
          e);
      throw new RuntimeException(
          "Failed to create and initialize partition statistics storage factory: " + className, e);
    }
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

  public boolean dropPartitionStatistics(
      String metalake,
      MetadataObject metadataObject,
      List<PartitionStatisticsDrop> partitionStatistics) {
    try {
      List<MetadataObjectStatisticsDrop> partitionStatisticsToDrop =
          Lists.newArrayList(MetadataObjectStatisticsDrop.of(metadataObject, partitionStatistics));
      NameIdentifier identifier = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);

      return TreeLockUtils.doWithTreeLock(
              identifier,
              LockType.WRITE,
              () -> partitionStorage.dropStatistics(metalake, partitionStatisticsToDrop))
          != 0;
    } catch (IOException ioe) {
      LOG.error(
          "Failed to drop partition statistics for {} in metalake {}.",
          metadataObject,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  public void updatePartitionStatistics(
      String metalake,
      MetadataObject metadataObject,
      List<PartitionStatisticsUpdate> partitionStatistics) {
    try {
      NameIdentifier identifier = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);

      List<MetadataObjectStatisticsUpdate> statisticsToUpdate = Lists.newArrayList();
      statisticsToUpdate.add(
          MetadataObjectStatisticsUpdate.of(metadataObject, partitionStatistics));
      TreeLockUtils.doWithTreeLock(
          identifier,
          LockType.WRITE,
          (Executable<Void, IOException>)
              () -> {
                partitionStorage.updateStatistics(metalake, statisticsToUpdate);
                return null;
              });
    } catch (IOException ioe) {
      LOG.error(
          "Failed to update partition statistics for {} in metalake {}.",
          metadataObject,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  public List<PartitionStatistics> listPartitionStatistics(
      String metalake, MetadataObject metadataObject, PartitionRange range) {
    try {
      NameIdentifier identifier = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);

      List<PersistedPartitionStatistics> partitionStats =
          TreeLockUtils.doWithTreeLock(
              identifier,
              LockType.READ,
              () -> partitionStorage.listStatistics(metalake, metadataObject, range));

      List<PartitionStatistics> listedStats = Lists.newArrayList();

      if (partitionStats == null || partitionStats.isEmpty()) {
        return listedStats;
      }

      return partitionStats.stream()
          .map(
              partitionStat -> {
                String partitionName = partitionStat.partitionName();
                Statistic[] statistics =
                    partitionStat.statistics().stream()
                        .map(
                            entry -> {
                              String statName = entry.name();
                              StatisticValue<?> value = entry.value();
                              AuditInfo auditInfo = entry.auditInfo();
                              return new CustomStatistic(statName, value, auditInfo);
                            })
                        .toArray(Statistic[]::new);

                return new CustomPartitionStatistic(partitionName, statistics);
              })
          .collect(Collectors.toList());
    } catch (IOException ioe) {
      LOG.error(
          "Failed to list partition statistics for {} in metalake {}.",
          metadataObject,
          metalake,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public void close() throws IOException {
    partitionStorage.close();
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

  @VisibleForTesting
  public static class CustomPartitionStatistic implements PartitionStatistics {

    private final String partitionName;
    private final Statistic[] statistics;

    public CustomPartitionStatistic(String partitionName, Statistic[] statistics) {
      this.partitionName = partitionName;
      this.statistics = statistics;
    }

    @Override
    public String partitionName() {
      return partitionName;
    }

    @Override
    public Statistic[] statistics() {
      return statistics;
    }
  }
}
