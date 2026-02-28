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

package org.apache.gravitino.maintenance.optimizer.api.recommender;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.Strategy;
import org.apache.gravitino.rel.Table;

/**
 * Immutable context handed to a {@link StrategyHandler}. Contains only the data requested through
 * {@link StrategyHandler#dataRequirements()} and safely copies any mutable collections.
 */
@DeveloperApi
public final class StrategyHandlerContext {
  private final NameIdentifier identifier;
  private final Strategy strategy;
  private final Optional<Table> tableMetadata;
  private final List<StatisticEntry<?>> tableStatistics;
  private final Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics;

  private StrategyHandlerContext(Builder builder) {
    this.identifier = builder.identifier;
    this.strategy = builder.strategy;
    this.tableMetadata = builder.tableMetadata;
    this.tableStatistics = builder.tableStatistics;
    this.partitionStatistics = builder.partitionStatistics;
  }

  /**
   * Fully qualified table identifier the strategy is evaluated against.
   *
   * @return target table identifier (catalog/schema/table)
   */
  public NameIdentifier nameIdentifier() {
    return identifier;
  }

  /**
   * Strategy definition that triggered the handler.
   *
   * @return recommender strategy
   */
  public Strategy strategy() {
    return strategy;
  }

  /**
   * Returns the table metadata if {@code DataRequirement.TABLE_METADATA} was requested in {@link
   * StrategyHandler#dataRequirements()}, otherwise returns {@link Optional#empty()}.
   *
   * @return the table metadata if requested, otherwise {@link Optional#empty()}
   */
  public Optional<Table> tableMetadata() {
    return tableMetadata;
  }

  /**
   * Returns table-level statistics if {@code DataRequirement.TABLE_STATISTICS} was requested,
   * otherwise returns an empty list.
   */
  public List<StatisticEntry<?>> tableStatistics() {
    return tableStatistics;
  }

  /**
   * Returns partition-level statistics if {@code DataRequirement.PARTITION_STATISTICS} was
   * requested, otherwise returns an empty map.
   *
   * @return map of partition path to statistics; empty when not requested or unavailable
   */
  public Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics() {
    return partitionStatistics;
  }

  /**
   * Create a new builder for a handler context with the required identifier and strategy.
   *
   * @param identifier target table identifier for the strategy handler (catalog/schema/table)
   * @param strategy recommender strategy definition
   * @return a builder to populate optional metadata and statistics
   */
  public static Builder builder(NameIdentifier identifier, Strategy strategy) {
    return new Builder(identifier, strategy);
  }

  public static final class Builder {
    private final NameIdentifier identifier;
    private final Strategy strategy;
    private Optional<Table> tableMetadata = Optional.empty();
    private List<StatisticEntry<?>> tableStatistics = Collections.emptyList();
    private Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics =
        Collections.emptyMap();

    private Builder(NameIdentifier identifier, Strategy strategy) {
      this.identifier = Objects.requireNonNull(identifier, "identifier must not be null");
      this.strategy = Objects.requireNonNull(strategy, "strategy must not be null");
    }

    /**
     * Attach table metadata when the handler requests {@code TABLE_METADATA}.
     *
     * @param metadata table definition; null clears metadata
     * @return builder for chaining
     */
    public Builder withTableMetadata(Table metadata) {
      this.tableMetadata = Optional.ofNullable(metadata);
      return this;
    }

    /**
     * Attach table-level statistics when the handler requests {@code TABLE_STATISTICS}.
     *
     * @param statistics statistics list; null treated as empty
     * @return builder for chaining
     */
    public Builder withTableStatistics(List<StatisticEntry<?>> statistics) {
      this.tableStatistics = statistics == null ? Collections.emptyList() : List.copyOf(statistics);
      return this;
    }

    /**
     * Attach partition-level statistics when the handler requests {@code PARTITION_STATISTICS}.
     *
     * @param partitionStatistics statistics map; null treated as empty
     * @return builder for chaining
     */
    public Builder withPartitionStatistics(
        Map<PartitionPath, List<StatisticEntry<?>>> partitionStatistics) {
      this.partitionStatistics =
          partitionStatistics == null ? Collections.emptyMap() : Map.copyOf(partitionStatistics);
      return this;
    }

    /**
     * Build an immutable {@link StrategyHandlerContext} instance.
     *
     * @return constructed handler context
     */
    public StrategyHandlerContext build() {
      return new StrategyHandlerContext(this);
    }
  }
}
