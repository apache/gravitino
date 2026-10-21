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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Enqueue-time snapshot stored as JSON in db-scheduler {@code task_data} for {@code cron-job} and
 * {@code commit-job} rows.
 *
 * <p>Thin envelope only: {@link #jobConf} is the same {@code Map<String,String>} passed to {@code
 * runJob} (including {@code updater_options} as a JSON string). Tables carry {@code tableId} plus
 * NameIdentifier levels; steps carry {@code policyId} plus the wire maintenance type.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class MaintenanceJobSnapshot {

  /** Current snapshot schema version. */
  public static final int CURRENT_VERSION = 1;

  private int version = CURRENT_VERSION;
  private String metalake;
  private List<TableTarget> tables = new ArrayList<>();
  private List<ChainStep> steps = new ArrayList<>();
  private Map<String, String> jobConf = new LinkedHashMap<>();

  /** Jackson / empty constructor. */
  public MaintenanceJobSnapshot() {}

  /**
   * @param metalake metalake name
   * @param tables table targets (order preserved)
   * @param steps chain / cron steps (order preserved)
   * @param jobConf runJob configuration snapshot
   */
  public MaintenanceJobSnapshot(
      String metalake,
      List<TableTarget> tables,
      List<ChainStep> steps,
      Map<String, String> jobConf) {
    this.version = CURRENT_VERSION;
    this.metalake = Preconditions.checkNotNull(metalake, "metalake");
    this.tables = new ArrayList<>(Preconditions.checkNotNull(tables, "tables"));
    this.steps = new ArrayList<>(Preconditions.checkNotNull(steps, "steps"));
    this.jobConf = new LinkedHashMap<>(Preconditions.checkNotNull(jobConf, "jobConf"));
  }

  /**
   * @return schema version
   */
  public int getVersion() {
    return version;
  }

  /**
   * @param version schema version
   */
  public void setVersion(int version) {
    this.version = version;
  }

  /**
   * @return metalake name
   */
  public String getMetalake() {
    return metalake;
  }

  /**
   * @param metalake metalake name
   */
  public void setMetalake(String metalake) {
    this.metalake = metalake;
  }

  /**
   * @return table targets
   */
  public List<TableTarget> getTables() {
    return tables;
  }

  /**
   * @param tables table targets
   */
  public void setTables(List<TableTarget> tables) {
    this.tables = tables == null ? new ArrayList<>() : new ArrayList<>(tables);
  }

  /**
   * @return maintenance steps
   */
  public List<ChainStep> getSteps() {
    return steps;
  }

  /**
   * @param steps maintenance steps
   */
  public void setSteps(List<ChainStep> steps) {
    this.steps = steps == null ? new ArrayList<>() : new ArrayList<>(steps);
  }

  /**
   * @return runJob configuration
   */
  public Map<String, String> getJobConf() {
    return jobConf;
  }

  /**
   * @param jobConf runJob configuration
   */
  public void setJobConf(Map<String, String> jobConf) {
    this.jobConf = jobConf == null ? new LinkedHashMap<>() : new LinkedHashMap<>(jobConf);
  }

  /**
   * @return unmodifiable table entity ids in snapshot order
   */
  @JsonIgnore
  public List<Long> tableIds() {
    if (tables == null || tables.isEmpty()) {
      return List.of();
    }
    return Collections.unmodifiableList(
        tables.stream().map(TableTarget::getTableId).collect(Collectors.toList()));
  }

  /**
   * @return true when version, metalake, tables, steps, and jobConf look usable for submit
   */
  @JsonIgnore
  public boolean isValid() {
    return version == CURRENT_VERSION
        && metalake != null
        && !metalake.isBlank()
        && tables != null
        && !tables.isEmpty()
        && steps != null
        && !steps.isEmpty()
        && jobConf != null
        && !jobConf.isEmpty()
        && steps.get(0).getPolicyId() > 0
        && tables.stream().allMatch(TableTarget::isValid)
        && steps.stream().allMatch(ChainStep::isValid);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MaintenanceJobSnapshot)) {
      return false;
    }
    MaintenanceJobSnapshot that = (MaintenanceJobSnapshot) o;
    return version == that.version
        && Objects.equals(metalake, that.metalake)
        && Objects.equals(tables, that.tables)
        && Objects.equals(steps, that.steps)
        && Objects.equals(jobConf, that.jobConf);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, metalake, tables, steps, jobConf);
  }

  @Override
  public String toString() {
    return "MaintenanceJobSnapshot{version="
        + version
        + ", metalake='"
        + metalake
        + "', tables="
        + tableIds()
        + ", steps="
        + (steps == null ? 0 : steps.size())
        + ", jobConfKeys="
        + (jobConf == null ? 0 : jobConf.size())
        + '}';
  }

  /** One table covered by this snapshot. */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class TableTarget {

    private long tableId;
    private List<String> identifier = new ArrayList<>();

    /** Jackson constructor. */
    public TableTarget() {}

    /**
     * @param tableId table entity id
     * @param identifier NameIdentifier levels (metalake, catalog, …, table)
     */
    public TableTarget(long tableId, List<String> identifier) {
      Preconditions.checkArgument(tableId > 0, "tableId must be positive");
      this.tableId = tableId;
      this.identifier = new ArrayList<>(Preconditions.checkNotNull(identifier, "identifier"));
    }

    /**
     * @return table entity id
     */
    public long getTableId() {
      return tableId;
    }

    /**
     * @param tableId table entity id
     */
    public void setTableId(long tableId) {
      this.tableId = tableId;
    }

    /**
     * @return NameIdentifier levels
     */
    public List<String> getIdentifier() {
      return identifier;
    }

    /**
     * @param identifier NameIdentifier levels
     */
    public void setIdentifier(List<String> identifier) {
      this.identifier = identifier == null ? new ArrayList<>() : new ArrayList<>(identifier);
    }

    /**
     * @return Spark catalog name ({@code identifier[1]})
     */
    @JsonIgnore
    @Nullable
    public String catalogName() {
      return identifier != null && identifier.size() >= 2 ? identifier.get(1) : null;
    }

    /**
     * @return {@code schema.table} (or nested) Spark table identifier
     */
    @JsonIgnore
    @Nullable
    public String sparkTableIdentifier() {
      if (identifier == null || identifier.size() < 3) {
        return null;
      }
      return String.join(".", identifier.subList(2, identifier.size()));
    }

    @JsonIgnore
    boolean isValid() {
      return tableId > 0
          && identifier != null
          && identifier.size() >= 3
          && identifier.stream().noneMatch(part -> part == null || part.isBlank());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TableTarget)) {
        return false;
      }
      TableTarget that = (TableTarget) o;
      return tableId == that.tableId && Objects.equals(identifier, that.identifier);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tableId, identifier);
    }
  }

  /** One maintenance step (cron: single type; commit: chain member). */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class ChainStep {

    private long policyId;
    private String maintenanceType;

    /** Jackson constructor. */
    public ChainStep() {}

    /**
     * @param policyId policy entity id
     * @param maintenanceType Spark wire name
     */
    public ChainStep(long policyId, String maintenanceType) {
      Preconditions.checkArgument(policyId > 0, "policyId must be positive");
      this.policyId = policyId;
      this.maintenanceType = Preconditions.checkNotNull(maintenanceType, "maintenanceType");
    }

    /**
     * @return policy entity id
     */
    public long getPolicyId() {
      return policyId;
    }

    /**
     * @param policyId policy entity id
     */
    public void setPolicyId(long policyId) {
      this.policyId = policyId;
    }

    /**
     * @return Spark wire maintenance type
     */
    public String getMaintenanceType() {
      return maintenanceType;
    }

    /**
     * @param maintenanceType Spark wire maintenance type
     */
    public void setMaintenanceType(String maintenanceType) {
      this.maintenanceType = maintenanceType;
    }

    @JsonIgnore
    boolean isValid() {
      return policyId > 0 && maintenanceType != null && !maintenanceType.isBlank();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ChainStep)) {
        return false;
      }
      ChainStep that = (ChainStep) o;
      return policyId == that.policyId && Objects.equals(maintenanceType, that.maintenanceType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(policyId, maintenanceType);
    }
  }
}
