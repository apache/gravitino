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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.maintenance.optimizer.common.util.IcebergSparkConfigUtils;
import org.apache.gravitino.maintenance.scheduler.job.cron.MaintenanceJobTemplates;

/**
 * Enqueue-time spec stored as JSON in db-scheduler {@code task_data} for {@code cron-job} and
 * {@code commit-job} rows.
 *
 * <p>Shape stays close to Iceberg update-stats {@code runJob} conf: each {@link Task} carries TMS
 * routing fields plus a flat {@code parameters} map (old template placeholders). Shared session
 * bags {@code updater_options} and {@code spark_conf} sit at the envelope top as JSON object
 * strings. Submit merges one task (cron) or all tasks (commit chain) into the flat {@code
 * Map<String,String>} passed to {@code runJob}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class MaintenanceJobSpec {

  private List<Task> tasks = new ArrayList<>();

  @JsonProperty("updater_options")
  private String updaterOptions;

  @JsonProperty("spark_conf")
  private String sparkConf;

  /** Jackson / empty constructor. */
  public MaintenanceJobSpec() {}

  /**
   * @param tasks maintenance tasks (order preserved)
   * @param updaterOptions JSON object string for {@code updater_options}
   * @param sparkConf JSON object string for {@code spark_conf}; may be null when empty
   */
  public MaintenanceJobSpec(
      List<Task> tasks, @Nullable String updaterOptions, @Nullable String sparkConf) {
    this.tasks = new ArrayList<>(Preconditions.checkNotNull(tasks, "tasks"));
    this.updaterOptions = updaterOptions;
    this.sparkConf = sparkConf;
  }

  /**
   * @return maintenance tasks
   */
  public List<Task> getTasks() {
    return tasks;
  }

  /**
   * @param tasks maintenance tasks
   */
  public void setTasks(List<Task> tasks) {
    this.tasks = tasks == null ? new ArrayList<>() : new ArrayList<>(tasks);
  }

  /**
   * @return shared updater options JSON string
   */
  public String getUpdaterOptions() {
    return updaterOptions;
  }

  /**
   * @param updaterOptions shared updater options JSON string
   */
  public void setUpdaterOptions(String updaterOptions) {
    this.updaterOptions = updaterOptions;
  }

  /**
   * @return shared spark_conf JSON string
   */
  public String getSparkConf() {
    return sparkConf;
  }

  /**
   * @param sparkConf shared spark_conf JSON string
   */
  public void setSparkConf(String sparkConf) {
    this.sparkConf = sparkConf;
  }

  /**
   * @return unmodifiable table entity ids in task order (may contain duplicates across types)
   */
  @JsonIgnore
  public List<Long> tableIds() {
    if (tasks == null || tasks.isEmpty()) {
      return List.of();
    }
    return Collections.unmodifiableList(
        tasks.stream().map(Task::getTableId).collect(Collectors.toList()));
  }

  /**
   * Metalake for {@code runJob} / cancel, taken from {@code updater_options.metalake}.
   *
   * @return metalake name, or {@code null} when missing
   */
  @JsonIgnore
  @Nullable
  public String metalake() {
    if (updaterOptions == null || updaterOptions.isBlank()) {
      return null;
    }
    Map<String, String> options =
        IcebergSparkConfigUtils.parseFlatJsonMap(
            updaterOptions, MaintenanceJobTemplates.UPDATER_OPTIONS_JOB_CONF_KEY);
    String metalake = options.get("metalake");
    return metalake == null || metalake.isBlank() ? null : metalake.trim();
  }

  /**
   * @return true when tasks and shared updater_options look usable for submit
   */
  @JsonIgnore
  public boolean isValid() {
    return metalake() != null
        && updaterOptions != null
        && !updaterOptions.isBlank()
        && tasks != null
        && !tasks.isEmpty()
        && tasks.stream().allMatch(Task::isValid);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MaintenanceJobSpec)) {
      return false;
    }
    MaintenanceJobSpec that = (MaintenanceJobSpec) o;
    return Objects.equals(tasks, that.tasks)
        && Objects.equals(updaterOptions, that.updaterOptions)
        && Objects.equals(sparkConf, that.sparkConf);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tasks, updaterOptions, sparkConf);
  }

  @Override
  public String toString() {
    return "MaintenanceJobSpec{tasks="
        + (tasks == null ? 0 : tasks.size())
        + ", tableIds="
        + tableIds()
        + '}';
  }

  /**
   * One runnable maintenance unit (typically one {@code runJob} on the cron path; commit chains
   * pack several tasks into one Spark submit).
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static final class Task {

    private long tableId;
    @Nullable private Long actionId;
    private long policyId;
    private String maintenanceType;
    private Map<String, String> parameters = new LinkedHashMap<>();

    /** Jackson constructor. */
    public Task() {}

    /**
     * @param tableId table entity id
     * @param policyId policy entity id
     * @param maintenanceType Spark wire maintenance type
     * @param actionId soft-slot action id when known; null at enqueue
     * @param parameters flat template placeholders (old builtin jobConf keys)
     */
    public Task(
        long tableId,
        long policyId,
        String maintenanceType,
        @Nullable Long actionId,
        Map<String, String> parameters) {
      Preconditions.checkArgument(tableId > 0, "tableId must be positive");
      Preconditions.checkArgument(policyId > 0, "policyId must be positive");
      this.tableId = tableId;
      this.policyId = policyId;
      this.maintenanceType = Preconditions.checkNotNull(maintenanceType, "maintenanceType");
      this.actionId = actionId;
      this.parameters = new LinkedHashMap<>(Preconditions.checkNotNull(parameters, "parameters"));
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
     * @return soft-slot action id, or null before acquire
     */
    @Nullable
    public Long getActionId() {
      return actionId;
    }

    /**
     * @param actionId soft-slot action id
     */
    public void setActionId(@Nullable Long actionId) {
      this.actionId = actionId;
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

    /**
     * @return flat template placeholders
     */
    public Map<String, String> getParameters() {
      return parameters;
    }

    /**
     * @param parameters flat template placeholders
     */
    public void setParameters(Map<String, String> parameters) {
      this.parameters =
          parameters == null ? new LinkedHashMap<>() : new LinkedHashMap<>(parameters);
    }

    /**
     * @return Spark catalog name from {@code parameters.catalog_name}
     */
    @JsonIgnore
    @Nullable
    public String catalogName() {
      return blankToNull(parameters == null ? null : parameters.get("catalog_name"));
    }

    /**
     * @return {@code schema.table} from {@code parameters.table_identifier}
     */
    @JsonIgnore
    @Nullable
    public String tableIdentifier() {
      return blankToNull(parameters == null ? null : parameters.get("table_identifier"));
    }

    @JsonIgnore
    boolean isValid() {
      return tableId > 0
          && policyId > 0
          && maintenanceType != null
          && !maintenanceType.isBlank()
          && parameters != null
          && catalogName() != null
          && tableIdentifier() != null;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Task)) {
        return false;
      }
      Task that = (Task) o;
      return tableId == that.tableId
          && policyId == that.policyId
          && Objects.equals(actionId, that.actionId)
          && Objects.equals(maintenanceType, that.maintenanceType)
          && Objects.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tableId, actionId, policyId, maintenanceType, parameters);
    }

    private static @Nullable String blankToNull(@Nullable String value) {
      return value == null || value.isBlank() ? null : value.trim();
    }
  }
}
