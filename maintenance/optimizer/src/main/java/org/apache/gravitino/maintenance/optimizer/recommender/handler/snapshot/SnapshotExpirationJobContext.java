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
package org.apache.gravitino.maintenance.optimizer.recommender.handler.snapshot;

import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.recommender.JobExecutionContext;

/** Immutable target and options for table-level snapshot expiration. */
public class SnapshotExpirationJobContext implements JobExecutionContext {
  private final NameIdentifier name;
  private final Map<String, String> jobOptions;
  private final String jobTemplateName;

  /**
   * Creates the context from table metadata and policy options.
   *
   * @param name normalized catalog.schema.table identifier
   * @param jobOptions policy options
   * @param jobTemplateName template to submit
   */
  public SnapshotExpirationJobContext(
      NameIdentifier name, Map<String, String> jobOptions, String jobTemplateName) {
    this.name = name;
    this.jobOptions = Map.copyOf(jobOptions);
    this.jobTemplateName = jobTemplateName;
  }

  @Override
  public NameIdentifier nameIdentifier() {
    return name;
  }

  @Override
  public Map<String, String> jobOptions() {
    return jobOptions;
  }

  @Override
  public String jobTemplateName() {
    return jobTemplateName;
  }
}
