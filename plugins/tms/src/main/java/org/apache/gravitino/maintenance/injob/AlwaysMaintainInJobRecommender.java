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
package org.apache.gravitino.maintenance.injob;

/**
 * Default cron in-job recommender: always maintain.
 *
 * <p>Cron expand already selected the table; the in-job gate still records {@code S0} and checks
 * snapshot drift. Type-specific threshold recommenders can replace this later.
 */
public final class AlwaysMaintainInJobRecommender implements InJobRecommender {

  /** Shared instance. */
  public static final AlwaysMaintainInJobRecommender INSTANCE =
      new AlwaysMaintainInJobRecommender();

  private AlwaysMaintainInJobRecommender() {}

  @Override
  public RecommendDecision recommend(
      long tableId, long policyId, long sampledSnapshotId, String metricsJson) {
    return RecommendDecision.MAINTAIN;
  }
}
