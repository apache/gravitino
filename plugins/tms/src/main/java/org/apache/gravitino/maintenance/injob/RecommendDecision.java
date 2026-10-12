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
 * Whether the in-job path should run maintenance work after a successful {@code S0} sample
 * (§5.2.2).
 *
 * <p>Type-specific strategy recommenders are wired in a follow-up subtask; this enum only models
 * the gate outcome.
 */
public enum RecommendDecision {
  /** Proceed to execute maintenance work. */
  MAINTAIN,
  /** End this job or type early; do not run maintenance work. */
  SKIP
}
