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

package org.apache.gravitino.spark.connector.authorization;

import java.util.Optional;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.Rule;

/**
 * A post-hoc resolution rule that reports the tables a query lacks Gravitino privileges for.
 *
 * <p>It runs after Spark's {@code ResolveRelations} has resolved every relation in the query, so
 * all denied tables have been collected by {@link AuthorizationTable#deny}, but before {@code
 * checkAnalysis} would fail the query for an unrelated reason (such as a column that cannot be
 * resolved against a denied table's placeholder schema). This lets a single error list every
 * inaccessible table and its required privileges instead of failing on the first one.
 */
public class RequiredPrivilegesCheck extends Rule<LogicalPlan> {

  @Override
  public LogicalPlan apply(LogicalPlan plan) {
    Optional<ForbiddenException> failure = AuthorizationTable.drainFailure();
    if (failure.isPresent()) {
      throw failure.get();
    }
    return plan;
  }
}
