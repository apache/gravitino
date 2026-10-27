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

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.spark.sql.catalyst.analysis.NamedRelation;
import org.apache.spark.sql.catalyst.analysis.ResolvedTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.V2WriteCommand;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import scala.collection.Iterator;

/**
 * A post-hoc resolution rule that reports the tables a query lacks Gravitino privileges for.
 *
 * <p>It runs after Spark's {@code ResolveRelations}, which leaves an {@link AuthorizationTable}
 * placeholder in the plan for each denied table, and before {@code checkAnalysis} would fail the
 * query for an unrelated reason (such as a column that cannot be resolved against a placeholder's
 * empty schema). It lists the denied tables of the plan and their required privileges in one error.
 * A subquery is analyzed separately, so its denied tables are reported on their own.
 */
public class RequiredPrivilegesCheck extends Rule<LogicalPlan> {

  @Override
  public LogicalPlan apply(LogicalPlan plan) {
    Optional<ForbiddenException> failure = AuthorizationTable.aggregateFailure(collectDenied(plan));
    if (failure.isPresent()) {
      throw failure.get();
    }
    return plan;
  }

  private static List<AuthorizationTable> collectDenied(LogicalPlan plan) {
    List<AuthorizationTable> denied = new LinkedList<>();
    collectDenied(plan, denied);
    return denied;
  }

  private static void collectDenied(LogicalPlan plan, List<AuthorizationTable> denied) {
    if (plan instanceof DataSourceV2Relation) {
      addIfDenied(((DataSourceV2Relation) plan).table(), denied);
    } else if (plan instanceof ResolvedTable) {
      // Commands such as DESCRIBE TABLE and SHOW CREATE TABLE resolve their table to this node.
      addIfDenied(((ResolvedTable) plan).table(), denied);
    } else if (plan instanceof StreamingRelationV2) {
      // Streaming sources resolve to this leaf node instead of DataSourceV2Relation.
      addIfDenied(((StreamingRelationV2) plan).table(), denied);
    } else if (plan instanceof V2WriteCommand) {
      // Write commands keep the target relation in their table() field, outside children.
      NamedRelation writeTarget = ((V2WriteCommand) plan).table();
      if (writeTarget instanceof DataSourceV2Relation) {
        addIfDenied(((DataSourceV2Relation) writeTarget).table(), denied);
      }
    }
    Iterator<LogicalPlan> children = plan.children().iterator();
    while (children.hasNext()) {
      collectDenied(children.next(), denied);
    }
  }

  private static void addIfDenied(Table table, List<AuthorizationTable> denied) {
    if (table instanceof AuthorizationTable) {
      denied.add((AuthorizationTable) table);
    }
  }
}
