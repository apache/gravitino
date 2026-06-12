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

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.V2WriteCommand;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

/** Checks a resolved Spark plan for tables with missing Gravitino privileges. */
public class RequiredPrivilegesCheck extends AbstractFunction1<LogicalPlan, BoxedUnit> {

  @Override
  public BoxedUnit apply(LogicalPlan plan) {
    Map<String, Set<Privilege.Name>> requiredPrivileges = new TreeMap<>();
    ForbiddenException[] firstFailure = new ForbiddenException[1];

    plan.foreach(
        node -> {
          if (node instanceof DataSourceV2Relation) {
            collectRequiredPrivileges(
                ((DataSourceV2Relation) node).table(), requiredPrivileges, firstFailure);
          }
          if (node instanceof V2WriteCommand
              && ((V2WriteCommand) node).table() instanceof DataSourceV2Relation) {
            collectRequiredPrivileges(
                ((DataSourceV2Relation) ((V2WriteCommand) node).table()).table(),
                requiredPrivileges,
                firstFailure);
          }
          return BoxedUnit.UNIT;
        });

    if (!requiredPrivileges.isEmpty()) {
      String requirements =
          requiredPrivileges.entrySet().stream()
              .map(
                  entry ->
                      entry.getKey()
                          + ": "
                          + entry.getValue().stream()
                              .map(Privilege.Name::name)
                              .collect(Collectors.joining(", ")))
              .collect(Collectors.joining("; "));
      throw new ForbiddenException(
          firstFailure[0], "Missing required privileges for Spark tables: [%s]", requirements);
    }
    return BoxedUnit.UNIT;
  }

  private static void collectRequiredPrivileges(
      Table table,
      Map<String, Set<Privilege.Name>> requiredPrivileges,
      ForbiddenException[] firstFailure) {
    if (table instanceof SupportsRequiredPrivileges) {
      SupportsRequiredPrivileges deniedTable = (SupportsRequiredPrivileges) table;
      requiredPrivileges
          .computeIfAbsent(deniedTable.tableIdentifier(), ignored -> new TreeSet<>())
          .addAll(deniedTable.requiredPrivileges());
      if (firstFailure[0] == null) {
        firstFailure[0] = deniedTable.forbiddenException();
      }
    }
  }
}
