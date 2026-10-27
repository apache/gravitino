/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.spark.connector.hive;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.apache.spark.sql.catalyst.analysis.CTESubstitution;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.collection.JavaConverters;

/**
 * Qualifies the table names in a parsed view query with the namespace the view resolves in, the way
 * Spark expands names inside a view: a one-part name gets the catalog and the namespace, and a name
 * whose first part is not a registered catalog gets the catalog.
 */
@SuppressWarnings("deprecation") // JavaConverters is deprecated on Scala 2.13 but required on 2.12
final class ViewRelationQualifier {

  private final String catalog;
  private final List<String> namespace;
  private final Predicate<String> isCatalog;

  /**
   * @param catalog the catalog that unqualified names resolve in
   * @param namespace the namespace that one-part names resolve in
   * @param isCatalog tests whether a name is a catalog registered in the session
   */
  ViewRelationQualifier(String catalog, List<String> namespace, Predicate<String> isCatalog) {
    this.catalog = catalog;
    this.namespace = namespace;
    this.isCatalog = isCatalog;
  }

  /**
   * Returns the plan with its table names qualified. CTE references are substituted first, so a CTE
   * name is not taken for a table.
   *
   * @param plan the parsed, unresolved view query
   * @return the plan with qualified table names
   */
  LogicalPlan qualify(LogicalPlan plan) {
    return qualifyPlan(CTESubstitution.apply(plan));
  }

  private LogicalPlan qualifyPlan(LogicalPlan plan) {
    if (plan instanceof UnresolvedRelation) {
      return qualifyRelation((UnresolvedRelation) plan);
    }
    return plan.mapChildren(this::qualifyPlan).mapExpressions(this::qualifyExpression);
  }

  private Expression qualifyExpression(Expression expression) {
    Expression qualified = expression;
    if (expression instanceof SubqueryExpression) {
      SubqueryExpression subquery = (SubqueryExpression) expression;
      qualified = subquery.withNewPlan(qualifyPlan(subquery.plan()));
    }
    return qualified.mapChildren(this::qualifyExpression);
  }

  private LogicalPlan qualifyRelation(UnresolvedRelation relation) {
    List<String> parts = JavaConverters.seqAsJavaList(relation.multipartIdentifier());
    List<String> qualified = new ArrayList<>();
    if (parts.size() == 1) {
      qualified.add(catalog);
      qualified.addAll(namespace);
    } else if (isCatalog.test(parts.get(0))) {
      return relation;
    } else {
      qualified.add(catalog);
    }
    qualified.addAll(parts);
    return new UnresolvedRelation(
        JavaConverters.asScalaBuffer(qualified).toList(),
        relation.options(),
        relation.isStreaming());
  }
}
