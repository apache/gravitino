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

package org.apache.gravitino.iceberg.service.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundLiteralPredicate;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.types.Type;

/**
 * Converts an Iceberg expression JSON string into the proposed read-restrictions response shape.
 */
public final class ReadRestrictionExpressionDemo {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private ReadRestrictionExpressionDemo() {}

  /**
   * Parses an Iceberg 1.11 expression JSON string and binds its column name to a schema field ID.
   *
   * @param expressionJson a string equality predicate in Iceberg 1.11 JSON format
   * @param schema the table schema used to resolve the column name
   * @return a JSON fragment containing the proposed {@code read-restrictions} field
   */
  public static String toReadRestrictions(String expressionJson, Schema schema) {
    Expression expression = ExpressionParser.fromJson(expressionJson, schema);
    Expression boundExpression = Binder.bind(schema.asStruct(), expression, true);
    if (boundExpression.op() != Expression.Operation.EQ
        || !(boundExpression instanceof BoundLiteralPredicate)) {
      throw new IllegalArgumentException("Demo supports only string equality predicates");
    }

    BoundLiteralPredicate<?> predicate = (BoundLiteralPredicate<?>) boundExpression;
    if (!(predicate.term() instanceof BoundReference)
        || predicate.ref().type().typeId() != Type.TypeID.STRING
        || !(predicate.literal().value() instanceof CharSequence)) {
      throw new IllegalArgumentException("Demo supports only direct string column references");
    }

    ObjectNode reference = MAPPER.createObjectNode();
    reference.put("type", "reference");
    reference.put("id", predicate.ref().fieldId());

    ObjectNode literal = MAPPER.createObjectNode();
    literal.put("type", "literal");
    literal.put("value", predicate.literal().value().toString());

    ObjectNode rowFilter = MAPPER.createObjectNode();
    rowFilter.put("type", "eq");
    rowFilter.set("left", reference);
    rowFilter.set("right", literal);

    ObjectNode restrictions = MAPPER.createObjectNode();
    restrictions.set("required-row-filter", rowFilter);
    ObjectNode response = MAPPER.createObjectNode();
    response.set("read-restrictions", restrictions);
    return response.toString();
  }
}
