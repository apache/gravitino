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

import org.apache.spark.sql.SparkSessionExtensions;
import org.apache.spark.sql.catalyst.FunctionIdentifier;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import scala.Function1;
import scala.collection.Seq;

/** Registers Gravitino authorization checks with a Spark session. */
public class GravitinoAuthorizationSparkSessionExtensions
    implements Function1<SparkSessionExtensions, Void> {

  @Override
  public Void apply(SparkSessionExtensions extensions) {
    // Post-hoc resolution runs after every relation is resolved but before checkAnalysis, so all
    // denied tables are reported together rather than failing on the first resolution error.
    extensions.injectPostHocResolutionRule(session -> new RequiredPrivilegesCheck());
    extensions.injectParser((session, parser) -> new AuthorizationParser(parser));
    return null;
  }

  private static class AuthorizationParser implements ParserInterface {
    private final ParserInterface delegate;

    private AuthorizationParser(ParserInterface delegate) {
      this.delegate = delegate;
    }

    @Override
    public LogicalPlan parsePlan(String sqlText) throws ParseException {
      AuthorizationTable.clear();
      return delegate.parsePlan(sqlText);
    }

    @Override
    public Expression parseExpression(String sqlText) throws ParseException {
      return delegate.parseExpression(sqlText);
    }

    @Override
    public TableIdentifier parseTableIdentifier(String sqlText) throws ParseException {
      return delegate.parseTableIdentifier(sqlText);
    }

    @Override
    public FunctionIdentifier parseFunctionIdentifier(String sqlText) throws ParseException {
      return delegate.parseFunctionIdentifier(sqlText);
    }

    @Override
    public Seq<String> parseMultipartIdentifier(String sqlText) throws ParseException {
      return delegate.parseMultipartIdentifier(sqlText);
    }

    @Override
    public LogicalPlan parseQuery(String sqlText) throws ParseException {
      AuthorizationTable.clear();
      return delegate.parseQuery(sqlText);
    }

    @Override
    public StructType parseTableSchema(String sqlText) throws ParseException {
      return delegate.parseTableSchema(sqlText);
    }

    @Override
    public DataType parseDataType(String sqlText) throws ParseException {
      return delegate.parseDataType(sqlText);
    }
  }
}
