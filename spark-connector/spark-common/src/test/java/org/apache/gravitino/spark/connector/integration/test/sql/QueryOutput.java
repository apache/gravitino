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
package org.apache.gravitino.spark.connector.integration.test.sql;

import lombok.Getter;

/** The SQL execution output, include schemas and output */
@Getter
public final class QueryOutput {
  private final String sql;
  private final String schema;
  private final String output;

  public QueryOutput(String sql, String schema, String output) {
    this.sql = sql;
    this.schema = schema;
    this.output = output;
  }

  @Override
  public String toString() {
    return "-- !query\n"
        + sql
        + "\n"
        + "-- !query schema\n"
        + schema
        + "\n"
        + "-- !query output\n"
        + output;
  }
}
