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

package org.apache.gravitino.spark.connector.jdbc;

import org.apache.gravitino.spark.connector.jdbc.postgresql.GravitinoPostgreSqlCatalogSpark35;

/** Runs catalog option and converter regressions through the Spark 3.5 production catalog. */
public class TestGravitinoJdbcCatalog35 extends TestGravitinoJdbcCatalog34 {
  @Override
  protected boolean expectedTimestampNtzSupport() {
    return true;
  }

  @Override
  protected GravitinoJdbcCatalog newPostgreSqlCatalog() {
    return new GravitinoPostgreSqlCatalogSpark35();
  }

  @Override
  protected GravitinoJdbcCatalog newCatalog() {
    return new GravitinoJdbcCatalogSpark35();
  }
}
