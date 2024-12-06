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
package org.apache.gravitino.spark.connector.integration.test.paimon;

import org.apache.gravitino.spark.connector.integration.test.SparkCommonIT;
import org.junit.jupiter.api.Test;

public abstract class SparkPaimonCatalogIT extends SparkCommonIT {

  @Override
  protected String getCatalogName() {
    return "paimon";
  }

  @Override
  protected String getProvider() {
    return "lakehouse-paimon";
  }

  @Override
  protected boolean supportsSparkSQLClusteredBy() {
    return false;
  }

  @Override
  protected boolean supportsPartition() {
    return false;
  }

  @Override
  protected boolean supportsDelete() {
    return false;
  }

  @Override
  protected boolean supportsSchemaEvolution() {
    return false;
  }

  @Test
  @Override
  protected void testListTables() {
    // TODO: implement table operations.
  }

  @Test
  @Override
  protected void testCreateSimpleTable() {
    // TODO: implement table operations.
  }

  @Test
  @Override
  protected void testCreateTableWithDatabase() {
    // TODO: implement table operations.
  }

  @Test
  @Override
  protected void testCreateTableWithComment() {
    // TODO: implement table operations.
  }

  @Test
  @Override
  protected void testDropTable() {
    // TODO: implement table operations.
  }

  @Test
  @Override
  protected void testRenameTable() {
    // TODO: implement table operations.
  }

  @Test
  @Override
  protected void testListTable() {
    // TODO: implement table operations.
  }

  @Test
  @Override
  protected void testAlterTableSetAndRemoveProperty() {
    // TODO: implement table operations.
  }

  @Test
  @Override
  protected void testAlterTableUpdateComment() {
    // TODO: implement table operations.
  }

  @Test
  @Override
  protected void testAlterTableAddAndDeleteColumn() {
    // TODO: implement table operations.
  }

  @Test
  @Override
  protected void testAlterTableUpdateColumnType() {
    // TODO: implement table operations.
  }

  @Test
  @Override
  protected void testAlterTableRenameColumn() {
    // TODO: implement table operations.
  }

  @Test
  @Override
  protected void testUpdateColumnPosition() {
    // TODO: implement table operations.
  }

  @Test
  @Override
  protected void testAlterTableUpdateColumnComment() {
    // TODO: implement table operations.
  }

  @Test
  @Override
  protected void testAlterTableReplaceColumns() {
    // TODO: implement table operations.
  }

  @Test
  @Override
  protected void testComplexType() {
    // TODO: implement table operations.
  }

  @Test
  @Override
  protected void testCreateTableAsSelect() {
    // TODO: implement table operations.
  }

  @Test
  @Override
  protected void testInsertTableAsSelect() {
    // TODO: implement table operations.
  }

  @Test
  @Override
  protected void testTableOptions() {
    // TODO: implement table operations.
  }

  @Test
  @Override
  protected void testDropAndWriteTable() {
    // TODO: implement table operations.
  }
}
