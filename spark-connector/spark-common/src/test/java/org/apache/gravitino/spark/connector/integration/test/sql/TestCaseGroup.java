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

import java.nio.file.Path;
import java.util.List;
import javax.annotation.Nullable;
import lombok.Getter;

/**
 * A group of test SQL files in same directory which belongs to one catalog and may contain
 * prepare.sql to init or cleanup.sql to clean.
 */
@Getter
public class TestCaseGroup {

  List<TestCase> testCases;
  @Nullable Path prepareFile;
  @Nullable Path cleanupFile;
  CatalogType catalogType;

  public TestCaseGroup(
      List<TestCase> testCases, Path prepareFile, Path cleanupFile, CatalogType catalogType) {
    this.testCases = testCases;
    this.prepareFile = prepareFile;
    this.cleanupFile = cleanupFile;
    this.catalogType = catalogType;
  }
}
