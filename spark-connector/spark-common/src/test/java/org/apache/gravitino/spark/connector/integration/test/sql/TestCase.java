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
import java.nio.file.Paths;
import lombok.Getter;
import lombok.ToString;

/** A test SQL files which contains multi SQL queries. */
@Getter
@ToString
public final class TestCase {
  private final Path testFile;

  public TestCase(Path testFile) {
    this.testFile = testFile;
  }

  // The SQL output to check the correctness the SQL result, The output file of '/a/b.sql' is
  // '/a/b.sql.out'
  public Path getTestOutputFile() {
    String fileName = testFile.getFileName().toString();
    String outputFileName = fileName + ".out";
    Path parentPath = testFile.getParent();
    if (parentPath == null) {
      return Paths.get(outputFileName);
    }
    return parentPath.resolve(outputFileName);
  }
}
