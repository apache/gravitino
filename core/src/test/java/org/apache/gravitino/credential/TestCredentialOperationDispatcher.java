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
package org.apache.gravitino.credential;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.catalog.TestOperationDispatcher;
import org.apache.gravitino.connector.credential.PathContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCredentialOperationDispatcher extends TestOperationDispatcher {

  @Test
  public void testMergePathContextsWithSameCredentialType() {
    List<PathContext> pathContexts =
        Arrays.asList(new PathContext("path1", "dummy"), new PathContext("path2", "dummy"));

    Map<String, CredentialContext> contexts =
        CredentialOperationDispatcher.getPathBasedCredentialContexts(
            CredentialPrivilege.WRITE, pathContexts);

    Assertions.assertEquals(1, contexts.size());
    PathBasedCredentialContext context = (PathBasedCredentialContext) contexts.get("dummy");
    Assertions.assertEquals(Set.of("path1", "path2"), context.getWritePaths());
    Assertions.assertTrue(context.getReadPaths().isEmpty());
  }

  @Test
  public void testMergePathContextsWithSameCredentialTypeAndSamePath() {
    List<PathContext> pathContexts =
        Arrays.asList(new PathContext("path1", "dummy"), new PathContext("path1", "dummy"));

    Map<String, CredentialContext> contexts =
        CredentialOperationDispatcher.getPathBasedCredentialContexts(
            CredentialPrivilege.WRITE, pathContexts);

    Assertions.assertEquals(1, contexts.size());
    PathBasedCredentialContext context = (PathBasedCredentialContext) contexts.get("dummy");
    Assertions.assertEquals(Set.of("path1"), context.getWritePaths());
    Assertions.assertTrue(context.getReadPaths().isEmpty());
  }
}
