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
package org.apache.gravitino.maintenance.jobs;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.gravitino.job.JobTemplate;
import org.junit.jupiter.api.Test;

public class TestBuiltInJob {

  private static class TestBuiltInJobImpl implements BuiltInJob {
    @Override
    public JobTemplate jobTemplate() {
      return null; // Not needed for this test
    }
  }

  @Test
  public void testResolveExecutableReturnsValidPath() {
    BuiltInJob job = new TestBuiltInJobImpl();
    String executable = job.resolveExecutable(TestBuiltInJobImpl.class);

    assertNotNull(executable, "Executable path should not be null");
    assertFalse(executable.trim().isEmpty(), "Executable path should not be empty");
  }

  @Test
  public void testResolveExecutablePointsToJarOrClassDirectory() {
    BuiltInJob job = new TestBuiltInJobImpl();
    String executable = job.resolveExecutable(TestBuiltInJobImpl.class);

    // Should end with .jar or be a directory path containing test classes
    assertTrue(
        executable.endsWith(".jar")
            || executable.contains("test-classes")
            || executable.contains("classes"),
        "Executable should point to a jar file or classes directory: " + executable);
  }
}
