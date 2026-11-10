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
package org.apache.gravitino.storage.relational.database;

import java.lang.reflect.Method;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestH2Database {

  @Test
  public void testGetStoragePathWithRelativePathWhenGravitinoHomeMissing() throws Exception {
    String javaBin = System.getProperty("java.home") + "/bin/java";
    String classPath = System.getProperty("java.class.path");
    String className =
        "org.apache.gravitino.storage.relational.database.H2DatabaseStoragePathProbe";

    ProcessBuilder pb = new ProcessBuilder(javaBin, "-cp", classPath, className);
    pb.environment().remove("GRAVITINO_HOME");
    Process process = pb.start();
    int exitCode = process.waitFor();

    Assertions.assertEquals(
        1,
        exitCode,
        "Expected relative storage path resolution to fail when GRAVITINO_HOME is unset");
  }
}

class H2DatabaseStoragePathProbe {
  public static void main(String[] args) {
    try {
      Config config = new Config(false) {};
      config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PATH, "relative-db-path");

      Method getStoragePath = H2Database.class.getDeclaredMethod("getStoragePath", Config.class);
      getStoragePath.setAccessible(true);
      getStoragePath.invoke(null, config);
      System.exit(0);
    } catch (Exception e) {
      if (e.getCause() instanceof IllegalArgumentException
          && e.getCause().getMessage().contains("GRAVITINO_HOME")) {
        System.exit(1);
      }
      System.exit(2);
    }
  }
}
