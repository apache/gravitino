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

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.CodeSource;
import org.apache.gravitino.job.JobTemplate;

/** Contract for built-in jobs to expose their job template definitions. */
public interface BuiltInJob {

  /** Returns the built-in job template provided by this job. */
  JobTemplate jobTemplate();

  /** Resolve the executable jar that hosts the built-in jobs. */
  default String resolveExecutable(Class<?> cls) {
    try {
      CodeSource codeSource = cls.getProtectionDomain().getCodeSource();
      if (codeSource == null || codeSource.getLocation() == null) {
        throw new RuntimeException("Failed to resolve built-in jobs jar location");
      }

      Path path = Paths.get(codeSource.getLocation().toURI());
      return path.toAbsolutePath().toString();
    } catch (URISyntaxException e) {
      throw new RuntimeException("Failed to resolve built-in jobs jar location", e);
    }
  }
}
