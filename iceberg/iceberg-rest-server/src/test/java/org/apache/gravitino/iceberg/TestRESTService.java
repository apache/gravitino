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
package org.apache.gravitino.iceberg;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

public class TestRESTService {

  /**
   * RESTService.initServer() previously registered /metrics and /prometheus/metrics (added by
   * JettyServer#initialize() itself, outside ICEBERG_SPEC) with no audit coverage at all, and
   * nothing in the build caught it. This test pins the fix by inspecting the source directly, since
   * RESTService's initServer() has too many external dependencies (catalog backends, config
   * providers) to boot in a plain unit test. See GH-12760.
   */
  @Test
  public void testMetricsPathsHaveAuditFilterCoverage() throws IOException {
    Path sourceFile = Path.of("src/main/java/org/apache/gravitino/iceberg/RESTService.java");
    String source = Files.readString(sourceFile);

    int loopStart = source.indexOf("for (String pathSpec : METRICS_PATHS)");
    assertTrue(
        loopStart >= 0,
        "RESTService must wire filters onto every path in METRICS_PATHS, see GH-12760");

    int loopEnd = source.indexOf("}", loopStart);
    assertTrue(loopEnd > loopStart, "Malformed METRICS_PATHS filter loop");

    String loopBody = source.substring(loopStart, loopEnd);
    assertTrue(
        loopBody.contains("HttpAuditFilter"),
        "The METRICS_PATHS filter loop must bind HttpAuditFilter so /metrics and "
            + "/prometheus/metrics get audit-on-failure coverage, see GH-12760");
  }
}
