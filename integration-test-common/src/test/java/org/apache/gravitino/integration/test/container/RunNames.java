/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.integration.test.container;

import com.google.common.annotations.VisibleForTesting;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Docker names for one test run (one JVM), so two runs on one Docker host, such as two worktrees,
 * never pick the same container, network or Compose project name.
 *
 * <p>Every name has the form {@code gravitino-<service>-<runId>[-<n>]} and uses only lowercase
 * letters, digits and hyphens: Docker answers a reverse lookup of a container's IP with its name,
 * and Hadoop clients reject a host name with an underscore. Host names and network aliases inside
 * the network stay fixed; only the host-wide names carry the run id.
 */
final class RunNames {
  private static final String PREFIX = "gravitino-";
  // Strip the old prefixes so a name reads gravitino-hive-..., not gravitino-gravitino-ci-hive-...
  private static final String[] HOST_NAME_PREFIXES = {"gravitino-ci-", "gravitino-"};
  // A DNS label holds at most 63 characters.
  @VisibleForTesting static final int MAX_NAME_LENGTH = 63;
  private static final String RUN_ID = UUID.randomUUID().toString().substring(0, 8);
  private static final AtomicInteger SEQUENCE = new AtomicInteger();

  private RunNames() {}

  /**
   * Returns the run id: eight lowercase hexadecimal characters, generated once per JVM.
   *
   * @return the run id
   */
  static String runId() {
    return RUN_ID;
  }

  /**
   * Returns a new container name for this run. Each call returns another name, because one run may
   * start a service more than once (Hive2 and Hive3, or a class-scoped container).
   *
   * @param hostName the container's host name, which names the service
   * @return {@code gravitino-<service>-<runId>-<n>}
   */
  static String containerName(String hostName) {
    return containerName(RUN_ID, hostName, SEQUENCE.incrementAndGet());
  }

  /**
   * Returns a new network name for this run. Each call returns another name, because a replaced
   * Trino cluster may build its next network before the old one is gone.
   *
   * @param purpose what the network is for, such as {@code network} or {@code trino}
   * @return {@code gravitino-<purpose>-<runId>-<n>}
   */
  static String networkName(String purpose) {
    return containerName(RUN_ID, purpose, SEQUENCE.incrementAndGet());
  }

  /**
   * Returns the Compose identifier for this run. Testcontainers appends six random characters to
   * it, so two projects of one run still differ.
   *
   * @param service the service, such as {@code doris}
   * @return {@code gravitino-<service>-<runId>-}
   */
  static String composeIdentifier(String service) {
    return name(RUN_ID, service, "") + "-";
  }

  @VisibleForTesting
  static String containerName(String runId, String service, int sequence) {
    return name(runId, service, "-" + sequence);
  }

  private static String name(String runId, String service, String suffix) {
    String tail = "-" + runId + suffix;
    String label = serviceLabel(service);
    int room = MAX_NAME_LENGTH - PREFIX.length() - tail.length();
    if (label.length() > room) {
      label = trimHyphens(label.substring(0, room));
    }
    return PREFIX + label + tail;
  }

  @VisibleForTesting
  static String serviceLabel(String hostName) {
    String label = hostName.toLowerCase(Locale.ROOT);
    for (String prefix : HOST_NAME_PREFIXES) {
      if (label.startsWith(prefix) && label.length() > prefix.length()) {
        label = label.substring(prefix.length());
        break;
      }
    }
    label = trimHyphens(label.replaceAll("[^a-z0-9-]+", "-"));
    return label.isEmpty() ? "container" : label;
  }

  private static String trimHyphens(String value) {
    return value.replaceAll("^-+|-+$", "");
  }
}
