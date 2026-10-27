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

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests {@link RunNames} without Docker. */
public class TestRunNames {

  // A host name label: lowercase letters, digits and inner hyphens, at most 63 characters.
  private static final Pattern HOST_LABEL = Pattern.compile("[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?");

  @Test
  void testRunIdIsStableAndShort() {
    Assertions.assertEquals(RunNames.runId(), RunNames.runId());
    Assertions.assertTrue(RunNames.runId().matches("[0-9a-f]{8}"), RunNames.runId());
  }

  @Test
  void testContainerNamesCarryTheRunIdAndDiffer() {
    String hive = RunNames.containerName("gravitino-ci-hive");
    String again = RunNames.containerName("gravitino-ci-hive");

    Assertions.assertTrue(hive.startsWith("gravitino-hive-" + RunNames.runId() + "-"), hive);
    Assertions.assertNotEquals(hive, again);
    Assertions.assertTrue(HOST_LABEL.matcher(hive).matches(), hive);
  }

  @Test
  void testOtherRunsGetOtherNames() {
    Assertions.assertEquals(
        "gravitino-mysql-aaaa1111-1", RunNames.containerName("aaaa1111", "gravitino-ci-mysql", 1));
    Assertions.assertNotEquals(
        RunNames.containerName("aaaa1111", "gravitino-ci-mysql", 1),
        RunNames.containerName("bbbb2222", "gravitino-ci-mysql", 1));
  }

  @Test
  void testNamesAreValidHostNamesWithoutUnderscores() {
    String[] hostNames = {
      "gravitino-ci-kerberos-hive",
      "gravitino-ci-doris-doris3",
      "trino-worker-1",
      "Some_Service.Name",
      "___",
      "gravitino-ci-clickhouse-cluster-with-a-very-long-name-that-does-not-fit-in-one-label-1"
    };
    Set<String> names = new HashSet<>();
    for (String hostName : hostNames) {
      String name = RunNames.containerName("0123abcd", hostName, 12345);
      Assertions.assertTrue(HOST_LABEL.matcher(name).matches(), name);
      Assertions.assertFalse(name.contains("_"), name);
      Assertions.assertTrue(name.endsWith("-0123abcd-12345"), name);
      Assertions.assertTrue(names.add(name), name);
    }
  }

  @Test
  void testServiceLabelDropsTheOldPrefixes() {
    Assertions.assertEquals("kerberos-hive", RunNames.serviceLabel("gravitino-ci-kerberos-hive"));
    Assertions.assertEquals("hive", RunNames.serviceLabel("hive"));
    Assertions.assertEquals("some-service-name", RunNames.serviceLabel("Some_Service.Name"));
    Assertions.assertEquals("container", RunNames.serviceLabel("___"));
  }

  @Test
  void testNetworkAndComposeNames() {
    String network = RunNames.networkName("ci-network");

    Assertions.assertTrue(
        network.startsWith("gravitino-ci-network-" + RunNames.runId() + "-"), network);
    Assertions.assertNotEquals(network, RunNames.networkName("ci-network"));
    Assertions.assertEquals(
        "gravitino-doris-" + RunNames.runId() + "-", RunNames.composeIdentifier("doris"));
  }
}
