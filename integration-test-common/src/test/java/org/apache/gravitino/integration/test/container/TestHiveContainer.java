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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests, without Docker, how the Kerberos Hive creates a test's principals when it starts. */
public class TestHiveContainer {

  // The source of the Kerberos Hive image's start script.
  private static final Path KERBEROS_START_SCRIPT =
      Paths.get("..", "dev", "docker", "kerberos-hive", "start.sh");

  @Test
  void testKeytabPathFollowsThePrincipal() {
    Assertions.assertEquals("/cli_schema.keytab", HiveContainer.kerberosKeytab("cli_schema"));
  }

  @Test
  void testScriptCreatesAndChecksEveryPrincipal() {
    String script = HiveContainer.principalsScript(List.of("cli_fileset", "cli_schema"));

    Assertions.assertEquals(
        "set -e\n"
            + "kadmin.local -q 'addprinc -randkey cli_fileset@HADOOPKRB'\n"
            + "kadmin.local -q 'xst -norandkey -k /cli_fileset.keytab cli_fileset@HADOOPKRB'\n"
            + "klist -k /cli_fileset.keytab | grep -q ' cli_fileset@HADOOPKRB$'"
            + " || { echo 'No keytab for cli_fileset@HADOOPKRB at /cli_fileset.keytab' >&2;"
            + " exit 1; }\n"
            + "kadmin.local -q 'addprinc -randkey cli_schema@HADOOPKRB'\n"
            + "kadmin.local -q 'xst -norandkey -k /cli_schema.keytab cli_schema@HADOOPKRB'\n"
            + "klist -k /cli_schema.keytab | grep -q ' cli_schema@HADOOPKRB$'"
            + " || { echo 'No keytab for cli_schema@HADOOPKRB at /cli_schema.keytab' >&2;"
            + " exit 1; }\n",
        script);
  }

  @Test
  void testEntrypointHooksTheScriptIntoTheImageStartAndFailsLoudly() {
    String entrypoint = HiveContainer.principalsEntrypoint("/usr/local/sbin/start.sh");

    Assertions.assertEquals(
        "sed -i '/kinit hive\\//i bash /tmp/gravitino-kerberos-principals.sh"
            + " || { echo \"Cannot create the test Kerberos principals\" >&2; exit 1; }'"
            + " /usr/local/sbin/start.sh"
            + " && grep -q '^bash /tmp/gravitino-kerberos-principals.sh ' /usr/local/sbin/start.sh"
            + " || { echo 'Cannot hook the Kerberos principals into /usr/local/sbin/start.sh' >&2;"
            + " exit 1; }"
            + " && exec /bin/bash /usr/local/sbin/start.sh",
        entrypoint);
  }

  // The hook goes in before the one line that logs in as hive, which comes after the KDC database
  // exists and before HDFS starts, so readiness implies the principals exist.
  @Test
  void testHookLineFollowsTheKdcAndPrecedesHdfs() throws IOException {
    List<String> lines = Files.readAllLines(KERBEROS_START_SCRIPT, StandardCharsets.UTF_8);
    List<Integer> anchors = indexesOf(lines, HiveContainer.PRINCIPALS_HOOK_ANCHOR);

    Assertions.assertEquals(1, anchors.size(), "lines with the hook anchor");
    int anchor = anchors.get(0);
    Assertions.assertTrue(last(lines, "kdb5_util create") < anchor);
    Assertions.assertTrue(last(lines, "service krb5-kdc start") < anchor);
    Assertions.assertTrue(last(lines, "kadmin.local") < anchor);
    Assertions.assertTrue(anchor < indexesOf(lines, "namenode -format").get(0));
  }

  @Test
  void testPrincipalsNeedKerberos() {
    HiveContainer.Builder builder =
        HiveContainer.builder().withKerberosPrincipals(List.of("cli_schema"));

    Assertions.assertThrows(IllegalArgumentException.class, builder::build);
  }

  private static int last(List<String> lines, String text) {
    List<Integer> indexes = indexesOf(lines, text);
    Assertions.assertFalse(indexes.isEmpty(), text);
    return indexes.get(indexes.size() - 1);
  }

  private static List<Integer> indexesOf(List<String> lines, String text) {
    return IntStream.range(0, lines.size())
        .filter(i -> lines.get(i).contains(text))
        .boxed()
        .toList();
  }
}
