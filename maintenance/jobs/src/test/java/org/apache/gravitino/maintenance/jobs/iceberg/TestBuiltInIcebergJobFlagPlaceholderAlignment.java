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
package org.apache.gravitino.maintenance.jobs.iceberg;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.maintenance.jobs.BuiltInJob;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Guards built-in Iceberg job templates against flag / placeholder name mismatches. */
public class TestBuiltInIcebergJobFlagPlaceholderAlignment {

  private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\{\\{([^}]+)}}");

  @Test
  public void testBuiltInIcebergJobTemplateFlagPlaceholderNamesAlign() {
    for (BuiltInJob job :
        Arrays.asList(
            new IcebergExpireSnapshotsJob(),
            new IcebergRewriteDataFilesJob(),
            new IcebergUpdateStatsAndMetricsJob())) {
      assertFlagPlaceholderPairsAlign((SparkJobTemplate) job.jobTemplate());
    }
  }

  private static void assertFlagPlaceholderPairsAlign(SparkJobTemplate template) {
    List<String> arguments = template.arguments();
    for (int i = 0; i < arguments.size(); i++) {
      String arg = arguments.get(i);
      if (!arg.startsWith("--") || i + 1 >= arguments.size()) {
        continue;
      }
      String next = arguments.get(i + 1);
      if (next.startsWith("--")) {
        continue;
      }
      Assertions.assertTrue(
          flagNameMatchesPlaceholderName(arg, next),
          String.format(
              "Flag %s must normalize to the same name as placeholder %s in %s",
              arg, next, template.name()));
    }
  }

  private static boolean flagNameMatchesPlaceholderName(String flagArg, String placeholderToken) {
    Matcher matcher = PLACEHOLDER_PATTERN.matcher(placeholderToken);
    if (!matcher.matches()) {
      return true;
    }
    String normalizedFlag = flagArg.substring(2).replace('-', '_').toLowerCase(Locale.ROOT);
    String normalizedPlaceholder = matcher.group(1).replace('-', '_').toLowerCase(Locale.ROOT);
    return normalizedFlag.equals(normalizedPlaceholder);
  }
}
