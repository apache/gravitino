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

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.gravitino.job.JobTemplate;

/**
 * Resolves the arguments of a job template the way the Gravitino server does, so a test can run a
 * job with the arguments its own template produces.
 */
public final class TemplateArguments {

  private static final Pattern PLACEHOLDER = Pattern.compile("\\{\\{([\\w.-]+)(?::-(.*?))?}}");

  private TemplateArguments() {}

  /**
   * Resolves the arguments of a job template against a job configuration. A parameter takes its
   * value from the job configuration, and otherwise the default value its placeholder declares.
   *
   * @param template the job template
   * @param jobConf the job configuration
   * @return the resolved arguments
   * @throws IllegalArgumentException if a parameter has neither a value nor a default value
   */
  public static String[] resolve(JobTemplate template, Map<String, String> jobConf) {
    return template.arguments().stream()
        .map(argument -> resolve(argument, jobConf))
        .toArray(String[]::new);
  }

  private static String resolve(String argument, Map<String, String> jobConf) {
    Matcher matcher = PLACEHOLDER.matcher(argument);
    StringBuilder resolved = new StringBuilder();
    while (matcher.find()) {
      String name = matcher.group(1);
      String replacement = jobConf.get(name);
      if (replacement == null) {
        replacement = matcher.group(2);
      }
      if (replacement == null) {
        throw new IllegalArgumentException(
            "No value is provided for the job template parameter " + name);
      }
      matcher.appendReplacement(resolved, Matcher.quoteReplacement(replacement));
    }
    matcher.appendTail(resolved);
    return resolved.toString();
  }
}
