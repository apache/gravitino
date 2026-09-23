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

package org.apache.gravitino.job;

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.utils.FileFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves a job template into the runtime job template of a job run: the template with its
 * placeholders replaced with the job configuration, and its executable, scripts, jars, files and
 * archives fetched into the job's staging directory.
 *
 * <p>Creating a resolver parses and validates the template's placeholders, so a resolver always
 * holds a valid template, and the placeholders are parsed only once per job run. To only validate a
 * template, use {@link #validate}.
 */
public final class JobTemplateResolver {

  private static final Logger LOG = LoggerFactory.getLogger(JobTemplateResolver.class);

  private static final int FETCH_TIMEOUT_IN_MS = 30 * 1000; // 30 seconds

  private final JobTemplateEntity jobTemplateEntity;

  // The parameters of the template, each mapped to its default value, or to an empty optional if
  // the parameter is required.
  private final Map<String, Optional<String>> parameters;

  /**
   * Creates a resolver for a job template.
   *
   * @param jobTemplateEntity the job template
   * @throws IllegalArgumentException if the template's placeholders are malformed, for example a
   *     parameter has conflicting default values
   */
  public JobTemplateResolver(JobTemplateEntity jobTemplateEntity) {
    this.jobTemplateEntity = jobTemplateEntity;
    this.parameters =
        JobTemplatePlaceholderUtils.parseParameters(jobTemplateEntity.templateContent());
  }

  /**
   * Validates the placeholders of a job template, for example when the template is registered or
   * updated, so a malformed template is rejected before any job runs from it.
   *
   * @param jobTemplateEntity the job template
   * @throws IllegalArgumentException if the template's placeholders are malformed, for example a
   *     parameter has conflicting default values
   */
  public static void validate(JobTemplateEntity jobTemplateEntity) {
    JobTemplatePlaceholderUtils.parseParameters(jobTemplateEntity.templateContent());
  }

  /**
   * Checks that a job configuration provides every required parameter of the template, and logs a
   * warning for the keys that the template does not use. It fetches nothing, so it can run before
   * any resource is created for the job.
   *
   * @param jobConf the job configuration, may be null
   * @throws IllegalArgumentException listing all the required parameters that have no value
   */
  public void checkJobConf(@Nullable Map<String, String> jobConf) {
    Map<String, String> conf = jobConf == null ? Collections.emptyMap() : jobConf;
    checkRequiredParameters(conf);

    Set<String> unusedKeys = JobTemplatePlaceholderUtils.findUnusedKeys(parameters, conf);
    if (!unusedKeys.isEmpty()) {
      LOG.warn(
          "Job configuration keys {} are not used by job template {}",
          unusedKeys,
          jobTemplateEntity.name());
    }
  }

  /**
   * Resolves the template into the runtime job template of a job run.
   *
   * @param jobConf the job configuration, may be null
   * @param stagingDir the staging directory of the job, where the files are fetched to
   * @return the runtime job template
   * @throws IllegalArgumentException if a required parameter has no value, or the resolved
   *     environments, custom fields or configs contain duplicate keys
   * @throws RuntimeException if a file cannot be fetched
   */
  public JobTemplate resolve(@Nullable Map<String, String> jobConf, File stagingDir) {
    String name = jobTemplateEntity.name();
    String comment = jobTemplateEntity.comment();

    JobTemplateEntity.TemplateContent content = jobTemplateEntity.templateContent();
    Map<String, String> conf = jobConf == null ? Collections.emptyMap() : jobConf;
    // Check every parameter before fetching any file, so a missing value fails without downloading
    // anything and reports all the missing parameters at once.
    checkRequiredParameters(conf);
    Function<String, String> resolver =
        value -> JobTemplatePlaceholderUtils.replacePlaceholders(value, conf, parameters);
    Function<String, String> fetcher =
        uri -> fetchFileFromUri(resolver.apply(uri), stagingDir, FETCH_TIMEOUT_IN_MS);

    String executable = fetcher.apply(content.executable());
    List<String> args = resolveList(content.arguments(), resolver);
    Map<String, String> environments = resolveMap(content.environments(), resolver, "environments");
    Map<String, String> customFields = resolveMap(content.customFields(), resolver, "customFields");

    if (content.jobType() == JobTemplate.JobType.SHELL) {
      return ShellJobTemplate.builder()
          .withName(name)
          .withComment(comment)
          .withExecutable(executable)
          .withArguments(args)
          .withEnvironments(environments)
          .withCustomFields(customFields)
          .withScripts(resolveList(content.scripts(), fetcher))
          .build();
    }

    if (content.jobType() == JobTemplate.JobType.SPARK) {
      return SparkJobTemplate.builder()
          .withName(name)
          .withComment(comment)
          .withExecutable(executable)
          .withArguments(args)
          .withEnvironments(environments)
          .withCustomFields(customFields)
          .withClassName(resolver.apply(content.className()))
          .withJars(resolveList(content.jars(), fetcher))
          .withFiles(resolveList(content.files(), fetcher))
          .withArchives(resolveList(content.archives(), fetcher))
          .withConfigs(resolveMap(content.configs(), resolver, "configs"))
          .build();
    }

    throw new IllegalArgumentException("Unsupported job type: " + content.jobType());
  }

  @VisibleForTesting
  static List<String> fetchFilesFromUri(List<String> uris, File stagingDir, int timeoutInMs) {
    return uris.stream()
        .map(uri -> fetchFileFromUri(uri, stagingDir, timeoutInMs))
        .collect(Collectors.toList());
  }

  @VisibleForTesting
  static String fetchFileFromUri(String uri, File stagingDir, int timeoutInMs) {
    try {
      URI fileUri = new URI(uri);
      File destFile = new File(stagingDir, new File(fileUri.getPath()).getName());
      return FileFetcher.get()
          .fetchFileFromUri(
              uri,
              destFile,
              timeoutInMs,
              null /* hadoopConf: job file URIs never use the hdfs scheme */);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to fetch file from URI %s", uri), e);
    }
  }

  private void checkRequiredParameters(Map<String, String> jobConf) {
    Set<String> missing = JobTemplatePlaceholderUtils.findMissingParameters(parameters, jobConf);
    if (!missing.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "No value is provided for the parameter(s) %s of job template %s. Set them in the "
                  + "job configuration, or give them a default value in the template, for example "
                  + "{{name:-default}}",
              missing, jobTemplateEntity.name()));
    }
  }

  private static List<String> resolveList(List<String> source, Function<String, String> resolver) {
    return source.stream().map(resolver).collect(Collectors.toList());
  }

  private static Map<String, String> resolveMap(
      Map<String, String> source, Function<String, String> resolver, String field) {
    Map<String, String> resolved = new LinkedHashMap<>();
    source.forEach(
        (key, value) -> {
          String resolvedKey = resolver.apply(key);
          String resolvedValue = resolver.apply(value);
          String previous = resolved.putIfAbsent(resolvedKey, resolvedValue);
          if (previous != null) {
            throw new IllegalArgumentException(
                String.format(
                    "Job template %s key %s is duplicated after resolving placeholders",
                    field, resolvedKey));
          }
        });
    return resolved;
  }
}
