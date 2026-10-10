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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.connector.job.JobResourceUtils;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves a job template into the runtime job template of a job run: the template with its
 * placeholders replaced with the job configuration. The resources of the runtime job template, that
 * is its executable, scripts, jars, files and archives, are kept as URIs; fetch them with {@link
 * JobResourceUtils} where needed.
 *
 * <p>Creating a resolver parses and validates the template's placeholders, so a resolver always
 * holds a valid template, and the placeholders are parsed only once per job run. To only validate a
 * template, use {@link #validate}.
 */
public final class JobTemplateResolver {

  private static final Logger LOG = LoggerFactory.getLogger(JobTemplateResolver.class);

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
   * Returns the parameters of the template that a job configuration has to provide, which are the
   * ones whose placeholders declare no default value.
   *
   * @return the required parameters, sorted
   */
  public Set<String> requiredParameters() {
    return JobTemplatePlaceholderUtils.findMissingParameters(parameters, Collections.emptyMap());
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
   * Resolves the template into the runtime job template of a job run. It fetches nothing, the
   * resources of the runtime job template are the resolved URIs.
   *
   * @param jobConf the job configuration, may be null
   * @return the runtime job template
   * @throws IllegalArgumentException if a required parameter has no value, or the resolved
   *     environments, custom fields or configs contain duplicate keys
   */
  public JobTemplate resolve(@Nullable Map<String, String> jobConf) {
    String name = jobTemplateEntity.name();
    String comment = jobTemplateEntity.comment();

    JobTemplateEntity.TemplateContent content = jobTemplateEntity.templateContent();
    Map<String, String> conf = jobConf == null ? Collections.emptyMap() : jobConf;
    // Check every parameter first, so all the missing parameters are reported at once.
    checkRequiredParameters(conf);
    Function<String, String> resolver =
        value -> JobTemplatePlaceholderUtils.replacePlaceholders(value, conf, parameters);

    String executable = resolver.apply(content.executable());
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
          .withScripts(resolveList(content.scripts(), resolver))
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
          .withJars(resolveList(content.jars(), resolver))
          .withFiles(resolveList(content.files(), resolver))
          .withArchives(resolveList(content.archives(), resolver))
          .withConfigs(resolveMap(content.configs(), resolver, "configs"))
          .build();
    }

    throw new IllegalArgumentException("Unsupported job type: " + content.jobType());
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

  private static List<String> resolveList(
      @Nullable List<String> source, Function<String, String> resolver) {
    if (source == null) {
      return Collections.emptyList();
    }
    return source.stream().map(resolver).collect(Collectors.toList());
  }

  private static Map<String, String> resolveMap(
      @Nullable Map<String, String> source, Function<String, String> resolver, String field) {
    Map<String, String> resolved = new LinkedHashMap<>();
    if (source == null) {
      return resolved;
    }
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
