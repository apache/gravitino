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

package org.apache.gravitino.meta;

import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Field;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.job.JobTemplate;
import org.apache.gravitino.job.ShellJobTemplate;
import org.apache.gravitino.job.SparkJobTemplate;

@ToString
public class JobTemplateEntity implements Entity, Auditable, HasIdentifier {

  /**
   * A job template content that will be used internally to maintain all the information of a job
   * template. this class will be serialized and stored in the entity store. Internally, we don't
   * separate the different types of job templates, so this class will be used to represent all the
   * job templates.
   */
  @Getter()
  @Accessors(fluent = true)
  @ToString
  @NoArgsConstructor
  @AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
  @EqualsAndHashCode
  @lombok.Builder(setterPrefix = "with")
  public static class TemplateContent {
    // Generic fields for job template content
    private JobTemplate.JobType jobType;
    private String executable;
    private List<String> arguments;
    private Map<String, String> environments;
    private Map<String, String> customFields;
    // Fields for Shell job template content
    private List<String> scripts;
    // Fields for Spark job template content
    private String className;
    private List<String> jars;
    private List<String> files;
    private List<String> archives;
    private Map<String, String> configs;

    public static TemplateContent fromJobTemplate(JobTemplate jobTemplate) {
      TemplateContentBuilder builder =
          TemplateContent.builder()
              .withJobType(jobTemplate.jobType())
              .withExecutable(jobTemplate.executable())
              .withArguments(jobTemplate.arguments())
              .withEnvironments(jobTemplate.environments())
              .withCustomFields(jobTemplate.customFields());

      if (jobTemplate instanceof ShellJobTemplate) {
        ShellJobTemplate shellJobTemplate = (ShellJobTemplate) jobTemplate;
        builder.withScripts(shellJobTemplate.scripts());

      } else if (jobTemplate instanceof SparkJobTemplate) {
        SparkJobTemplate sparkJobTemplate = (SparkJobTemplate) jobTemplate;
        builder
            .withClassName(sparkJobTemplate.className())
            .withJars(sparkJobTemplate.jars())
            .withFiles(sparkJobTemplate.files())
            .withArchives(sparkJobTemplate.archives())
            .withConfigs(sparkJobTemplate.configs());
      }

      return builder.build();
    }
  }

  public static final Field ID =
      Field.required("id", Long.class, "The unique id of the job template entity.");
  public static final Field NAME =
      Field.required("name", String.class, "The name of the job template entity.");
  public static final Field COMMENT =
      Field.optional(
          "comment", String.class, "The comment or description of the job template entity.");
  public static final Field TEMPLATE_CONTENT =
      Field.required(
          "template_content",
          TemplateContent.class,
          "The template content of the job template entity.");
  public static final Field AUDIT_INFO =
      Field.required(
          "audit_info", AuditInfo.class, "The audit details of the job template entity.");

  private Long id;
  private String name;
  private Namespace namespace;
  private String comment;
  private TemplateContent templateContent;
  private AuditInfo auditInfo;

  private JobTemplateEntity() {}

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, id);
    fields.put(NAME, name);
    fields.put(COMMENT, comment);
    fields.put(TEMPLATE_CONTENT, templateContent);
    fields.put(AUDIT_INFO, auditInfo);
    return Collections.unmodifiableMap(fields);
  }

  @Override
  public Long id() {
    return id;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Namespace namespace() {
    return namespace;
  }

  public String comment() {
    return comment;
  }

  public TemplateContent templateContent() {
    return templateContent;
  }

  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  @Override
  public EntityType type() {
    return EntityType.JOB_TEMPLATE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof JobTemplateEntity)) {
      return false;
    }

    JobTemplateEntity that = (JobTemplateEntity) o;
    return Objects.equals(id, that.id)
        && Objects.equals(name, that.name)
        && Objects.equals(namespace, that.namespace)
        && Objects.equals(comment, that.comment)
        && Objects.equals(templateContent, that.templateContent)
        && Objects.equals(auditInfo, that.auditInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, namespace, comment, templateContent, auditInfo);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final JobTemplateEntity jobTemplateEntity;

    private Builder() {
      this.jobTemplateEntity = new JobTemplateEntity();
    }

    public Builder withId(Long id) {
      jobTemplateEntity.id = id;
      return this;
    }

    public Builder withName(String name) {
      jobTemplateEntity.name = name;
      return this;
    }

    public Builder withNamespace(Namespace namespace) {
      jobTemplateEntity.namespace = namespace;
      return this;
    }

    public Builder withComment(String comment) {
      jobTemplateEntity.comment = comment;
      return this;
    }

    public Builder withTemplateContent(TemplateContent templateContent) {
      jobTemplateEntity.templateContent = templateContent;
      return this;
    }

    public Builder withAuditInfo(AuditInfo auditInfo) {
      jobTemplateEntity.auditInfo = auditInfo;
      return this;
    }

    public JobTemplateEntity build() {
      jobTemplateEntity.validate();
      return jobTemplateEntity;
    }
  }
}
