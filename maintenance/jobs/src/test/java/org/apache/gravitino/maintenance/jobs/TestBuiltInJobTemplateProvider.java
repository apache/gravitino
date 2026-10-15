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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableSet;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.job.JobTemplate;
import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.job.JobTemplateResolver;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Test;

public class TestBuiltInJobTemplateProvider {

  /** Ensures manifest rewriting is discoverable exactly once through the built-in provider. */
  @Test
  public void testRewriteManifestsTemplateRegistered() {
    assertEquals(
        1L,
        new BuiltInJobTemplateProvider()
            .jobTemplates().stream()
                .filter(template -> template.name().equals("builtin-iceberg-rewrite-manifests"))
                .count());
  }

  @Test
  public void testJobTemplatesReturnsNonEmptyList() {
    BuiltInJobTemplateProvider provider = new BuiltInJobTemplateProvider();
    List<? extends JobTemplate> templates = provider.jobTemplates();

    assertNotNull(templates, "Job templates list should not be null");
    assertFalse(templates.isEmpty(), "Should provide at least one built-in job template");
  }

  @Test
  public void testAllTemplatesHaveBuiltInPrefix() {
    BuiltInJobTemplateProvider provider = new BuiltInJobTemplateProvider();
    List<? extends JobTemplate> templates = provider.jobTemplates();

    for (JobTemplate template : templates) {
      assertTrue(
          template.name().startsWith(JobTemplateProvider.BUILTIN_NAME_PREFIX),
          "Template name should start with builtin- prefix: " + template.name());
    }
  }

  @Test
  public void testAllTemplatesHaveValidVersion() {
    BuiltInJobTemplateProvider provider = new BuiltInJobTemplateProvider();
    List<? extends JobTemplate> templates = provider.jobTemplates();

    for (JobTemplate template : templates) {
      String version = template.customFields().get(JobTemplateProvider.PROPERTY_VERSION_KEY);
      assertNotNull(version, "Template should have version property: " + template.name());
      assertTrue(
          version.matches(JobTemplateProvider.VERSION_VALUE_PATTERN),
          "Version should match pattern v\\d+: " + version);
    }
  }

  @Test
  public void testTemplatesOnlyRequireTheParametersTheyHaveTo() {
    // A built-in template must let a caller run it with the catalog and the table alone, so every
    // other parameter needs a default value. The catalog connection has no default that fits every
    // catalog, and an empty where clause rewrites a whole table, so those stay required.
    Map<String, Set<String>> expected = new HashMap<>();
    Set<String> icebergCatalog =
        ImmutableSet.of(
            "catalog_name",
            "catalog_type",
            "catalog_uri",
            "table_identifier",
            "warehouse_location");
    expected.put("builtin-sparkpi", ImmutableSet.of());
    expected.put("builtin-iceberg-update-stats", icebergCatalog);
    expected.put("builtin-iceberg-expire-snapshots", icebergCatalog);
    expected.put("builtin-iceberg-remove-orphan-files", icebergCatalog);
    expected.put("builtin-iceberg-rewrite-manifests", icebergCatalog);
    expected.put(
        "builtin-iceberg-rewrite-data-files",
        ImmutableSet.<String>builder().addAll(icebergCatalog).add("where_clause").build());

    for (JobTemplate template : new BuiltInJobTemplateProvider().jobTemplates()) {
      Set<String> required = new JobTemplateResolver(toEntity(template)).requiredParameters();
      assertEquals(
          expected.get(template.name()),
          required,
          "Unexpected required parameters of built-in job template " + template.name());
    }
  }

  @Test
  public void testAllTemplatePlaceholdersAreValid() {
    // Built-in templates are stored without going through registerJobTemplate, so a malformed
    // placeholder in one of them would only fail when a job runs from it.
    for (JobTemplate template : new BuiltInJobTemplateProvider().jobTemplates()) {
      assertDoesNotThrow(
          () -> JobTemplateResolver.validate(toEntity(template)),
          "Built-in job template has invalid placeholders: " + template.name());
    }
  }

  private static JobTemplateEntity toEntity(JobTemplate template) {
    return JobTemplateEntity.builder()
        .withId(1L)
        .withName(template.name())
        .withComment(template.comment())
        .withNamespace(NamespaceUtil.ofJobTemplate("test"))
        .withTemplateContent(JobTemplateEntity.TemplateContent.fromJobTemplate(template))
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
        .build();
  }
}
