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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.gravitino.job.JobTemplate;
import org.apache.gravitino.job.JobTemplateProvider;
import org.junit.jupiter.api.Test;

public class TestBuiltInJobTemplateProvider {

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
}
