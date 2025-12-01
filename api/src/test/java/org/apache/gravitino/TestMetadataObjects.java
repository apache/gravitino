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
package org.apache.gravitino;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMetadataObjects {

  @Test
  public void testColumnObject() {
    MetadataObject columnObject =
        MetadataObjects.of("catalog.schema.table", "c1", MetadataObject.Type.COLUMN);
    Assertions.assertEquals("catalog.schema.table", columnObject.parent());
    Assertions.assertEquals("c1", columnObject.name());
    Assertions.assertEquals(MetadataObject.Type.COLUMN, columnObject.type());
    Assertions.assertEquals("catalog.schema.table.c1", columnObject.fullName());

    MetadataObject columnObject2 =
        MetadataObjects.of(
            Lists.newArrayList("catalog", "schema", "table", "c2"), MetadataObject.Type.COLUMN);
    Assertions.assertEquals("catalog.schema.table", columnObject2.parent());
    Assertions.assertEquals("c2", columnObject2.name());
    Assertions.assertEquals(MetadataObject.Type.COLUMN, columnObject2.type());
    Assertions.assertEquals("catalog.schema.table.c2", columnObject2.fullName());

    MetadataObject columnObject3 =
        MetadataObjects.parse("catalog.schema.table.c3", MetadataObject.Type.COLUMN);
    Assertions.assertEquals("catalog.schema.table", columnObject3.parent());
    Assertions.assertEquals("c3", columnObject3.name());
    Assertions.assertEquals(MetadataObject.Type.COLUMN, columnObject3.type());
    Assertions.assertEquals("catalog.schema.table.c3", columnObject3.fullName());

    // Test parent
    MetadataObject parent = MetadataObjects.parent(columnObject);
    Assertions.assertEquals("catalog.schema.table", parent.fullName());
    Assertions.assertEquals("catalog.schema", parent.parent());
    Assertions.assertEquals("table", parent.name());
    Assertions.assertEquals(MetadataObject.Type.TABLE, parent.type());

    // Test incomplete name
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> MetadataObjects.parse("c1", MetadataObject.Type.COLUMN));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> MetadataObjects.parse("catalog", MetadataObject.Type.COLUMN));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> MetadataObjects.parse("catalog.schema", MetadataObject.Type.COLUMN));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> MetadataObjects.parse("catalog.schema.table", MetadataObject.Type.COLUMN));

    // Test incomplete name list
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> MetadataObjects.of(Lists.newArrayList("catalog"), MetadataObject.Type.COLUMN));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            MetadataObjects.of(
                Lists.newArrayList("catalog", "schema"), MetadataObject.Type.COLUMN));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            MetadataObjects.of(
                Lists.newArrayList("catalog", "schema", "table"), MetadataObject.Type.COLUMN));
  }

  @Test
  public void testRoleObject() {
    MetadataObject roleObject = MetadataObjects.of(null, "role.test", MetadataObject.Type.ROLE);
    Assertions.assertEquals("role.test", roleObject.fullName());

    MetadataObject roleObject1 = MetadataObjects.of(null, "role", MetadataObject.Type.ROLE);
    Assertions.assertEquals("role", roleObject1.fullName());

    MetadataObject roleObject2 = MetadataObjects.parse("role.test", MetadataObject.Type.ROLE);
    Assertions.assertEquals("role.test", roleObject2.fullName());

    MetadataObject roleObject3 = MetadataObjects.parse("role", MetadataObject.Type.ROLE);
    Assertions.assertEquals("role", roleObject3.fullName());
  }

  @Test
  public void testJobObject() {
    MetadataObject jobObject = MetadataObjects.of(null, "job_12345", MetadataObject.Type.JOB);
    Assertions.assertEquals("job_12345", jobObject.fullName());

    MetadataObject jobObject2 = MetadataObjects.parse("job_12345", MetadataObject.Type.JOB);
    Assertions.assertEquals("job_12345", jobObject2.fullName());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> MetadataObjects.of("parent", "job_12345", MetadataObject.Type.JOB));
  }

  @Test
  public void testJobTemplateObject() {
    MetadataObject jobTemplateObject =
        MetadataObjects.of(null, "template_abc", MetadataObject.Type.JOB_TEMPLATE);
    Assertions.assertEquals("template_abc", jobTemplateObject.fullName());

    MetadataObject jobTemplateObject2 =
        MetadataObjects.parse("template_abc", MetadataObject.Type.JOB_TEMPLATE);
    Assertions.assertEquals("template_abc", jobTemplateObject2.fullName());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> MetadataObjects.of("parent", "template_abc", MetadataObject.Type.JOB_TEMPLATE));
  }
}
