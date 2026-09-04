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
package org.apache.gravitino.utils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AccessControlDispatcher;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.catalog.FilesetDispatcher;
import org.apache.gravitino.catalog.FunctionDispatcher;
import org.apache.gravitino.catalog.ModelDispatcher;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.catalog.TopicDispatcher;
import org.apache.gravitino.catalog.ViewDispatcher;
import org.apache.gravitino.job.JobOperationDispatcher;
import org.apache.gravitino.metalake.MetalakeDispatcher;
import org.apache.gravitino.policy.PolicyDispatcher;
import org.apache.gravitino.tag.TagDispatcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class TestMetadataObjectUtil {

  @Test
  public void testToEntityType() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> MetadataObjectUtil.toEntityType((MetadataObject) null),
        "metadataObject cannot be null");

    Assertions.assertEquals(
        Entity.EntityType.METALAKE,
        MetadataObjectUtil.toEntityType(
            MetadataObjects.of(null, "metalake", MetadataObject.Type.METALAKE)));

    Assertions.assertEquals(
        Entity.EntityType.CATALOG,
        MetadataObjectUtil.toEntityType(
            MetadataObjects.of(null, "catalog", MetadataObject.Type.CATALOG)));

    Assertions.assertEquals(
        Entity.EntityType.SCHEMA,
        MetadataObjectUtil.toEntityType(
            MetadataObjects.of("catalog", "schema", MetadataObject.Type.SCHEMA)));

    Assertions.assertEquals(
        Entity.EntityType.TABLE,
        MetadataObjectUtil.toEntityType(
            MetadataObjects.of("catalog.schema", "table", MetadataObject.Type.TABLE)));

    Assertions.assertEquals(
        Entity.EntityType.TOPIC,
        MetadataObjectUtil.toEntityType(
            MetadataObjects.of("catalog.schema", "topic", MetadataObject.Type.TOPIC)));

    Assertions.assertEquals(
        Entity.EntityType.FILESET,
        MetadataObjectUtil.toEntityType(
            MetadataObjects.of("catalog.schema", "fileset", MetadataObject.Type.FILESET)));

    Assertions.assertEquals(
        Entity.EntityType.COLUMN,
        MetadataObjectUtil.toEntityType(
            MetadataObjects.of("catalog.schema.table", "column", MetadataObject.Type.COLUMN)));

    Assertions.assertEquals(
        Entity.EntityType.MODEL,
        MetadataObjectUtil.toEntityType(
            MetadataObjects.of("catalog.schema", "model", MetadataObject.Type.MODEL)));

    Assertions.assertEquals(
        Entity.EntityType.VIEW,
        MetadataObjectUtil.toEntityType(
            MetadataObjects.of("catalog.schema", "view", MetadataObject.Type.VIEW)));

    Assertions.assertEquals(
        Entity.EntityType.FUNCTION,
        MetadataObjectUtil.toEntityType(
            MetadataObjects.of("catalog.schema", "function", MetadataObject.Type.FUNCTION)));
  }

  @Test
  public void testToEntityIdent() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> MetadataObjectUtil.toEntityIdent(null, null),
        "metadataName cannot be blank");

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> MetadataObjectUtil.toEntityIdent("metalake", null),
        "metadataObject cannot be null");

    Assertions.assertEquals(
        NameIdentifier.of("metalake"),
        MetadataObjectUtil.toEntityIdent(
            "metalake", MetadataObjects.of(null, "metalake", MetadataObject.Type.METALAKE)));

    // Verify that toEntityIdent uses the metadata object's name, not the context metalakeName
    Assertions.assertEquals(
        NameIdentifier.of("target_metalake"),
        MetadataObjectUtil.toEntityIdent(
            "request_metalake",
            MetadataObjects.of(null, "target_metalake", MetadataObject.Type.METALAKE)));

    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog"),
        MetadataObjectUtil.toEntityIdent(
            "metalake", MetadataObjects.of(null, "catalog", MetadataObject.Type.CATALOG)));

    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "schema"),
        MetadataObjectUtil.toEntityIdent(
            "metalake", MetadataObjects.of("catalog", "schema", MetadataObject.Type.SCHEMA)));

    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "schema", "table"),
        MetadataObjectUtil.toEntityIdent(
            "metalake", MetadataObjects.of("catalog.schema", "table", MetadataObject.Type.TABLE)));

    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "schema", "topic"),
        MetadataObjectUtil.toEntityIdent(
            "metalake", MetadataObjects.of("catalog.schema", "topic", MetadataObject.Type.TOPIC)));

    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "schema", "fileset"),
        MetadataObjectUtil.toEntityIdent(
            "metalake",
            MetadataObjects.of("catalog.schema", "fileset", MetadataObject.Type.FILESET)));

    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "schema", "model"),
        MetadataObjectUtil.toEntityIdent(
            "metalake", MetadataObjects.of("catalog.schema", "model", MetadataObject.Type.MODEL)));

    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "schema", "table", "column"),
        MetadataObjectUtil.toEntityIdent(
            "metalake",
            MetadataObjects.of("catalog.schema.table", "column", MetadataObject.Type.COLUMN)));

    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "schema", "view"),
        MetadataObjectUtil.toEntityIdent(
            "metalake", MetadataObjects.of("catalog.schema", "view", MetadataObject.Type.VIEW)));

    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "schema", "function"),
        MetadataObjectUtil.toEntityIdent(
            "metalake",
            MetadataObjects.of("catalog.schema", "function", MetadataObject.Type.FUNCTION)));
  }

  @Test
  public void testGetParentMetadataObjectsForFlatSchema() {
    // A table under a flat (single-level) schema inherits from [schema, catalog].
    MetadataObject table = MetadataObjects.of("catalog.schema", "table", MetadataObject.Type.TABLE);
    Assertions.assertEquals(
        List.of("SCHEMA:catalog.schema", "CATALOG:catalog"),
        describe(MetadataObjectUtil.getParentMetadataObjects(table, ":")));

    // A flat schema inherits only from its catalog.
    MetadataObject schema = MetadataObjects.of("catalog", "schema", MetadataObject.Type.SCHEMA);
    Assertions.assertEquals(
        List.of("CATALOG:catalog"),
        describe(MetadataObjectUtil.getParentMetadataObjects(schema, ":")));

    // A catalog has no ancestors.
    MetadataObject catalog = MetadataObjects.of(null, "catalog", MetadataObject.Type.CATALOG);
    Assertions.assertTrue(MetadataObjectUtil.getParentMetadataObjects(catalog, ":").isEmpty());
  }

  @Test
  public void testGetParentMetadataObjectsForHierarchicalSchema() {
    // A table under hierarchical schema a:b:c inherits from the schema and all its ancestor
    // schemas (nearest first), then the catalog.
    MetadataObject table = MetadataObjects.of("catalog.a:b:c", "table", MetadataObject.Type.TABLE);
    Assertions.assertEquals(
        List.of(
            "SCHEMA:catalog.a:b:c", "SCHEMA:catalog.a:b", "SCHEMA:catalog.a", "CATALOG:catalog"),
        describe(MetadataObjectUtil.getParentMetadataObjects(table, ":")));

    // The hierarchical schema itself inherits from its ancestor schemas and the catalog, but not
    // from itself.
    MetadataObject schema = MetadataObjects.of("catalog", "a:b:c", MetadataObject.Type.SCHEMA);
    Assertions.assertEquals(
        List.of("SCHEMA:catalog.a:b", "SCHEMA:catalog.a", "CATALOG:catalog"),
        describe(MetadataObjectUtil.getParentMetadataObjects(schema, ":")));

    // A column under a hierarchical schema walks table -> schema -> ancestor schemas -> catalog.
    MetadataObject column =
        MetadataObjects.of("catalog.a:b.table", "col", MetadataObject.Type.COLUMN);
    Assertions.assertEquals(
        List.of(
            "TABLE:catalog.a:b.table", "SCHEMA:catalog.a:b", "SCHEMA:catalog.a", "CATALOG:catalog"),
        describe(MetadataObjectUtil.getParentMetadataObjects(column, ":")));
  }

  @Test
  public void testCheckMetadataObjectUsesInternalDispatchers() {
    GravitinoEnv env = mock(GravitinoEnv.class);
    MetalakeDispatcher metalakeDispatcher = mock(MetalakeDispatcher.class);
    CatalogDispatcher catalogDispatcher = mock(CatalogDispatcher.class);
    SchemaDispatcher schemaDispatcher = mock(SchemaDispatcher.class);
    FilesetDispatcher filesetDispatcher = mock(FilesetDispatcher.class);
    TableDispatcher tableDispatcher = mock(TableDispatcher.class);
    TopicDispatcher topicDispatcher = mock(TopicDispatcher.class);
    ModelDispatcher modelDispatcher = mock(ModelDispatcher.class);
    FunctionDispatcher functionDispatcher = mock(FunctionDispatcher.class);
    ViewDispatcher viewDispatcher = mock(ViewDispatcher.class);
    AccessControlDispatcher accessControlDispatcher = mock(AccessControlDispatcher.class);
    TagDispatcher tagDispatcher = mock(TagDispatcher.class);
    PolicyDispatcher policyDispatcher = mock(PolicyDispatcher.class);
    JobOperationDispatcher jobDispatcher = mock(JobOperationDispatcher.class);

    when(env.internalMetalakeDispatcher()).thenReturn(metalakeDispatcher);
    when(env.internalCatalogDispatcher()).thenReturn(catalogDispatcher);
    when(env.internalSchemaDispatcher()).thenReturn(schemaDispatcher);
    when(env.internalFilesetDispatcher()).thenReturn(filesetDispatcher);
    when(env.internalTableDispatcher()).thenReturn(tableDispatcher);
    when(env.internalTopicDispatcher()).thenReturn(topicDispatcher);
    when(env.internalModelDispatcher()).thenReturn(modelDispatcher);
    when(env.internalFunctionDispatcher()).thenReturn(functionDispatcher);
    when(env.internalViewDispatcher()).thenReturn(viewDispatcher);
    when(env.internalAccessControlDispatcher()).thenReturn(accessControlDispatcher);
    when(env.internalTagDispatcher()).thenReturn(tagDispatcher);
    when(env.internalPolicyDispatcher()).thenReturn(policyDispatcher);
    when(env.internalJobOperationDispatcher()).thenReturn(jobDispatcher);

    NameIdentifier metalakeIdent = NameIdentifier.of("metalake");
    NameIdentifier catalogIdent = NameIdentifier.of("metalake", "catalog");
    NameIdentifier schemaIdent = NameIdentifier.of("metalake", "catalog", "schema");
    NameIdentifier filesetIdent = NameIdentifier.of("metalake", "catalog", "schema", "fileset");
    NameIdentifier tableIdent = NameIdentifier.of("metalake", "catalog", "schema", "table");
    NameIdentifier topicIdent = NameIdentifier.of("metalake", "catalog", "schema", "topic");
    NameIdentifier modelIdent = NameIdentifier.of("metalake", "catalog", "schema", "model");
    NameIdentifier functionIdent = NameIdentifier.of("metalake", "catalog", "schema", "function");
    NameIdentifier viewIdent = NameIdentifier.of("metalake", "catalog", "schema", "view");

    when(metalakeDispatcher.metalakeExists(metalakeIdent)).thenReturn(true);
    when(catalogDispatcher.catalogExists(catalogIdent)).thenReturn(true);
    when(schemaDispatcher.schemaExists(schemaIdent)).thenReturn(true);
    when(filesetDispatcher.filesetExists(filesetIdent)).thenReturn(true);
    when(tableDispatcher.tableExists(tableIdent)).thenReturn(true);
    when(topicDispatcher.topicExists(topicIdent)).thenReturn(true);
    when(modelDispatcher.modelExists(modelIdent)).thenReturn(true);
    when(functionDispatcher.functionExists(functionIdent)).thenReturn(true);
    when(viewDispatcher.viewExists(viewIdent)).thenReturn(true);

    try (MockedStatic<GravitinoEnv> mockedEnv = mockStatic(GravitinoEnv.class)) {
      mockedEnv.when(GravitinoEnv::getInstance).thenReturn(env);

      MetadataObjectUtil.checkMetadataObject(
          "metalake", MetadataObjects.of(null, "metalake", MetadataObject.Type.METALAKE));
      MetadataObjectUtil.checkMetadataObject(
          "metalake", MetadataObjects.of(null, "catalog", MetadataObject.Type.CATALOG));
      MetadataObjectUtil.checkMetadataObject(
          "metalake", MetadataObjects.of("catalog", "schema", MetadataObject.Type.SCHEMA));
      MetadataObjectUtil.checkMetadataObject(
          "metalake", MetadataObjects.of("catalog.schema", "fileset", MetadataObject.Type.FILESET));
      MetadataObjectUtil.checkMetadataObject(
          "metalake", MetadataObjects.of("catalog.schema", "table", MetadataObject.Type.TABLE));
      MetadataObjectUtil.checkMetadataObject(
          "metalake",
          MetadataObjects.of("catalog.schema.table", "column", MetadataObject.Type.COLUMN));
      MetadataObjectUtil.checkMetadataObject(
          "metalake", MetadataObjects.of("catalog.schema", "topic", MetadataObject.Type.TOPIC));
      MetadataObjectUtil.checkMetadataObject(
          "metalake", MetadataObjects.of("catalog.schema", "model", MetadataObject.Type.MODEL));
      MetadataObjectUtil.checkMetadataObject(
          "metalake",
          MetadataObjects.of("catalog.schema", "function", MetadataObject.Type.FUNCTION));
      MetadataObjectUtil.checkMetadataObject(
          "metalake", MetadataObjects.of("catalog.schema", "view", MetadataObject.Type.VIEW));
      MetadataObjectUtil.checkMetadataObject(
          "metalake", MetadataObjects.of(null, "role", MetadataObject.Type.ROLE));
      MetadataObjectUtil.checkMetadataObject(
          "metalake", MetadataObjects.of(null, "tag", MetadataObject.Type.TAG));
      MetadataObjectUtil.checkMetadataObject(
          "metalake", MetadataObjects.of(null, "policy", MetadataObject.Type.POLICY));
      MetadataObjectUtil.checkMetadataObject(
          "metalake", MetadataObjects.of(null, "job", MetadataObject.Type.JOB));
      MetadataObjectUtil.checkMetadataObject(
          "metalake", MetadataObjects.of(null, "template", MetadataObject.Type.JOB_TEMPLATE));
    }

    verify(metalakeDispatcher).metalakeExists(metalakeIdent);
    verify(catalogDispatcher).catalogExists(catalogIdent);
    verify(schemaDispatcher).schemaExists(schemaIdent);
    verify(filesetDispatcher).filesetExists(filesetIdent);
    verify(tableDispatcher, times(2)).tableExists(tableIdent);
    verify(topicDispatcher).topicExists(topicIdent);
    verify(modelDispatcher).modelExists(modelIdent);
    verify(functionDispatcher).functionExists(functionIdent);
    verify(viewDispatcher).viewExists(viewIdent);
    verify(accessControlDispatcher).getRole("metalake", "role");
    verify(tagDispatcher).getTag("metalake", "tag");
    verify(policyDispatcher).getPolicy("metalake", "policy");
    verify(jobDispatcher).getJob("metalake", "job");
    verify(jobDispatcher).getJobTemplate("metalake", "template");
  }

  private static List<String> describe(List<MetadataObject> objects) {
    return objects.stream().map(o -> o.type() + ":" + o.fullName()).collect(Collectors.toList());
  }
}
