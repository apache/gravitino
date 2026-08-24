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

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSupportsRelationOperations {

  @Test
  public void testRelationOperationsDoesNotExposeTagSpecificMethods() {
    Set<String> methodNames =
        Arrays.stream(SupportsRelationOperations.class.getDeclaredMethods())
            .map(Method::getName)
            .collect(Collectors.toSet());

    Assertions.assertFalse(methodNames.contains("listMetadataObjectsForTag"));
    Assertions.assertFalse(methodNames.contains("updateTagRelations"));
  }

  @Test
  public void testRelationEdgeTargetCarriesOptionalRelationValue() {
    NameIdentifier targetIdent = NameIdentifier.of("metalake", "tag");

    RelationEdgeTarget targetWithValue =
        RelationEdgeTarget.of(targetIdent, Entity.EntityType.TAG, "finance");
    Assertions.assertEquals(Entity.EntityType.TAG, targetWithValue.entityType());
    Assertions.assertEquals(targetIdent, targetWithValue.nameIdentifier());
    Assertions.assertTrue(targetWithValue.relationValue().isPresent());
    Assertions.assertEquals("finance", targetWithValue.relationValue().get());

    RelationEdgeTarget targetWithNoValue =
        RelationEdgeTarget.of(targetIdent, Entity.EntityType.TAG, null);
    Assertions.assertEquals(Entity.EntityType.TAG, targetWithNoValue.entityType());
    Assertions.assertEquals(targetIdent, targetWithNoValue.nameIdentifier());
    Assertions.assertFalse(targetWithNoValue.relationValue().isPresent());
  }

  @Test
  public void testPrimitiveListDelegatesToRelationQuery() throws IOException {
    RecordingRelationOperations relationOperations = new RecordingRelationOperations();
    NameIdentifier tagIdent = NameIdentifier.of("metalake", "tag");

    relationOperations.listEntitiesByRelation(
        SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
        tagIdent,
        Entity.EntityType.TAG,
        false);

    RelationQuery query = relationOperations.relationQuery;
    Assertions.assertEquals(
        SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL, query.relationType());
    Assertions.assertEquals(tagIdent, query.anchorIdentifier());
    Assertions.assertEquals(Entity.EntityType.TAG, query.anchorEntityType());
    Assertions.assertFalse(query.allFields());
    Assertions.assertFalse(query.relationValue().isPresent());
  }

  @Test
  public void testPrimitiveUpdateDelegatesToRelationUpdate()
      throws IOException, NoSuchEntityException, EntityAlreadyExistsException {
    RecordingRelationOperations relationOperations = new RecordingRelationOperations();
    NameIdentifier srcIdent = NameIdentifier.of("metalake", "catalog", "schema", "table");
    NameIdentifier targetIdent = NameIdentifier.of("metalake", "tag");
    NameIdentifier[] targetsToAdd = new NameIdentifier[] {targetIdent};
    NameIdentifier[] targetsToRemove = new NameIdentifier[0];

    relationOperations.updateEntityRelations(
        SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
        srcIdent,
        Entity.EntityType.TABLE,
        targetsToAdd,
        targetsToRemove);

    RelationUpdate update = relationOperations.relationUpdate;
    Assertions.assertEquals(
        SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL, update.relationType());
    Assertions.assertEquals(srcIdent, update.sourceIdentifier());
    Assertions.assertEquals(Entity.EntityType.TABLE, update.sourceEntityType());
    Assertions.assertEquals(1, update.targetsToAdd().length);
    Assertions.assertEquals(targetIdent, update.targetsToAdd()[0].nameIdentifier());
    Assertions.assertEquals(Entity.EntityType.TAG, update.targetsToAdd()[0].entityType());
    Assertions.assertFalse(update.targetsToAdd()[0].relationValue().isPresent());
    Assertions.assertEquals(0, update.targetsToRemove().length);
  }

  @Test
  public void testRelationUpdateCopiesTargetArrays() {
    NameIdentifier srcIdent = NameIdentifier.of("metalake", "catalog", "schema", "table");
    RelationEdgeTarget originalTarget =
        RelationEdgeTarget.of(NameIdentifier.of("metalake", "tag"), Entity.EntityType.TAG, "dev");
    RelationEdgeTarget replacementTarget =
        RelationEdgeTarget.of(NameIdentifier.of("metalake", "tag2"), Entity.EntityType.TAG, "prod");
    RelationEdgeTarget[] targetsToAdd = new RelationEdgeTarget[] {originalTarget};
    RelationEdgeTarget[] targetsToRemove = new RelationEdgeTarget[] {originalTarget};

    RelationUpdate update =
        RelationUpdate.of(
            SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
            srcIdent,
            Entity.EntityType.TABLE,
            targetsToAdd,
            targetsToRemove);

    targetsToAdd[0] = replacementTarget;
    targetsToRemove[0] = replacementTarget;
    Assertions.assertSame(originalTarget, update.targetsToAdd()[0]);
    Assertions.assertSame(originalTarget, update.targetsToRemove()[0]);

    RelationEdgeTarget[] returnedTargetsToAdd = update.targetsToAdd();
    RelationEdgeTarget[] returnedTargetsToRemove = update.targetsToRemove();
    returnedTargetsToAdd[0] = replacementTarget;
    returnedTargetsToRemove[0] = replacementTarget;
    Assertions.assertSame(originalTarget, update.targetsToAdd()[0]);
    Assertions.assertSame(originalTarget, update.targetsToRemove()[0]);
  }

  private static class RecordingRelationOperations implements SupportsRelationOperations {
    private RelationQuery relationQuery;
    private RelationUpdate relationUpdate;

    @Override
    public <E extends Entity & HasIdentifier> List<E> listEntitiesByRelation(RelationQuery query)
        throws IOException {
      this.relationQuery = query;
      return List.of();
    }

    @Override
    public List<RelationalEntity<?>> batchListEntitiesByRelation(
        Type relType, List<NameIdentifier> nameIdentifiers, Entity.EntityType identType)
        throws IOException {
      return List.of();
    }

    @Override
    public <E extends Entity & HasIdentifier> E getEntityByRelation(
        Type relType,
        NameIdentifier srcIdentifier,
        Entity.EntityType srcType,
        NameIdentifier destEntityIdent)
        throws IOException, NoSuchEntityException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void insertRelation(
        Type relType,
        NameIdentifier srcIdentifier,
        Entity.EntityType srcType,
        NameIdentifier dstIdentifier,
        Entity.EntityType dstType,
        boolean override)
        throws IOException {}

    @Override
    public <E extends Entity & HasIdentifier> List<E> updateEntityRelations(RelationUpdate update)
        throws IOException, NoSuchEntityException, EntityAlreadyExistsException {
      this.relationUpdate = update;
      return List.of();
    }
  }
}
