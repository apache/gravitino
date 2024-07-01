package com.datastrato.gravitino.storage.relational.service;

import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.TagEntity;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.datastrato.gravitino.storage.relational.TestJDBCBackend;
import com.datastrato.gravitino.tag.TagManager;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public class TestTagMetaService extends TestJDBCBackend {

  private final String metalakeName = "metalake_for_tag_test";

  private final AuditInfo auditInfo =
      AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

  private final Map<String, String> props = ImmutableMap.of("k1", "v1");

  @Test
  public void testInsertAndGetTagByIdentifier() {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    // Test no tag entity.
    TagMetaService tagMetaService = TagMetaService.getInstance();
    Exception excep = Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> tagMetaService.getTagByIdentifier(
            TagManager.ofTagIdent(metalakeName, "tag1")));
    Assertions.assertEquals("No such tag entity: tag1", excep.getMessage());

    // Test get tag entity
    TagEntity tagEntity =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(TagManager.ofTagNamespace(metalakeName))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(auditInfo)
            .build();
    tagMetaService.insertTag(tagEntity, false);

    TagEntity resultTagEntity =
        tagMetaService.getTagByIdentifier(TagManager.ofTagIdent(metalakeName, "tag1"));
    Assertions.assertEquals(tagEntity, resultTagEntity);

    // Test with null comment and properties.
    TagEntity tagEntity1 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag2")
            .withNamespace(TagManager.ofTagNamespace(metalakeName))
            .withAuditInfo(auditInfo)
            .build();

    tagMetaService.insertTag(tagEntity1, false);
    TagEntity resultTagEntity1 =
        tagMetaService.getTagByIdentifier(TagManager.ofTagIdent(metalakeName, "tag2"));
    Assertions.assertEquals(tagEntity1, resultTagEntity1);
    Assertions.assertNull(resultTagEntity1.comment());
    Assertions.assertNull(resultTagEntity1.properties());

    // Test insert with overwrite.
    TagEntity tagEntity2 =
        TagEntity.builder()
            .withId(tagEntity1.id())
            .withName("tag3")
            .withNamespace(TagManager.ofTagNamespace(metalakeName))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(auditInfo)
            .build();

    Assertions.assertThrows(Exception.class,
        () -> tagMetaService.insertTag(tagEntity2, false));

    tagMetaService.insertTag(tagEntity2, true);

    TagEntity resultTagEntity2 =
        tagMetaService.getTagByIdentifier(TagManager.ofTagIdent(metalakeName, "tag3"));
    Assertions.assertEquals(tagEntity2, resultTagEntity2);
  }

  @Test
  public void testCreateAndListTags() {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    TagMetaService tagMetaService = TagMetaService.getInstance();
    TagEntity tagEntity1 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(TagManager.ofTagNamespace(metalakeName))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(auditInfo)
            .build();
    tagMetaService.insertTag(tagEntity1, false);

    TagEntity tagEntity2 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag2")
            .withNamespace(TagManager.ofTagNamespace(metalakeName))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(auditInfo)
            .build();
    tagMetaService.insertTag(tagEntity2, false);

    List<TagEntity> tagEntities =
        tagMetaService.listTagsByNamespace(TagManager.ofTagNamespace(metalakeName));
    Assertions.assertEquals(2, tagEntities.size());
    Assertions.assertTrue(tagEntities.contains(tagEntity1));
    Assertions.assertTrue(tagEntities.contains(tagEntity2));
  }

  @Test
  public void testUpdateTag() throws IOException {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    TagMetaService tagMetaService = TagMetaService.getInstance();
    TagEntity tagEntity1 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(TagManager.ofTagNamespace(metalakeName))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(auditInfo)
            .build();
    tagMetaService.insertTag(tagEntity1, false);

    // Update with no tag entity.
    Exception excep = Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> tagMetaService.updateTag(
            TagManager.ofTagIdent(metalakeName, "tag2"), tagEntity -> tagEntity));
    Assertions.assertEquals("No such tag entity: tag2", excep.getMessage());

    // Update tag entity.
    TagEntity tagEntity2 =
        TagEntity.builder()
            .withId(tagEntity1.id())
            .withName("tag1")
            .withNamespace(TagManager.ofTagNamespace(metalakeName))
            .withComment("comment1")
            .withProperties(ImmutableMap.of("k2", "v2"))
            .withAuditInfo(auditInfo)
            .build();
    TagEntity updatedTagEntity =
        tagMetaService.updateTag(
            TagManager.ofTagIdent(metalakeName, "tag1"), tagEntity -> tagEntity2);
    Assertions.assertEquals(tagEntity2, updatedTagEntity);

    TagEntity loadedTagEntity =
        tagMetaService.getTagByIdentifier(TagManager.ofTagIdent(metalakeName, "tag1"));
    Assertions.assertEquals(tagEntity2, loadedTagEntity);

    // Update with different id.
    TagEntity tagEntity3 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(TagManager.ofTagNamespace(metalakeName))
            .withComment("comment1")
            .withProperties(ImmutableMap.of("k2", "v2"))
            .withAuditInfo(auditInfo)
            .build();

    Exception excep1 = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> tagMetaService.updateTag(
            TagManager.ofTagIdent(metalakeName, "tag1"), tagEntity -> tagEntity3));
    Assertions.assertEquals(
        "The updated tag entity id: " + tagEntity3.id() + " must have the same id as the old " +
            "entity id " + tagEntity2.id(),
        excep1.getMessage());

    TagEntity loadedTagEntity1 =
        tagMetaService.getTagByIdentifier(TagManager.ofTagIdent(metalakeName, "tag1"));
    Assertions.assertEquals(tagEntity2, loadedTagEntity1);
  }

  @Test
  public void testDeleteTag() {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    TagMetaService tagMetaService = TagMetaService.getInstance();
    TagEntity tagEntity1 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(TagManager.ofTagNamespace(metalakeName))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(auditInfo)
            .build();
    tagMetaService.insertTag(tagEntity1, false);

    boolean deleted = tagMetaService.deleteTag(TagManager.ofTagIdent(metalakeName, "tag1"));
    Assertions.assertTrue(deleted);

    deleted = tagMetaService.deleteTag(TagManager.ofTagIdent(metalakeName, "tag1"));
    Assertions.assertFalse(deleted);

    Exception excep = Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> tagMetaService.getTagByIdentifier(
            TagManager.ofTagIdent(metalakeName, "tag1")));
    Assertions.assertEquals("No such tag entity: tag1", excep.getMessage());
  }

  @Test
  public void testDeleteMetalake() {
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    backend.insert(metalake, false);

    TagMetaService tagMetaService = TagMetaService.getInstance();
    TagEntity tagEntity1 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag1")
            .withNamespace(TagManager.ofTagNamespace(metalakeName))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(auditInfo)
            .build();
    tagMetaService.insertTag(tagEntity1, false);

    Assertions.assertTrue(
        MetalakeMetaService.getInstance().deleteMetalake(metalake.nameIdentifier(), false));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> tagMetaService.getTagByIdentifier(
            TagManager.ofTagIdent(metalakeName, "tag1")));

    // Test delete metalake with cascade.
    BaseMetalake metalake1 =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName + "1", auditInfo);
    backend.insert(metalake1, false);

    TagEntity tagEntity2 =
        TagEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName("tag2")
            .withNamespace(TagManager.ofTagNamespace(metalakeName + "1"))
            .withComment("comment")
            .withProperties(props)
            .withAuditInfo(auditInfo)
            .build();

    tagMetaService.insertTag(tagEntity2, false);
    Assertions.assertTrue(
        MetalakeMetaService.getInstance().deleteMetalake(metalake1.nameIdentifier(), true));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> tagMetaService.getTagByIdentifier(
            TagManager.ofTagIdent(metalakeName + "1", "tag2")));
  }
}
