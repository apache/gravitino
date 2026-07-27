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
package org.apache.gravitino.storage.relational.service;

import static org.apache.gravitino.metrics.source.MetricsSource.GRAVITINO_RELATIONAL_STORE_METRIC_NAME;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.GenericEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.metrics.Monitored;
import org.apache.gravitino.storage.relational.mapper.TagMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TagMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.po.TagMetadataObjectRelPO;
import org.apache.gravitino.storage.relational.po.TagPO;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.tag.TagValuePair;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;

public class TagMetaService {

  private static final TagMetaService INSTANCE = new TagMetaService();

  public static TagMetaService getInstance() {
    return INSTANCE;
  }

  private TagMetaService() {}

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listTagsByNamespace")
  public List<TagEntity> listTagsByNamespace(Namespace ns) {
    String metalakeName = ns.level(0);
    List<TagPO> tagPOs =
        SessionUtils.getWithoutCommit(
            TagMetaMapper.class, mapper -> mapper.listTagPOsByMetalake(metalakeName));
    return tagPOs.stream()
        .map(tagPO -> POConverters.fromTagPO(tagPO, ns))
        .collect(Collectors.toList());
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getTagByIdentifier")
  public TagEntity getTagByIdentifier(NameIdentifier ident) {
    String metalakeName = ident.namespace().level(0);
    TagPO tagPO = getTagPOByMetalakeAndName(metalakeName, ident.name());
    return POConverters.fromTagPO(tagPO, ident.namespace());
  }

  @Monitored(metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME, baseMetricName = "insertTag")
  public void insertTag(TagEntity tagEntity, boolean overwritten) throws IOException {
    Namespace ns = tagEntity.namespace();
    String metalakeName = ns.level(0);

    try {
      Long metalakeId = MetalakeMetaService.getInstance().getMetalakeIdByName(metalakeName);

      TagPO.Builder builder = TagPO.builder().withMetalakeId(metalakeId);
      TagPO tagPO = POConverters.initializeTagPOWithVersion(tagEntity, builder);

      SessionUtils.doWithCommit(
          TagMetaMapper.class,
          mapper -> {
            if (overwritten) {
              mapper.insertTagMetaOnDuplicateKeyUpdate(tagPO);
            } else {
              mapper.insertTagMeta(tagPO);
            }
          });
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.TAG, tagEntity.toString());
      throw e;
    }
  }

  @Monitored(metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME, baseMetricName = "updateTag")
  public <E extends Entity & HasIdentifier> TagEntity updateTag(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
    String metalakeName = identifier.namespace().level(0);

    try {
      TagPO tagPO = getTagPOByMetalakeAndName(metalakeName, identifier.name());
      TagEntity oldTagEntity = POConverters.fromTagPO(tagPO, identifier.namespace());
      TagEntity updatedTagEntity = (TagEntity) updater.apply((E) oldTagEntity);
      Preconditions.checkArgument(
          Objects.equals(oldTagEntity.id(), updatedTagEntity.id()),
          "The updated tag entity id: %s must have the same id as the old entity id %s",
          updatedTagEntity.id(),
          oldTagEntity.id());

      Integer result =
          SessionUtils.doWithCommitAndFetchResult(
              TagMetaMapper.class,
              mapper ->
                  mapper.updateTagMeta(
                      POConverters.updateTagPOWithVersion(tagPO, updatedTagEntity), tagPO));

      if (result == null || result == 0) {
        throw new IOException("Failed to update the entity: " + identifier);
      }

      return updatedTagEntity;

    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.TAG, identifier.toString());
      throw e;
    }
  }

  @Monitored(metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME, baseMetricName = "deleteTag")
  public boolean deleteTag(NameIdentifier identifier) {
    String metalakeName = identifier.namespace().level(0);
    int[] tagDeletedCount = new int[] {0};
    int[] tagMetadataObjectRelDeletedCount = new int[] {0};

    SessionUtils.doMultipleWithCommit(
        () ->
            tagDeletedCount[0] =
                SessionUtils.getWithoutCommit(
                    TagMetaMapper.class,
                    mapper ->
                        mapper.softDeleteTagMetaByMetalakeAndTagName(
                            metalakeName, identifier.name())),
        () ->
            tagMetadataObjectRelDeletedCount[0] =
                SessionUtils.getWithoutCommit(
                    TagMetadataObjectRelMapper.class,
                    mapper ->
                        mapper.softDeleteTagMetadataObjectRelsByMetalakeAndTagName(
                            metalakeName, identifier.name())));

    return tagDeletedCount[0] + tagMetadataObjectRelDeletedCount[0] > 0;
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listTagsForMetadataObject")
  public List<TagEntity> listTagsForMetadataObject(
      NameIdentifier objectIdent, Entity.EntityType objectType)
      throws NoSuchTagException, IOException {
    MetadataObject metadataObject = NameIdentifierUtil.toMetadataObject(objectIdent, objectType);
    String metalake = objectIdent.namespace().level(0);

    List<TagPO> tagPOs = null;
    try {
      Long metadataObjectId = EntityIdService.getEntityId(objectIdent, objectType);

      tagPOs =
          SessionUtils.getWithoutCommit(
              TagMetadataObjectRelMapper.class,
              mapper ->
                  mapper.listTagPOsByMetadataObjectIdAndType(
                      metadataObjectId, metadataObject.type().toString()));
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.TAG, objectIdent.toString());
      throw e;
    }

    return tagPOsToTagEntitiesWithAssignmentValues(tagPOs, NamespaceUtil.ofTag(metalake));
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "getTagForMetadataObject")
  public TagEntity getTagForMetadataObject(
      NameIdentifier objectIdent, Entity.EntityType objectType, NameIdentifier tagIdent)
      throws NoSuchEntityException, IOException {
    MetadataObject metadataObject = NameIdentifierUtil.toMetadataObject(objectIdent, objectType);
    String metalake = objectIdent.namespace().level(0);

    List<TagPO> tagPOs = null;
    try {
      Long metadataObjectId = EntityIdService.getEntityId(objectIdent, objectType);

      tagPOs =
          SessionUtils.getWithoutCommit(
              TagMetadataObjectRelMapper.class,
              mapper ->
                  mapper.getTagPOsByMetadataObjectAndTagName(
                      metadataObjectId, metadataObject.type().toString(), tagIdent.name()));
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.TAG, tagIdent.toString());
      throw e;
    }

    if (tagPOs == null || tagPOs.isEmpty()) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.TAG.name().toLowerCase(),
          tagIdent.name());
    }

    return tagPOsToTagEntitiesWithAssignmentValues(tagPOs, NamespaceUtil.ofTag(metalake)).get(0);
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listAssociatedMetadataObjectsForTag")
  public List<GenericEntity> listAssociatedMetadataObjectsForTag(NameIdentifier tagIdent)
      throws IOException {
    return listAssociatedMetadataObjectsForTag(tagIdent, null);
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "listAssociatedMetadataObjectsForTagByValue")
  public List<GenericEntity> listAssociatedMetadataObjectsForTag(
      NameIdentifier tagIdent, @Nullable String value) throws IOException {
    String metalakeName = tagIdent.namespace().level(0);
    String tagName = tagIdent.name();

    try {
      List<TagMetadataObjectRelPO> tagMetadataObjectRelPOs =
          SessionUtils.doWithCommitAndFetchResult(
              TagMetadataObjectRelMapper.class,
              mapper ->
                  value == null
                      ? mapper.listTagMetadataObjectRelsByMetalakeAndTagName(metalakeName, tagName)
                      : mapper.listTagMetadataObjectRelsByMetalakeAndTagNameAndValue(
                          metalakeName, tagName, value));

      List<GenericEntity> metadataObjects = Lists.newArrayList();
      Map<String, List<TagMetadataObjectRelPO>> tagMetadataObjectRelPOsByType =
          tagMetadataObjectRelPOs.stream()
              .collect(Collectors.groupingBy(TagMetadataObjectRelPO::getMetadataObjectType));

      for (Map.Entry<String, List<TagMetadataObjectRelPO>> entry :
          tagMetadataObjectRelPOsByType.entrySet()) {
        String metadataObjectType = entry.getKey();
        List<TagMetadataObjectRelPO> rels = entry.getValue();

        List<Long> metadataObjectIds =
            rels.stream()
                .map(TagMetadataObjectRelPO::getMetadataObjectId)
                .collect(Collectors.toList());
        Map<Long, String> metadataObjectNames =
            MetadataObjectService.TYPE_TO_FULLNAME_FUNCTION_MAP
                .get(MetadataObject.Type.valueOf(metadataObjectType))
                .apply(metadataObjectIds);

        for (Map.Entry<Long, String> metadataObjectName : metadataObjectNames.entrySet()) {
          String fullName = metadataObjectName.getValue();

          // Metadata object may be deleted asynchronously when we query the name, so it will
          // return null, we should skip this metadata object.
          if (fullName != null) {
            metadataObjects.add(
                GenericEntity.builder()
                    .withName(fullName)
                    .withEntityType(Entity.EntityType.valueOf(metadataObjectType))
                    .withId(metadataObjectName.getKey())
                    .build());
          }
        }
      }

      return metadataObjects;
    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.TAG, tagIdent.toString());
      throw e;
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "associateTagsWithMetadataObject")
  public List<TagEntity> associateTagsWithMetadataObject(
      NameIdentifier objectIdent,
      Entity.EntityType objectType,
      NameIdentifier[] tagsToAdd,
      NameIdentifier[] tagsToRemove)
      throws NoSuchEntityException, EntityAlreadyExistsException, IOException {
    return associateTagValuesWithMetadataObject(
        objectIdent, objectType, toValuelessPairs(tagsToAdd), toValuelessPairs(tagsToRemove));
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "associateTagValuesWithMetadataObject")
  public List<TagEntity> associateTagValuesWithMetadataObject(
      NameIdentifier objectIdent,
      Entity.EntityType objectType,
      TagValuePair[] tagsToAdd,
      TagValuePair[] tagsToRemove)
      throws NoSuchEntityException, EntityAlreadyExistsException, IOException {
    MetadataObject metadataObject = NameIdentifierUtil.toMetadataObject(objectIdent, objectType);
    String metalake = objectIdent.namespace().level(0);

    try {
      Long metadataObjectId = EntityIdService.getEntityId(objectIdent, objectType);
      List<TagValuePair> pairsToAdd = Arrays.asList(nullToEmpty(tagsToAdd));
      List<TagValuePair> pairsToRemove = Arrays.asList(nullToEmpty(tagsToRemove));

      List<String> tagNamesToUpdate = tagNamesToUpdate(pairsToAdd, pairsToRemove);
      List<TagPO> tagPOsToUpdate =
          tagNamesToUpdate.isEmpty()
              ? Collections.emptyList()
              : getTagPOsByMetalakeAndNames(metalake, tagNamesToUpdate);
      Map<String, TagPO> tagPOsByName = tagPOsByName(tagPOsToUpdate);

      List<TagPO> currentTagPOs =
          SessionUtils.getWithoutCommit(
              TagMetadataObjectRelMapper.class,
              mapper ->
                  mapper.listTagPOsByMetadataObjectIdAndType(
                      metadataObjectId, metadataObject.type().toString()));
      Map<Long, Set<Optional<String>>> activeValuesByTagId = new LinkedHashMap<>();
      Map<Long, Integer> maxValueOrderByTagId = new LinkedHashMap<>();
      for (TagPO currentTagPO : currentTagPOs) {
        trackExistingAssignment(currentTagPO, activeValuesByTagId, maxValueOrderByTagId);
      }

      List<Long> tagIdsToRemove = new ArrayList<>();
      List<TagMetadataObjectRelPO> tagRelsToRemove = new ArrayList<>();
      for (TagValuePair pairToRemove : pairsToRemove) {
        TagPO tagPO = tagPOsByName.get(pairToRemove.name());
        if (tagPO == null) {
          continue;
        }

        if (pairToRemove.value().isPresent()) {
          activeValuesByTagId
              .computeIfAbsent(tagPO.getTagId(), ignored -> new LinkedHashSet<>())
              .remove(pairToRemove.value());
          tagRelsToRemove.add(
              tagRelForValue(tagPO, metadataObjectId, metadataObject, pairToRemove));
        } else {
          activeValuesByTagId.remove(tagPO.getTagId());
          maxValueOrderByTagId.remove(tagPO.getTagId());
          tagIdsToRemove.add(tagPO.getTagId());
        }
      }

      List<TagMetadataObjectRelPO> tagRelsToAdd = new ArrayList<>();
      for (TagValuePair pairToAdd : pairsToAdd) {
        TagPO tagPO = tagPOsByName.get(pairToAdd.name());
        if (tagPO == null) {
          continue;
        }

        validateAllowedValue(tagPO, pairToAdd);
        Set<Optional<String>> activeValues =
            activeValuesByTagId.computeIfAbsent(tagPO.getTagId(), ignored -> new LinkedHashSet<>());
        Optional<String> value = pairToAdd.value();
        if (activeValues.contains(value)) {
          continue;
        }

        if (value.isPresent()) {
          if (activeValues.remove(Optional.empty())) {
            tagRelsToRemove.add(
                tagRelForValue(
                    tagPO,
                    metadataObjectId,
                    metadataObject,
                    TagValuePair.valueless(pairToAdd.name())));
          }
        } else {
          Preconditions.checkArgument(
              activeValues.isEmpty(),
              "Cannot add valueless tag %s while valued assignments are active",
              pairToAdd.name());
        }

        int nextValueOrder = 0;
        if (value.isPresent()) {
          nextValueOrder = maxValueOrderByTagId.getOrDefault(tagPO.getTagId(), 0) + 1;
          maxValueOrderByTagId.put(tagPO.getTagId(), nextValueOrder);
        }
        activeValues.add(value);
        tagRelsToAdd.add(
            POConverters.initializeTagMetadataObjectRelPOWithVersion(
                tagPO.getTagId(),
                metadataObjectId,
                metadataObject.type().toString(),
                pairToAdd.getValue(),
                nextValueOrder));
      }

      SessionUtils.doMultipleWithCommit(
          () -> {
            if (tagIdsToRemove.isEmpty()) {
              return;
            }

            SessionUtils.doWithoutCommit(
                TagMetadataObjectRelMapper.class,
                mapper ->
                    mapper.batchDeleteTagMetadataObjectRelsByTagIdsAndMetadataObject(
                        metadataObjectId, metadataObject.type().toString(), tagIdsToRemove));
          },
          () -> {
            if (tagRelsToRemove.isEmpty()) {
              return;
            }

            SessionUtils.doWithoutCommit(
                TagMetadataObjectRelMapper.class,
                mapper ->
                    mapper.batchDeleteTagMetadataObjectRelsByTagIdsAndValuesAndMetadataObject(
                        metadataObjectId, metadataObject.type().toString(), tagRelsToRemove));
          },
          () -> {
            if (tagRelsToAdd.isEmpty()) {
              return;
            }

            SessionUtils.doWithoutCommit(
                TagMetadataObjectRelMapper.class,
                mapper -> mapper.batchInsertTagMetadataObjectRels(tagRelsToAdd));
          });

      List<TagPO> tagPOs =
          SessionUtils.getWithoutCommit(
              TagMetadataObjectRelMapper.class,
              mapper ->
                  mapper.listTagPOsByMetadataObjectIdAndType(
                      metadataObjectId, metadataObject.type().toString()));

      return tagPOsToTagEntitiesWithAssignmentValues(tagPOs, NamespaceUtil.ofTag(metalake));

    } catch (RuntimeException e) {
      ExceptionUtils.checkSQLException(e, Entity.EntityType.TAG, objectIdent.toString());
      throw e;
    }
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "deleteTagMetasByLegacyTimeline")
  public int deleteTagMetasByLegacyTimeline(long legacyTimeline, int limit) {
    int[] tagDeletedCount = new int[] {0};
    int[] tagMetadataObjectRelDeletedCount = new int[] {0};

    SessionUtils.doMultipleWithCommit(
        () ->
            tagDeletedCount[0] =
                SessionUtils.getWithoutCommit(
                    TagMetaMapper.class,
                    mapper -> mapper.deleteTagMetasByLegacyTimeline(legacyTimeline, limit)),
        () ->
            tagMetadataObjectRelDeletedCount[0] =
                SessionUtils.getWithoutCommit(
                    TagMetadataObjectRelMapper.class,
                    mapper -> mapper.deleteTagEntityRelsByLegacyTimeline(legacyTimeline, limit)));

    return tagDeletedCount[0] + tagMetadataObjectRelDeletedCount[0];
  }

  private static List<TagEntity> tagPOsToTagEntitiesWithAssignmentValues(
      List<TagPO> tagPOs, Namespace namespace) {
    Map<Long, List<TagPO>> tagPOsByTagId = new LinkedHashMap<>();
    for (TagPO tagPO : tagPOs) {
      tagPOsByTagId.computeIfAbsent(tagPO.getTagId(), ignored -> new ArrayList<>()).add(tagPO);
    }

    return tagPOsByTagId.values().stream()
        .map(tagPOGroup -> tagPOsToTagEntityWithAssignmentValues(tagPOGroup, namespace))
        .collect(Collectors.toList());
  }

  private static TagEntity tagPOsToTagEntityWithAssignmentValues(
      List<TagPO> tagPOGroup, Namespace namespace) {
    TagPO firstTagPO = tagPOGroup.get(0);
    String[] assignmentValues =
        tagPOGroup.stream()
            .filter(tagPO -> tagPO.getAssignmentValue() != null)
            .sorted(
                Comparator.comparing(
                        (TagPO tagPO) -> tagPO.getValueOrder() == null ? 0 : tagPO.getValueOrder())
                    .thenComparing(TagPO::getAssignmentValue))
            .map(TagPO::getAssignmentValue)
            .distinct()
            .toArray(String[]::new);

    TagEntity tagEntity = POConverters.fromTagPO(firstTagPO, namespace);
    return TagEntity.builder()
        .withId(tagEntity.id())
        .withName(tagEntity.name())
        .withNamespace(tagEntity.namespace())
        .withComment(tagEntity.comment())
        .withProperties(tagEntity.properties())
        .withAllowedValues(tagEntity.allowedValues().orElse(null))
        .withAssignmentValues(assignmentValues.length == 0 ? null : assignmentValues)
        .withAuditInfo(tagEntity.auditInfo())
        .build();
  }

  private static TagValuePair[] toValuelessPairs(NameIdentifier[] tags) {
    if (tags == null) {
      return null;
    }

    return Arrays.stream(tags)
        .map(tag -> TagValuePair.valueless(tag.name()))
        .toArray(TagValuePair[]::new);
  }

  private static TagValuePair[] nullToEmpty(TagValuePair[] tagPairs) {
    return tagPairs == null ? new TagValuePair[0] : tagPairs;
  }

  private static List<String> tagNamesToUpdate(
      List<TagValuePair> tagsToAdd, List<TagValuePair> tagsToRemove) {
    Set<String> tagNames = new LinkedHashSet<>();
    tagsToAdd.stream().map(TagValuePair::name).forEach(tagNames::add);
    tagsToRemove.stream().map(TagValuePair::name).forEach(tagNames::add);
    return new ArrayList<>(tagNames);
  }

  private static Map<String, TagPO> tagPOsByName(List<TagPO> tagPOs) {
    Map<String, TagPO> tagPOsByName = new LinkedHashMap<>();
    for (TagPO tagPO : tagPOs) {
      tagPOsByName.put(tagPO.getTagName(), tagPO);
    }
    return tagPOsByName;
  }

  private static void trackExistingAssignment(
      TagPO tagPO,
      Map<Long, Set<Optional<String>>> activeValuesByTagId,
      Map<Long, Integer> maxValueOrderByTagId) {
    activeValuesByTagId
        .computeIfAbsent(tagPO.getTagId(), ignored -> new LinkedHashSet<>())
        .add(Optional.ofNullable(tagPO.getAssignmentValue()));
    maxValueOrderByTagId.compute(
        tagPO.getTagId(),
        (ignored, currentMax) ->
            Math.max(
                currentMax == null ? 0 : currentMax,
                tagPO.getValueOrder() == null ? 0 : tagPO.getValueOrder()));
  }

  private static TagMetadataObjectRelPO tagRelForValue(
      TagPO tagPO, Long metadataObjectId, MetadataObject metadataObject, TagValuePair pair) {
    return POConverters.initializeTagMetadataObjectRelPOWithVersion(
        tagPO.getTagId(), metadataObjectId, metadataObject.type().toString(), pair.getValue(), 0);
  }

  private static void validateAllowedValue(TagPO tagPO, TagValuePair tagValuePair)
      throws JsonProcessingException {
    if (tagPO.getAllowedValues() == null) {
      return;
    }

    String[] allowedValues =
        JsonUtils.anyFieldMapper().readValue(tagPO.getAllowedValues(), String[].class);
    if (!tagValuePair.value().isPresent()) {
      return;
    }

    Preconditions.checkArgument(
        allowedValues.length > 0, "Tag %s does not allow assignment values", tagValuePair.name());
    Preconditions.checkArgument(
        Arrays.asList(allowedValues).contains(tagValuePair.value().get()),
        "Tag %s value %s is not in allowed values %s",
        tagValuePair.name(),
        tagValuePair.value().get(),
        Arrays.toString(allowedValues));
  }

  private TagPO getTagPOByMetalakeAndName(String metalakeName, String tagName) {
    TagPO tagPO =
        SessionUtils.getWithoutCommit(
            TagMetaMapper.class,
            mapper -> mapper.selectTagMetaByMetalakeAndName(metalakeName, tagName));

    if (tagPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.TAG.name().toLowerCase(),
          tagName);
    }
    return tagPO;
  }

  public Long getTagIdByTagName(Long metalakeId, String tagName) {
    TagPO tagPO =
        SessionUtils.getWithoutCommit(
            TagMetaMapper.class,
            mapper -> mapper.selectTagMetaByMetalakeIdAndName(metalakeId, tagName));

    if (tagPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE,
          Entity.EntityType.TAG.name().toLowerCase(),
          tagName);
    }
    return tagPO.getTagId();
  }

  private List<TagPO> getTagPOsByMetalakeAndNames(String metalakeName, List<String> tagNames) {
    return SessionUtils.getWithoutCommit(
        TagMetaMapper.class,
        mapper -> mapper.listTagPOsByMetalakeAndTagNames(metalakeName, tagNames));
  }

  @Monitored(
      metricsSource = GRAVITINO_RELATIONAL_STORE_METRIC_NAME,
      baseMetricName = "batchGetTagByIdentifier")
  public List<TagEntity> batchGetTagByIdentifier(List<NameIdentifier> identifiers) {
    NameIdentifier firstIdent = identifiers.get(0);
    String metalakeName = firstIdent.namespace().level(0);
    List<String> tagNames =
        identifiers.stream().map(NameIdentifier::name).collect(Collectors.toList());

    return SessionUtils.doWithCommitAndFetchResult(
        TagMetaMapper.class,
        mapper -> {
          List<TagPO> tagPOs = mapper.batchSelectTagByIdentifier(metalakeName, tagNames);
          return POConverters.fromTagPOs(tagPOs, firstIdent.namespace());
        });
  }
}
