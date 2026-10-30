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
package org.apache.gravitino.tag;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves effective tags for a metadata object using direct-assignment override semantics.
 *
 * <p>The requested object is evaluated first, followed by its ancestors from nearest to outermost.
 * The first assignment for a tag name wins, so a direct or nearer assignment overrides a more
 * distant inherited assignment, including its assignment values.
 */
public class EffectiveTagResolver {

  private static final Logger LOG = LoggerFactory.getLogger(EffectiveTagResolver.class);

  private final EntityStore entityStore;

  /**
   * Creates an effective tag resolver.
   *
   * @param entityStore The entity store used to read tag relations.
   */
  public EffectiveTagResolver(EntityStore entityStore) {
    this.entityStore = entityStore;
  }

  /**
   * Resolves effective tags for a metadata object.
   *
   * @param metalake The metalake name.
   * @param metadataObject The metadata object.
   * @return Effective tags in deterministic nearest-assignment order.
   */
  public TagEntity[] resolve(String metalake, MetadataObject metadataObject) {
    ResolvedTag[] resolved = resolveWithSources(metalake, metadataObject);
    TagEntity[] tags = new TagEntity[resolved.length];
    for (int i = 0; i < resolved.length; i++) {
      tags[i] = resolved[i].tag();
    }
    return tags;
  }

  /**
   * Resolves effective tags and the metadata object that contributed each assignment.
   *
   * @param metalake The metalake name.
   * @param metadataObject The metadata object.
   * @return Effective tags with sources in deterministic nearest-assignment order.
   */
  public ResolvedTag[] resolveWithSources(String metalake, MetadataObject metadataObject) {
    MetadataObjectUtil.checkMetadataObject(metalake, metadataObject);
    return resolveWithSources(metadataObject, object -> loadTags(metalake, object));
  }

  /**
   * Resolves nearest-wins assignments using a reader of direct tags.
   *
   * @param metadataObject the target object
   * @param reader the reader of direct assignments, returning a non-null list or throwing
   * @return effective tags, with the target's assignment taking precedence over its ancestors
   */
  public static TagEntity[] resolve(
      MetadataObject metadataObject, Function<MetadataObject, List<TagEntity>> reader) {
    return Arrays.stream(resolveWithSources(metadataObject, reader))
        .map(ResolvedTag::tag)
        .toArray(TagEntity[]::new);
  }

  /**
   * An effective tag assignment together with the metadata object that contributed it.
   *
   * <p>The source is the nearest object in the inheritance chain that assigned the tag name.
   */
  public static final class ResolvedTag {

    private final TagEntity tag;
    private final MetadataObject source;

    /**
     * Creates a resolved tag.
     *
     * @param tag The effective tag entity, including assignment and inherited flag.
     * @param source The metadata object that contributed this assignment.
     */
    public ResolvedTag(TagEntity tag, MetadataObject source) {
      this.tag = Objects.requireNonNull(tag, "tag");
      this.source = Objects.requireNonNull(source, "source");
    }

    /**
     * Returns the effective tag entity.
     *
     * @return The effective tag entity.
     */
    public TagEntity tag() {
      return tag;
    }

    /**
     * Returns the metadata object that contributed this assignment.
     *
     * @return The contributing metadata object.
     */
    public MetadataObject source() {
      return source;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ResolvedTag)) {
        return false;
      }
      ResolvedTag that = (ResolvedTag) o;
      return Objects.equals(tag, that.tag) && Objects.equals(source, that.source);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tag, source);
    }

    @Override
    public String toString() {
      return "ResolvedTag{tag=" + tag + ", source=" + source + '}';
    }
  }

  /**
   * Resolves effective tags for multiple objects, reading each distinct ancestor only once.
   *
   * @param metalake the metalake name
   * @param metadataObjects the objects to resolve
   * @return effective tags keyed by requested object
   */
  public Map<MetadataObject, TagEntity[]> resolve(
      String metalake, List<MetadataObject> metadataObjects) {
    Map<MetadataObject, TagEntity[]> result = new LinkedHashMap<>();
    resolveWithSources(metalake, metadataObjects)
        .forEach(
            (object, tags) ->
                result.put(
                    object, Arrays.stream(tags).map(ResolvedTag::tag).toArray(TagEntity[]::new)));
    return result;
  }

  /**
   * Resolves effective tags with their sources, reading each distinct ancestor only once.
   *
   * @param metalake the metalake name
   * @param metadataObjects the objects to resolve
   * @return effective tags with sources keyed by requested object
   */
  public Map<MetadataObject, ResolvedTag[]> resolveWithSources(
      String metalake, List<MetadataObject> metadataObjects) {
    if (metadataObjects.isEmpty()) {
      return new LinkedHashMap<>();
    }
    MetadataObjectUtil.checkMetadataObjects(metalake, metadataObjects);
    Map<MetadataObject, List<MetadataObject>> resolutionOrders = new LinkedHashMap<>();
    Map<Entity.EntityType, Set<MetadataObject>> objectsByType =
        new EnumMap<>(Entity.EntityType.class);
    for (MetadataObject requested : metadataObjects) {
      List<MetadataObject> order = new ArrayList<>();
      order.add(requested);
      order.addAll(MetadataObjectUtil.getParentMetadataObjects(requested));
      resolutionOrders.put(requested, order);
      for (MetadataObject object : order) {
        objectsByType
            .computeIfAbsent(
                MetadataObjectUtil.toEntityType(object), ignored -> new LinkedHashSet<>())
            .add(object);
      }
    }

    Map<MetadataObject, List<TagEntity>> directTags = new LinkedHashMap<>();
    for (Map.Entry<Entity.EntityType, Set<MetadataObject>> entry : objectsByType.entrySet()) {
      List<NameIdentifier> identifiers = new ArrayList<>();
      Map<NameIdentifier, MetadataObject> objectsByIdentifier = new LinkedHashMap<>();
      for (MetadataObject object : entry.getValue()) {
        NameIdentifier identifier = MetadataObjectUtil.toEntityIdent(metalake, object);
        identifiers.add(identifier);
        objectsByIdentifier.put(identifier, object);
      }
      try {
        for (RelationalEntity<?> relation :
            entityStore
                .relationOperations()
                .batchListEntitiesByRelation(
                    SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
                    identifiers,
                    entry.getKey())) {
          directTags
              .computeIfAbsent(
                  objectsByIdentifier.get(relation.source()), ignored -> new ArrayList<>())
              .add((TagEntity) relation.targetEntity());
        }
      } catch (NoSuchEntityException e) {
        throw new NoSuchMetadataObjectException(
            e, "Failed to resolve effective tags for metadata objects due to not found");
      } catch (IOException e) {
        LOG.error("Failed to resolve effective tags for metadata objects", e);
        throw new RuntimeException(e);
      }
    }

    Map<MetadataObject, ResolvedTag[]> result = new LinkedHashMap<>();
    resolutionOrders.forEach(
        (requested, order) -> {
          Map<String, ResolvedTag> effectiveTags = new LinkedHashMap<>();
          for (int index = 0; index < order.size(); index++) {
            boolean inherited = index > 0;
            MetadataObject source = order.get(index);
            directTags
                .getOrDefault(source, Collections.emptyList())
                .forEach(
                    tag ->
                        effectiveTags.putIfAbsent(
                            tag.name(), new ResolvedTag(tag.copyWithInherited(inherited), source)));
          }
          result.put(requested, effectiveTags.values().toArray(new ResolvedTag[0]));
        });
    return result;
  }

  private static ResolvedTag[] resolveWithSources(
      MetadataObject metadataObject, Function<MetadataObject, List<TagEntity>> reader) {
    List<MetadataObject> resolutionOrder = new ArrayList<>();
    resolutionOrder.add(metadataObject);
    resolutionOrder.addAll(MetadataObjectUtil.getParentMetadataObjects(metadataObject));

    Map<String, ResolvedTag> effectiveTags = new LinkedHashMap<>();
    for (int index = 0; index < resolutionOrder.size(); index++) {
      MetadataObject object = resolutionOrder.get(index);
      boolean inherited = index > 0;
      reader
          .apply(object)
          .forEach(
              tag ->
                  effectiveTags.putIfAbsent(
                      tag.name(), new ResolvedTag(tag.copyWithInherited(inherited), object)));
    }
    return effectiveTags.values().toArray(new ResolvedTag[0]);
  }

  private List<TagEntity> loadTags(String metalake, MetadataObject object) {
    NameIdentifier identifier = MetadataObjectUtil.toEntityIdent(metalake, object);
    Entity.EntityType entityType = MetadataObjectUtil.toEntityType(object);
    try {
      return entityStore
          .relationOperations()
          .listEntitiesByRelation(
              SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL, identifier, entityType);
    } catch (NoSuchEntityException e) {
      throw new NoSuchMetadataObjectException(
          e, "Failed to resolve effective tags for metadata object %s due to not found", object);
    } catch (IOException e) {
      LOG.error("Failed to resolve effective tags for metadata object {}", object, e);
      throw new RuntimeException(e);
    }
  }
}
