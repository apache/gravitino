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

package org.apache.gravitino.kb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.kb.KnowledgeBaseArticleEntity.Binding;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.storage.relational.RelationalEntityStoreIdResolver;
import org.apache.gravitino.storage.relational.mapper.KnowledgeBaseArticleMapper;
import org.apache.gravitino.storage.relational.mapper.KnowledgeBaseArticleMapper.AssetRow;
import org.apache.gravitino.storage.relational.service.LiveEndpointService;
import org.apache.gravitino.storage.relational.service.LiveEndpointService.Endpoint;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;

/** Plans Article asset edits and projects live, readable bindings in stable ID order. */
final class ArticleAssets {
  private static final int BATCH_SIZE = 128;
  private final String metalake;
  private final boolean authorization;

  ArticleAssets(String metalake, boolean authorization) {
    this.metalake = metalake;
    this.authorization = authorization;
  }

  record Edit(boolean add, List<ArticleObjectIdentifier> objects) {}

  record Plan(List<Edit> edits, Map<ArticleObjectIdentifier, Endpoint> targets) {
    Set<Binding> apply(Set<Binding> before) {
      Set<Binding> result = new HashSet<>(before);
      for (Edit edit : edits) {
        for (var object : edit.objects()) {
          Binding binding = new Binding(object.type().name(), targets.get(object).ids().entityId());
          if (edit.add()) {
            result.add(binding);
          } else {
            result.remove(binding);
          }
        }
      }
      return result;
    }
  }

  static List<Edit> edits(KnowledgeBaseArticleChange[] changes) {
    List<Edit> result = new ArrayList<>();
    for (var change : changes) {
      if (change instanceof KnowledgeBaseArticleChange.AddObjects add) {
        result.add(new Edit(true, add.getObjects()));
      } else if (change instanceof KnowledgeBaseArticleChange.RemoveObjects remove) {
        result.add(new Edit(false, remove.getObjects()));
      }
    }
    return result;
  }

  Plan plan(List<Edit> edits) {
    Map<ArticleObjectIdentifier, Endpoint> targets = new HashMap<>();
    edits.forEach(
        edit -> edit.objects().forEach(object -> targets.computeIfAbsent(object, this::resolve)));
    return new Plan(edits, targets);
  }

  Set<Binding> lock(long lake, Plan plan) {
    if (plan.targets().isEmpty()) {
      return Set.of();
    }
    try {
      LiveEndpointService.lockLiveEndpoints(plan.targets().values());
    } catch (NoSuchEntityException missing) {
      throw new NoSuchMetadataObjectException(missing, "Article binding target does not exist");
    }
    Set<Binding> locked = new HashSet<>();
    plan.targets().forEach((object, endpoint) -> locked.add(check(lake, object, endpoint.ids())));
    return locked;
  }

  Binding readableTarget(long lake, ArticleObjectIdentifier object) {
    return check(lake, object, resolve(object).ids());
  }

  record Window(AssetBindingPage page, Set<Binding> ids) {}

  KnowledgeBaseArticleEntity preview(
      long lake, KnowledgeBaseArticleEntity article, int limit, int offset) {
    var window = window(lake, article.id(), limit, offset);
    Set<Binding> bindings = new HashSet<>(window.ids());
    article.bindings().stream()
        .filter(b -> "GLOSSARY_TERM".equals(b.targetType()))
        .forEach(bindings::add);
    return article.toBuilder()
        .withBindings(bindings)
        .withObjects(window.page().objects())
        .withObjectsHasMore(window.page().hasMore())
        .build();
  }

  Window window(long lake, long article, int limit, int offset) {
    List<ArticleObjectIdentifier> objects = new ArrayList<>();
    Set<Binding> visible = new HashSet<>();
    Binding cursor = null;
    int remaining = offset;
    while (true) {
      Binding after = cursor;
      var rows =
          SessionUtils.getWithoutCommit(
              KnowledgeBaseArticleMapper.class,
              mapper -> mapper.boundAssets(lake, article, after, BATCH_SIZE));
      Map<Binding, Target> live = liveTargets(lake, rows);
      for (Binding binding : rows) {
        Target target = live.get(binding);
        if (target == null
            || !ArticleAssetAccess.readable(
                metalake, authorization, target.object(), target.ids())) {
          continue;
        }
        if (remaining > 0) {
          remaining--;
        } else if (objects.size() == limit) {
          return new Window(new AssetBindingPage(objects, true), visible);
        } else {
          objects.add(target.object());
          visible.add(binding);
        }
      }
      if (rows.size() < BATCH_SIZE) {
        return new Window(new AssetBindingPage(objects, false), visible);
      }
      cursor = rows.get(rows.size() - 1);
    }
  }

  record Target(ArticleObjectIdentifier object, NamespacedEntityId ids) {}

  private Endpoint resolve(ArticleObjectIdentifier object) {
    List<String> components = new ArrayList<>();
    components.add(metalake);
    components.addAll(object.namespace());
    components.add(object.name());
    NameIdentifier name = NameIdentifier.of(components.toArray(String[]::new));
    EntityType type = EntityType.valueOf(object.type().name());
    try {
      return new Endpoint(
          name, type, new RelationalEntityStoreIdResolver().getEntityIds(name, type));
    } catch (NoSuchEntityException missing) {
      throw new NoSuchMetadataObjectException("Article binding target %s does not exist", object);
    }
  }

  private Binding check(long lake, ArticleObjectIdentifier object, NamespacedEntityId ids) {
    if (ids.namespaceIds()[0] != lake
        || !ArticleAssetAccess.readable(metalake, authorization, object, ids)) {
      throw new NoSuchMetadataObjectException("Article binding target %s does not exist", object);
    }
    return new Binding(object.type().name(), ids.entityId());
  }

  private static Map<Binding, Target> liveTargets(long lake, List<Binding> rows) {
    Map<Binding, Target> result = new HashMap<>();
    String separator = HierarchicalSchemaUtil.schemaSeparator();
    var groups =
        rows.stream()
            .collect(
                Collectors.groupingBy(
                    Binding::targetType,
                    Collectors.mapping(Binding::targetId, Collectors.toList())));
    groups.forEach(
        (name, ids) -> {
          MetadataObject.Type type = MetadataObject.Type.valueOf(name);
          for (AssetRow row :
              SessionUtils.getWithoutCommit(
                  KnowledgeBaseArticleMapper.class, mapper -> mapper.liveAssets(lake, type, ids))) {
            List<String> parents = new ArrayList<>();
            List<Long> chain = new ArrayList<>(List.of(lake));
            if (row.catalogId() != null) {
              parents.add(row.catalogName());
              chain.add(row.catalogId());
            }
            if (row.schemaId() != null) {
              parents.add(HierarchicalSchemaUtil.physicalToLogical(row.schemaName(), separator));
              chain.add(row.schemaId());
            }
            if (row.tableId() != null) {
              parents.add(row.tableName());
              chain.add(row.tableId());
            }
            String targetName =
                type == MetadataObject.Type.SCHEMA
                    ? HierarchicalSchemaUtil.physicalToLogical(row.name(), separator)
                    : row.name();
            result.put(
                new Binding(name, row.id()),
                new Target(
                    ArticleObjectIdentifier.of(type, parents, targetName),
                    new NamespacedEntityId(
                        row.id(), chain.stream().mapToLong(Long::longValue).toArray())));
          }
        });
    return result;
  }
}
