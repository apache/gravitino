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
package org.apache.gravitino.messaging;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;

/**
 * Helpers and conventional names for topic {@link DataLayout} maps.
 *
 * <p>Topic message schemas are modeled as a map from layout name → {@link DataLayout}. Conventional
 * names:
 *
 * <ul>
 *   <li>{@link #KEY} — schema of the record key
 *   <li>{@link #VALUE} — schema of the record value
 * </ul>
 *
 * <p>Additional names are allowed for vendor-specific roles. Persisted layouts are stored under
 * {@link #ENTITY_STORAGE_KEY} inside entity-store topic properties and must never be supplied by
 * clients as a topic property.
 */
@Evolving
public final class DataLayouts {

  /** Conventional layout name for the topic record key. */
  public static final String KEY = "key";

  /** Conventional layout name for the topic record value. */
  public static final String VALUE = "value";

  /**
   * Reserved property key used when serializing named {@link DataLayout}s into entity-store topic
   * properties JSON. Not exposed on user-facing {@link Topic#properties()}.
   */
  public static final String ENTITY_STORAGE_KEY = "gravitino.internal.data-layouts";

  private DataLayouts() {}

  /**
   * Builds a map containing only the value layout.
   *
   * @param valueLayout value layout, must not be null
   * @return immutable map
   */
  public static Map<String, DataLayout> ofValue(DataLayout valueLayout) {
    Objects.requireNonNull(valueLayout, "valueLayout");
    return ImmutableMap.of(VALUE, toSchemaDataLayout(valueLayout));
  }

  /**
   * Builds a map with optional key and value layouts. Null entries are omitted.
   *
   * @param keyLayout key layout, or null
   * @param valueLayout value layout, or null
   * @return immutable map, or null if both are null
   */
  @Nullable
  public static Map<String, DataLayout> of(
      @Nullable DataLayout keyLayout, @Nullable DataLayout valueLayout) {
    if (keyLayout == null && valueLayout == null) {
      return null;
    }
    ImmutableMap.Builder<String, DataLayout> builder = ImmutableMap.builder();
    if (keyLayout != null) {
      builder.put(KEY, toSchemaDataLayout(keyLayout));
    }
    if (valueLayout != null) {
      builder.put(VALUE, toSchemaDataLayout(valueLayout));
    }
    return builder.build();
  }

  /**
   * Copy a {@link DataLayout} into an immutable {@link SchemaDataLayout}.
   *
   * @param layout source layout, may be null
   * @return SchemaDataLayout or null
   */
  @Nullable
  public static SchemaDataLayout toSchemaDataLayout(@Nullable DataLayout layout) {
    if (layout == null) {
      return null;
    }
    if (layout instanceof SchemaDataLayout) {
      return (SchemaDataLayout) layout;
    }
    return SchemaDataLayout.builder()
        .withFormat(layout.format())
        .withTypeName(layout.typeName())
        .withSchemaId(layout.schemaId())
        .withSchemaVersion(layout.schemaVersion())
        .withSchemaUri(layout.schemaUri())
        .withSchemaSubject(layout.schemaSubject())
        .withSchemaText(layout.schemaText())
        .withProperties(layout.properties())
        .build();
  }

  /**
   * Returns an immutable defensive copy of the layout map, or null when empty/null.
   *
   * @param layouts source map
   * @return immutable copy or null
   */
  @Nullable
  public static Map<String, DataLayout> copyOrNull(
      @Nullable Map<String, ? extends DataLayout> layouts) {
    if (layouts == null || layouts.isEmpty()) {
      return null;
    }
    Map<String, DataLayout> copy = new LinkedHashMap<>();
    layouts.forEach(
        (name, layout) -> {
          Preconditions.checkArgument(
              StringUtils.isNotBlank(name), "layout name must not be blank");
          Preconditions.checkArgument(layout != null, "layout for '%s' must not be null", name);
          copy.put(name, toSchemaDataLayout(layout));
        });
    return Collections.unmodifiableMap(copy);
  }

  /**
   * Returns a mutable working copy of layouts (never null).
   *
   * @param layouts source map
   * @return mutable copy
   */
  public static Map<String, DataLayout> mutableCopy(
      @Nullable Map<String, ? extends DataLayout> layouts) {
    Map<String, DataLayout> copy = Maps.newLinkedHashMap();
    if (layouts != null) {
      layouts.forEach((name, layout) -> copy.put(name, toSchemaDataLayout(layout)));
    }
    return copy;
  }

  /**
   * Rejects reserved internal storage keys in user-supplied topic properties.
   *
   * @param properties topic properties, may be null
   * @throws IllegalArgumentException if a reserved key is present
   */
  public static void validateNoReservedProperties(@Nullable Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return;
    }
    Preconditions.checkArgument(
        !properties.containsKey(ENTITY_STORAGE_KEY),
        "Property %s is reserved and cannot be set",
        ENTITY_STORAGE_KEY);
  }

  /**
   * @param change topic change
   * @return true if the change mutates named data layouts
   */
  public static boolean isLayoutChange(TopicChange change) {
    return change instanceof TopicChange.UpdateDataLayout
        || change instanceof TopicChange.RemoveDataLayout
        || change instanceof TopicChange.RemoveDataLayouts;
  }

  /**
   * Applies layout-related changes onto a starting map.
   *
   * @param current current layouts, may be null
   * @param changes topic changes
   * @return resulting layouts, or null when empty
   */
  @Nullable
  public static Map<String, DataLayout> applyChanges(
      @Nullable Map<String, ? extends DataLayout> current, TopicChange... changes) {
    Map<String, DataLayout> working = mutableCopy(current);
    for (TopicChange change : changes) {
      if (change instanceof TopicChange.UpdateDataLayout) {
        TopicChange.UpdateDataLayout update = (TopicChange.UpdateDataLayout) change;
        working.put(update.getName(), toSchemaDataLayout(update.getNewDataLayout()));
      } else if (change instanceof TopicChange.RemoveDataLayout) {
        working.remove(((TopicChange.RemoveDataLayout) change).getName());
      } else if (change instanceof TopicChange.RemoveDataLayouts) {
        working.clear();
      }
    }
    return working.isEmpty() ? null : Collections.unmodifiableMap(working);
  }
}
