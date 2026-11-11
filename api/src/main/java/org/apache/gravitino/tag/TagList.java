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

import com.google.common.base.Preconditions;
import org.apache.gravitino.annotation.Evolving;

/**
 * A list of visible tags and whether authorization filtering omitted other tags.
 *
 * @param <T> The type of the returned tag names or tag objects.
 */
@Evolving
public final class TagList<T> {

  private final T[] tags;
  private final boolean hasInaccessibleTags;

  /**
   * Creates a tag list.
   *
   * @param tags The visible tag names or tag objects.
   * @param hasInaccessibleTags Whether one or more inaccessible tags were omitted.
   */
  public TagList(T[] tags, boolean hasInaccessibleTags) {
    Preconditions.checkArgument(tags != null, "Tags must not be null");
    this.tags = tags.clone();
    this.hasInaccessibleTags = hasInaccessibleTags;
  }

  /**
   * @return The visible tag names or tag objects.
   */
  public T[] tags() {
    return tags.clone();
  }

  /**
   * @return True if one or more inaccessible tags were omitted, false otherwise.
   */
  public boolean hasInaccessibleTags() {
    return hasInaccessibleTags;
  }
}
