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

import java.util.Objects;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.meta.TagEntity;

/**
 * An effective tag assignment together with the metadata object that contributed it.
 *
 * <p>The source is the nearest object in the inheritance chain that assigned the tag name. Direct
 * assignments on a child override ancestor assignments for the same name.
 */
public final class EffectiveTag {

  private final TagEntity tag;
  private final MetadataObject source;

  /**
   * @param tag effective tag entity, including assignment and inherited flag
   * @param source metadata object that contributed this assignment
   */
  public EffectiveTag(TagEntity tag, MetadataObject source) {
    this.tag = Objects.requireNonNull(tag, "tag");
    this.source = Objects.requireNonNull(source, "source");
  }

  /**
   * @return the effective tag entity
   */
  public TagEntity tag() {
    return tag;
  }

  /**
   * @return the metadata object that contributed this assignment
   */
  public MetadataObject source() {
    return source;
  }
}
