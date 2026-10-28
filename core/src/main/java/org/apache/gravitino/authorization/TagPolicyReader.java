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

package org.apache.gravitino.authorization;

import java.util.List;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.meta.TagEntity;

/** Reads tag assignments and policy bindings from storage or a list preload. */
public interface TagPolicyReader {
  /**
   * Loads only the tags directly assigned to an object, including their assignment values.
   *
   * @param metalake the containing metalake
   * @param object the canonical object whose direct assignments are requested
   * @return direct assignments, empty if none exist
   * @throws RuntimeException if the assignments cannot be read
   */
  List<TagEntity> loadTags(String metalake, MetadataObject object);

  /**
   * Loads policy bindings for effective tags resolved at one target object.
   *
   * @param metalake the containing metalake
   * @param effectiveTags the target object's effective tags, one per tag name
   * @return bindings paired with the supplied effective tags, empty if none exist
   * @throws RuntimeException if the bindings cannot be read
   */
  List<ResolvedTagPolicy> loadPolicyBindings(String metalake, List<TagEntity> effectiveTags);
}
