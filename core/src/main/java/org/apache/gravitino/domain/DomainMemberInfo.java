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
package org.apache.gravitino.domain;

import java.util.Objects;
import org.apache.gravitino.MetadataObject;

/** An immutable member and its relations to the queried domain. */
public final class DomainMemberInfo implements DomainMember {
  private final MetadataObject object;
  private final DomainRelation[] relations;

  /**
   * Creates a member query result.
   *
   * @param object The leaf metadata object.
   * @param relations All relations to the queried domain.
   */
  public DomainMemberInfo(MetadataObject object, DomainRelation[] relations) {
    this.object = Objects.requireNonNull(object, "object");
    this.relations = Objects.requireNonNull(relations, "relations").clone();
  }

  @Override
  public MetadataObject object() {
    return object;
  }

  @Override
  public DomainRelation[] relations() {
    return relations.clone();
  }
}
