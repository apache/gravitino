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
package org.apache.gravitino.meta;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.ArrayUtils;

public class NamespacedEntityId {

  private final long entityId;
  private final long[] namespaceIds;

  public NamespacedEntityId(long entityId, long... namespaceIds) {
    Preconditions.checkArgument(namespaceIds != null, "namespaceIds cannot be null");
    this.entityId = entityId;
    this.namespaceIds = namespaceIds;
  }

  public NamespacedEntityId(long entityId) {
    this(entityId, new long[0]);
  }

  public long entityId() {
    return entityId;
  }

  public long[] namespaceIds() {
    return namespaceIds;
  }

  public long[] fullIds() {
    return ArrayUtils.add(namespaceIds, entityId);
  }
}
