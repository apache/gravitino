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
package org.apache.gravitino.listener.api.event.semantic;

import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.OperationType;

/** Event for a successful publication of a Semantic Model revision. */
@DeveloperApi
public final class PublishSemanticModelRevisionEvent extends SemanticModelEvent {
  private final int revision;

  /** Creates an event for the principal, model, and requested revision. */
  public PublishSemanticModelRevisionEvent(
      String principal, NameIdentifier identifier, int revision) {
    super(principal, identifier);
    this.revision = revision;
  }

  /** Returns the requested revision number. */
  public int revision() {
    return revision;
  }

  @Override
  public OperationType operationType() {
    return OperationType.PUBLISH_SEMANTIC_MODEL_REVISION;
  }

  @Override
  protected Map<String, String> ownCustomInfo() {
    return Map.of("revision", Integer.toString(revision));
  }
}
