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

package org.apache.gravitino.listener.api.event;

import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.listener.api.info.ModelInfo;

/**
 * Represents an event that is generated after a model is successfully registered and a model
 * version is linked.
 */
public class RegisterAndLinkModelEvent extends ModelEvent {
  private final ModelInfo registeredModelInfo;
  private final Map<String, String> uris;

  /**
   * Constructs a new instance of {@link RegisterAndLinkModelEvent}, capturing the user, identifier,
   * registered model information, and linked model version information.
   *
   * @param user The user responsible for triggering the model operation.
   * @param identifier The identifier of the Model involved in the operation. This encapsulates some
   *     information.
   * @param registeredModelInfo The final state of the model post-creation.
   * @param uris The URIs of the linked model version.
   */
  public RegisterAndLinkModelEvent(
      String user,
      NameIdentifier identifier,
      ModelInfo registeredModelInfo,
      Map<String, String> uris) {
    super(user, identifier);

    this.registeredModelInfo = registeredModelInfo;
    this.uris = uris;
  }

  /**
   * Retrieves the registered model information.
   *
   * @return The model information.
   */
  public ModelInfo registeredModelInfo() {
    return registeredModelInfo;
  }

  /**
   * Retrieves the URIs of the linked model version.
   *
   * @return the URIs of the linked model version
   */
  public Map<String, String> uris() {
    return uris;
  }

  /**
   * Returns the type of operation.
   *
   * @return The operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.REGISTER_AND_LINK_MODEL_VERSION;
  }
}
