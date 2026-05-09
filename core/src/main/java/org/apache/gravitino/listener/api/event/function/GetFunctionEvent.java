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

package org.apache.gravitino.listener.api.event.function;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.listener.api.info.FunctionInfo;

/** Represents an event triggered upon the successful retrieval of a function. */
@DeveloperApi
public final class GetFunctionEvent extends FunctionEvent {
  private final FunctionInfo functionInfo;

  /**
   * Constructs an instance of {@code GetFunctionEvent}.
   *
   * @param user The username of the individual who initiated the function retrieval.
   * @param identifier The unique identifier of the function that was retrieved.
   * @param functionInfo The state of the function post-retrieval.
   */
  public GetFunctionEvent(String user, NameIdentifier identifier, FunctionInfo functionInfo) {
    super(user, identifier);
    this.functionInfo = functionInfo;
  }

  /**
   * Retrieves the state of the function as it was made available to the user after successful
   * retrieval.
   *
   * @return A {@link FunctionInfo} instance encapsulating the details of the function as retrieved.
   */
  public FunctionInfo functionInfo() {
    return functionInfo;
  }

  @Override
  public OperationType operationType() {
    return OperationType.GET_FUNCTION;
  }
}
