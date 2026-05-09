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

/**
 * Represents an event that is generated when an attempt to register a function fails due to an
 * exception.
 */
@DeveloperApi
public final class RegisterFunctionFailureEvent extends FunctionFailureEvent {
  private final FunctionInfo registerFunctionRequest;

  /**
   * Constructs a {@code RegisterFunctionFailureEvent} instance.
   *
   * @param user The user who initiated the function registration operation.
   * @param identifier The identifier of the function that was attempted to be registered.
   * @param exception The exception that was thrown during the function registration operation.
   * @param registerFunctionRequest The original request information used to attempt to register the
   *     function.
   */
  public RegisterFunctionFailureEvent(
      String user,
      NameIdentifier identifier,
      Exception exception,
      FunctionInfo registerFunctionRequest) {
    super(user, identifier, exception);
    this.registerFunctionRequest = registerFunctionRequest;
  }

  /**
   * Retrieves the original request information for the attempted function registration.
   *
   * @return The {@link FunctionInfo} instance representing the request information for the failed
   *     function registration attempt.
   */
  public FunctionInfo registerFunctionRequest() {
    return registerFunctionRequest;
  }

  @Override
  public OperationType operationType() {
    return OperationType.REGISTER_FUNCTION;
  }
}
