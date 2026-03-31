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

/** Represents an event triggered before registering a function. */
@DeveloperApi
public class RegisterFunctionPreEvent extends FunctionPreEvent {
  private final FunctionInfo registerFunctionRequest;

  public RegisterFunctionPreEvent(
      String user, NameIdentifier identifier, FunctionInfo registerFunctionRequest) {
    super(user, identifier);
    this.registerFunctionRequest = registerFunctionRequest;
  }

  /**
   * Retrieves the register function request.
   *
   * @return A {@link FunctionInfo} instance encapsulating the details of the register function
   *     request.
   */
  public FunctionInfo registerFunctionRequest() {
    return registerFunctionRequest;
  }

  @Override
  public OperationType operationType() {
    return OperationType.REGISTER_FUNCTION;
  }
}
