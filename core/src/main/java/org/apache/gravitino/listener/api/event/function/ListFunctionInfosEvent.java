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
import org.apache.gravitino.Namespace;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.listener.api.info.FunctionInfo;

/**
 * Represents an event that is triggered upon the successful listing of function infos within a
 * namespace.
 */
@DeveloperApi
public final class ListFunctionInfosEvent extends FunctionEvent {
  private final Namespace namespace;
  private final FunctionInfo[] functionInfos;

  /**
   * Constructs an instance of {@code ListFunctionInfosEvent}.
   *
   * @param user The username of the individual who initiated the function info listing.
   * @param namespace The namespace from which function infos were listed.
   * @param functionInfos The function infos that were listed.
   */
  public ListFunctionInfosEvent(String user, Namespace namespace, FunctionInfo[] functionInfos) {
    super(user, NameIdentifier.of(namespace.levels()));
    this.namespace = namespace;
    this.functionInfos = functionInfos;
  }

  /**
   * Provides the namespace associated with this event.
   *
   * @return A {@link Namespace} instance from which function infos were listed.
   */
  public Namespace namespace() {
    return namespace;
  }

  /**
   * Provides the function infos associated with this event.
   *
   * @return An array of {@link FunctionInfo} instances that were listed.
   */
  public FunctionInfo[] functionInfos() {
    return functionInfos;
  }

  @Override
  public OperationType operationType() {
    return OperationType.LIST_FUNCTION_INFOS;
  }
}
