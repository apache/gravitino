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

package org.apache.gravitino.listener;

import java.util.Arrays;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.FunctionDispatcher;
import org.apache.gravitino.exceptions.FunctionAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchFunctionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionChange;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.listener.api.event.function.AlterFunctionEvent;
import org.apache.gravitino.listener.api.event.function.AlterFunctionFailureEvent;
import org.apache.gravitino.listener.api.event.function.AlterFunctionPreEvent;
import org.apache.gravitino.listener.api.event.function.DropFunctionEvent;
import org.apache.gravitino.listener.api.event.function.DropFunctionFailureEvent;
import org.apache.gravitino.listener.api.event.function.DropFunctionPreEvent;
import org.apache.gravitino.listener.api.event.function.GetFunctionEvent;
import org.apache.gravitino.listener.api.event.function.GetFunctionFailureEvent;
import org.apache.gravitino.listener.api.event.function.GetFunctionPreEvent;
import org.apache.gravitino.listener.api.event.function.ListFunctionEvent;
import org.apache.gravitino.listener.api.event.function.ListFunctionFailureEvent;
import org.apache.gravitino.listener.api.event.function.ListFunctionInfosEvent;
import org.apache.gravitino.listener.api.event.function.ListFunctionPreEvent;
import org.apache.gravitino.listener.api.event.function.RegisterFunctionEvent;
import org.apache.gravitino.listener.api.event.function.RegisterFunctionFailureEvent;
import org.apache.gravitino.listener.api.event.function.RegisterFunctionPreEvent;
import org.apache.gravitino.listener.api.info.FunctionInfo;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * {@code FunctionEventDispatcher} is a decorator for {@link FunctionDispatcher} that not only
 * delegates function operations to the underlying dispatcher but also dispatches corresponding
 * events to an {@link EventBus} after each operation is completed.
 */
public class FunctionEventDispatcher implements FunctionDispatcher {

  private final EventBus eventBus;
  private final FunctionDispatcher dispatcher;

  /**
   * Constructs a FunctionEventDispatcher with a specified EventBus and FunctionDispatcher.
   *
   * @param eventBus The EventBus to which events will be dispatched.
   * @param dispatcher The underlying {@link FunctionDispatcher} that will perform the actual
   *     function operations.
   */
  public FunctionEventDispatcher(EventBus eventBus, FunctionDispatcher dispatcher) {
    this.eventBus = eventBus;
    this.dispatcher = dispatcher;
  }

  @Override
  public NameIdentifier[] listFunctions(Namespace namespace) throws NoSuchSchemaException {
    String user = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new ListFunctionPreEvent(user, namespace));
    try {
      NameIdentifier[] nameIdentifiers = dispatcher.listFunctions(namespace);
      eventBus.dispatchEvent(new ListFunctionEvent(user, namespace));
      return nameIdentifiers;
    } catch (Exception e) {
      eventBus.dispatchEvent(new ListFunctionFailureEvent(user, namespace, e));
      throw e;
    }
  }

  @Override
  public Function[] listFunctionInfos(Namespace namespace) throws NoSuchSchemaException {
    String user = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new ListFunctionPreEvent(user, namespace));
    try {
      Function[] functions = dispatcher.listFunctionInfos(namespace);
      FunctionInfo[] functionInfos =
          Arrays.stream(functions).map(FunctionInfo::new).toArray(FunctionInfo[]::new);
      eventBus.dispatchEvent(new ListFunctionInfosEvent(user, namespace, functionInfos));
      return functions;
    } catch (Exception e) {
      eventBus.dispatchEvent(new ListFunctionFailureEvent(user, namespace, e));
      throw e;
    }
  }

  @Override
  public Function getFunction(NameIdentifier ident) throws NoSuchFunctionException {
    String user = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new GetFunctionPreEvent(user, ident));
    try {
      Function function = dispatcher.getFunction(ident);
      eventBus.dispatchEvent(new GetFunctionEvent(user, ident, new FunctionInfo(function)));
      return function;
    } catch (Exception e) {
      eventBus.dispatchEvent(new GetFunctionFailureEvent(user, ident, e));
      throw e;
    }
  }

  @Override
  public Function registerFunction(
      NameIdentifier ident,
      String comment,
      FunctionType functionType,
      boolean deterministic,
      FunctionDefinition[] definitions)
      throws NoSuchSchemaException, FunctionAlreadyExistsException {
    String user = PrincipalUtils.getCurrentUserName();
    FunctionInfo registerFunctionRequest =
        new FunctionInfo(ident.name(), functionType, deterministic, comment, definitions, null);
    eventBus.dispatchEvent(new RegisterFunctionPreEvent(user, ident, registerFunctionRequest));
    try {
      Function function =
          dispatcher.registerFunction(ident, comment, functionType, deterministic, definitions);
      eventBus.dispatchEvent(new RegisterFunctionEvent(user, ident, new FunctionInfo(function)));
      return function;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new RegisterFunctionFailureEvent(user, ident, e, registerFunctionRequest));
      throw e;
    }
  }

  @Override
  public Function alterFunction(NameIdentifier ident, FunctionChange... changes)
      throws NoSuchFunctionException, IllegalArgumentException {
    String user = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new AlterFunctionPreEvent(user, ident, changes));
    try {
      Function function = dispatcher.alterFunction(ident, changes);
      eventBus.dispatchEvent(
          new AlterFunctionEvent(user, ident, changes, new FunctionInfo(function)));
      return function;
    } catch (Exception e) {
      eventBus.dispatchEvent(new AlterFunctionFailureEvent(user, ident, e, changes));
      throw e;
    }
  }

  @Override
  public boolean dropFunction(NameIdentifier ident) {
    String user = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new DropFunctionPreEvent(user, ident));
    try {
      boolean isExists = dispatcher.dropFunction(ident);
      eventBus.dispatchEvent(new DropFunctionEvent(user, ident, isExists));
      return isExists;
    } catch (Exception e) {
      eventBus.dispatchEvent(new DropFunctionFailureEvent(user, ident, e));
      throw e;
    }
  }

  @Override
  public boolean functionExists(NameIdentifier ident) {
    return dispatcher.functionExists(ident);
  }
}
