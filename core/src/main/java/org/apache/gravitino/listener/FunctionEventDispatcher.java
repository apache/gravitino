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
import org.apache.gravitino.listener.api.event.AlterFunctionEvent;
import org.apache.gravitino.listener.api.event.AlterFunctionFailureEvent;
import org.apache.gravitino.listener.api.event.AlterFunctionPreEvent;
import org.apache.gravitino.listener.api.event.DropFunctionEvent;
import org.apache.gravitino.listener.api.event.DropFunctionFailureEvent;
import org.apache.gravitino.listener.api.event.DropFunctionPreEvent;
import org.apache.gravitino.listener.api.event.GetFunctionEvent;
import org.apache.gravitino.listener.api.event.GetFunctionFailureEvent;
import org.apache.gravitino.listener.api.event.GetFunctionPreEvent;
import org.apache.gravitino.listener.api.event.ListFunctionEvent;
import org.apache.gravitino.listener.api.event.ListFunctionFailureEvent;
import org.apache.gravitino.listener.api.event.ListFunctionPreEvent;
import org.apache.gravitino.listener.api.event.RegisterFunctionEvent;
import org.apache.gravitino.listener.api.event.RegisterFunctionFailureEvent;
import org.apache.gravitino.listener.api.event.RegisterFunctionPreEvent;
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
    eventBus.dispatchEvent(
        new ListFunctionPreEvent(PrincipalUtils.getCurrentUserName(), namespace));
    try {
      NameIdentifier[] nameIdentifiers = dispatcher.listFunctions(namespace);
      eventBus.dispatchEvent(new ListFunctionEvent(PrincipalUtils.getCurrentUserName(), namespace));
      return nameIdentifiers;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new ListFunctionFailureEvent(PrincipalUtils.getCurrentUserName(), namespace, e));
      throw e;
    }
  }

  @Override
  public Function[] listFunctionInfos(Namespace namespace) throws NoSuchSchemaException {
    eventBus.dispatchEvent(
        new ListFunctionPreEvent(PrincipalUtils.getCurrentUserName(), namespace));
    try {
      Function[] functions = dispatcher.listFunctionInfos(namespace);
      eventBus.dispatchEvent(new ListFunctionEvent(PrincipalUtils.getCurrentUserName(), namespace));
      return functions;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new ListFunctionFailureEvent(PrincipalUtils.getCurrentUserName(), namespace, e));
      throw e;
    }
  }

  @Override
  public Function getFunction(NameIdentifier ident) throws NoSuchFunctionException {
    eventBus.dispatchEvent(new GetFunctionPreEvent(PrincipalUtils.getCurrentUserName(), ident));
    try {
      Function function = dispatcher.getFunction(ident);
      eventBus.dispatchEvent(
          new GetFunctionEvent(
              PrincipalUtils.getCurrentUserName(), ident, new FunctionInfo(function)));
      return function;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new GetFunctionFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e));
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
    FunctionInfo registerFunctionRequest =
        new FunctionInfo(ident.name(), functionType, deterministic, comment, definitions, null);
    eventBus.dispatchEvent(
        new RegisterFunctionPreEvent(
            PrincipalUtils.getCurrentUserName(), ident, registerFunctionRequest));
    try {
      Function function =
          dispatcher.registerFunction(ident, comment, functionType, deterministic, definitions);
      eventBus.dispatchEvent(
          new RegisterFunctionEvent(
              PrincipalUtils.getCurrentUserName(), ident, new FunctionInfo(function)));
      return function;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new RegisterFunctionFailureEvent(
              PrincipalUtils.getCurrentUserName(), ident, e, registerFunctionRequest));
      throw e;
    }
  }

  @Override
  public Function alterFunction(NameIdentifier ident, FunctionChange... changes)
      throws NoSuchFunctionException, IllegalArgumentException {
    eventBus.dispatchEvent(
        new AlterFunctionPreEvent(PrincipalUtils.getCurrentUserName(), ident, changes));
    try {
      Function function = dispatcher.alterFunction(ident, changes);
      eventBus.dispatchEvent(
          new AlterFunctionEvent(
              PrincipalUtils.getCurrentUserName(), ident, changes, new FunctionInfo(function)));
      return function;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new AlterFunctionFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e, changes));
      throw e;
    }
  }

  @Override
  public boolean dropFunction(NameIdentifier ident) {
    eventBus.dispatchEvent(new DropFunctionPreEvent(PrincipalUtils.getCurrentUserName(), ident));
    try {
      boolean isExists = dispatcher.dropFunction(ident);
      eventBus.dispatchEvent(
          new DropFunctionEvent(PrincipalUtils.getCurrentUserName(), ident, isExists));
      return isExists;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new DropFunctionFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e));
      throw e;
    }
  }

  @Override
  public boolean functionExists(NameIdentifier ident) {
    return dispatcher.functionExists(ident);
  }
}
