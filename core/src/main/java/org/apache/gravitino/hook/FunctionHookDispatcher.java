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

package org.apache.gravitino.hook;

import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.FunctionDispatcher;
import org.apache.gravitino.exceptions.FunctionAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchFunctionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionChange;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * {@code FunctionHookDispatcher} is a decorator for {@link FunctionDispatcher} that not only
 * delegates function operations to the underlying function dispatcher but also executes some hook
 * operations before or after the underlying operations.
 */
public class FunctionHookDispatcher implements FunctionDispatcher {

  private final FunctionDispatcher dispatcher;

  public FunctionHookDispatcher(FunctionDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public NameIdentifier[] listFunctions(Namespace namespace) throws NoSuchSchemaException {
    return dispatcher.listFunctions(namespace);
  }

  @Override
  public Function[] listFunctionInfos(Namespace namespace) throws NoSuchSchemaException {
    return dispatcher.listFunctionInfos(namespace);
  }

  @Override
  public Function getFunction(NameIdentifier ident) throws NoSuchFunctionException {
    return dispatcher.getFunction(ident);
  }

  @Override
  public boolean functionExists(NameIdentifier ident) {
    return dispatcher.functionExists(ident);
  }

  @Override
  public Function registerFunction(
      NameIdentifier ident,
      String comment,
      FunctionType functionType,
      boolean deterministic,
      FunctionDefinition[] definitions)
      throws NoSuchSchemaException, FunctionAlreadyExistsException {
    Function function =
        dispatcher.registerFunction(ident, comment, functionType, deterministic, definitions);

    // Set the creator as owner of the function.
    OwnerDispatcher ownerManager = GravitinoEnv.getInstance().ownerDispatcher();
    if (ownerManager != null) {
      ownerManager.setOwner(
          ident.namespace().level(0),
          NameIdentifierUtil.toMetadataObject(ident, Entity.EntityType.FUNCTION),
          PrincipalUtils.getCurrentUserName(),
          Owner.Type.USER);
    }
    return function;
  }

  @Override
  public Function alterFunction(NameIdentifier ident, FunctionChange... changes)
      throws NoSuchFunctionException, IllegalArgumentException {
    return dispatcher.alterFunction(ident, changes);
  }

  @Override
  public boolean dropFunction(NameIdentifier ident) {
    return dispatcher.dropFunction(ident);
  }
}
