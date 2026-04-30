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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.FunctionDispatcher;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionType;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TestFunctionHookDispatcher {

  @Test
  public void testRegisterFunctionSetOwnerAfterRegister() throws Exception {
    GravitinoEnv gravitinoEnv = GravitinoEnv.getInstance();
    Object originalOwnerDispatcher = FieldUtils.readField(gravitinoEnv, "ownerDispatcher", true);

    NameIdentifier functionIdentifier =
        NameIdentifier.of("metalake1", "catalog1", "schema1", "func1");
    FunctionDefinition[] definitions = new FunctionDefinition[] {};
    FunctionDispatcher dispatcher = Mockito.mock(FunctionDispatcher.class);
    Function registeredFunction = Mockito.mock(Function.class);
    OwnerDispatcher ownerDispatcher = Mockito.mock(OwnerDispatcher.class);

    Mockito.when(
            dispatcher.registerFunction(
                Mockito.eq(functionIdentifier),
                Mockito.eq("comment"),
                Mockito.eq(FunctionType.SCALAR),
                Mockito.eq(true),
                Mockito.eq(definitions)))
        .thenReturn(registeredFunction);

    FieldUtils.writeField(gravitinoEnv, "ownerDispatcher", ownerDispatcher, true);
    try {
      FunctionHookDispatcher hookDispatcher = new FunctionHookDispatcher(dispatcher);
      Function result =
          hookDispatcher.registerFunction(
              functionIdentifier, "comment", FunctionType.SCALAR, true, definitions);

      assertSame(registeredFunction, result);

      ArgumentCaptor<MetadataObject> metadataObjectCaptor =
          ArgumentCaptor.forClass(MetadataObject.class);
      Mockito.verify(ownerDispatcher)
          .setOwner(
              Mockito.eq("metalake1"),
              metadataObjectCaptor.capture(),
              Mockito.eq(AuthConstants.ANONYMOUS_USER),
              Mockito.eq(Owner.Type.USER));
      assertEquals(MetadataObject.Type.FUNCTION, metadataObjectCaptor.getValue().type());
      assertEquals("catalog1.schema1.func1", metadataObjectCaptor.getValue().fullName());
    } finally {
      FieldUtils.writeField(gravitinoEnv, "ownerDispatcher", originalOwnerDispatcher, true);
    }
  }

  @Test
  public void testRegisterFunctionSucceedsWhenOwnerDispatcherIsDisabled() throws Exception {
    GravitinoEnv gravitinoEnv = GravitinoEnv.getInstance();
    Object originalOwnerDispatcher = FieldUtils.readField(gravitinoEnv, "ownerDispatcher", true);

    NameIdentifier functionIdentifier =
        NameIdentifier.of("metalake1", "catalog1", "schema1", "func1");
    FunctionDefinition[] definitions = new FunctionDefinition[] {};
    FunctionDispatcher dispatcher = Mockito.mock(FunctionDispatcher.class);
    Function registeredFunction = Mockito.mock(Function.class);

    Mockito.when(
            dispatcher.registerFunction(
                Mockito.eq(functionIdentifier),
                Mockito.eq("comment"),
                Mockito.eq(FunctionType.SCALAR),
                Mockito.eq(true),
                Mockito.eq(definitions)))
        .thenReturn(registeredFunction);

    FieldUtils.writeField(gravitinoEnv, "ownerDispatcher", null, true);
    try {
      FunctionHookDispatcher hookDispatcher = new FunctionHookDispatcher(dispatcher);
      Function result =
          hookDispatcher.registerFunction(
              functionIdentifier, "comment", FunctionType.SCALAR, true, definitions);

      assertSame(registeredFunction, result);
      Mockito.verify(dispatcher)
          .registerFunction(functionIdentifier, "comment", FunctionType.SCALAR, true, definitions);
    } finally {
      FieldUtils.writeField(gravitinoEnv, "ownerDispatcher", originalOwnerDispatcher, true);
    }
  }
}
