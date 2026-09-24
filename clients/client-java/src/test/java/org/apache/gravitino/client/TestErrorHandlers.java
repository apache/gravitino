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
package org.apache.gravitino.client;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests REST error handling for optimistic-lock conflicts. */
public class TestErrorHandlers {

  @Test
  @SuppressWarnings("unchecked")
  public void testOptimisticLockConflictAcrossHandlers() throws ReflectiveOperationException {
    ErrorResponse response =
        ErrorResponse.optimisticLockConflict(
            OptimisticLockException.class.getSimpleName(), "Concurrent update", null);
    List<Method> factories =
        Arrays.stream(ErrorHandlers.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> Modifier.isStatic(method.getModifiers()))
            .filter(method -> method.getName().endsWith("ErrorHandler"))
            .filter(method -> method.getParameterCount() == 0)
            .filter(method -> Consumer.class.isAssignableFrom(method.getReturnType()))
            .collect(Collectors.toList());
    Assertions.assertFalse(factories.isEmpty());

    for (Method factory : factories) {
      Consumer<ErrorResponse> handler = (Consumer<ErrorResponse>) factory.invoke(null);
      OptimisticLockException exception =
          Assertions.assertThrows(
              OptimisticLockException.class, () -> handler.accept(response), factory.getName());
      Assertions.assertEquals("Concurrent update", exception.getMessage(), factory.getName());
    }
  }
}
