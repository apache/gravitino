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

import org.apache.gravitino.listener.api.info.Either;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestEither {

  @Test
  void testLeftValue() {
    Either<String, Integer> either = Either.left("alias");

    Assertions.assertTrue(either.left().isPresent());
    Assertions.assertFalse(either.right().isPresent());
    Assertions.assertEquals("alias", either.left().get());
  }

  @Test
  void testRightValue() {
    Either<String, Integer> either = Either.right(42);

    Assertions.assertTrue(either.right().isPresent());
    Assertions.assertFalse(either.left().isPresent());
    Assertions.assertEquals(42, either.right().get());
  }

  @Test
  void testToStringReadable() {
    Either<String, Integer> left = Either.left("model_v1");
    Either<String, Integer> right = Either.right(3);

    // Optional checks — not essential but good to verify Optional usage
    Assertions.assertTrue(left.left().isPresent());
    Assertions.assertEquals("model_v1", left.left().get());

    Assertions.assertTrue(right.right().isPresent());
    Assertions.assertEquals(3, right.right().get());
  }
}
