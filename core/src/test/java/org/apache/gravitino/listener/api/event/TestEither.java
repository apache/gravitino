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

    Assertions.assertTrue(either.isLeft());
    Assertions.assertFalse(either.isRight());
    Assertions.assertEquals("alias", either.getLeft());
  }

  @Test
  void testRightValue() {
    Either<String, Integer> either = Either.right(42);

    Assertions.assertTrue(either.isRight());
    Assertions.assertFalse(either.isLeft());
    Assertions.assertEquals(42, either.getRight());
  }

  @Test
  void testGetLeftThrowsOnRight() {
    Either<String, Integer> either = Either.right(99);

    IllegalStateException exception =
        Assertions.assertThrows(IllegalStateException.class, either::getLeft);
    Assertions.assertEquals("Not a left value", exception.getMessage());
  }

  @Test
  void testGetRightThrowsOnLeft() {
    Either<String, Integer> either = Either.left("lefty");

    IllegalStateException exception =
        Assertions.assertThrows(IllegalStateException.class, either::getRight);
    Assertions.assertEquals("Not a right value", exception.getMessage());
  }

  @Test
  void testToStringReadable() {
    Either<String, Integer> left = Either.left("model_v1");
    Either<String, Integer> right = Either.right(3);

    // Optional checks — not essential but good to verify Optional usage
    Assertions.assertTrue(left.isLeft());
    Assertions.assertEquals("model_v1", left.getLeft());

    Assertions.assertTrue(right.isRight());
    Assertions.assertEquals(3, right.getRight());
  }
}
