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
package org.apache.gravitino.exceptions;

import static org.apache.gravitino.exceptions.NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestExceptions {

  @Test
  public void testNoSuchEntityExceptionIsInGravitinoHierarchy() {
    // Before the fix, NoSuchEntityException extended RuntimeException directly, outside the
    // GravitinoRuntimeException hierarchy every sibling NoSuch* exception uses, so
    // catch (GravitinoRuntimeException) blocks silently missed it.
    Object noCause = new NoSuchEntityException(NO_SUCH_ENTITY_MESSAGE, "table", "a.b.c");
    Object withCause =
        new NoSuchEntityException(
            new IllegalStateException("cause"), NO_SUCH_ENTITY_MESSAGE, "table", "a.b.c");
    Assertions.assertTrue(noCause instanceof GravitinoRuntimeException);
    Assertions.assertTrue(withCause instanceof GravitinoRuntimeException);
  }
}
