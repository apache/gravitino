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
package org.apache.gravitino.storage.relational.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.junit.jupiter.api.Test;

public class TestExceptionUtils {

  @Test
  public void testConcurrentModificationMessage() {
    OptimisticLockException e =
        ExceptionUtils.concurrentModification(
            Entity.EntityType.CATALOG, NameIdentifier.of("m1", "c1"));

    assertEquals(
        "The catalog m1.c1 was modified concurrently; retry the operation", e.getMessage());
  }

  @Test
  public void testConcurrentChildModificationMessage() {
    OptimisticLockException e =
        ExceptionUtils.concurrentChildModification(
            Entity.EntityType.SCHEMA, Entity.EntityType.CATALOG, NameIdentifier.of("m1", "c1"));

    assertEquals(
        "A schema under catalog m1.c1 was modified concurrently; retry the operation",
        e.getMessage());
  }
}
