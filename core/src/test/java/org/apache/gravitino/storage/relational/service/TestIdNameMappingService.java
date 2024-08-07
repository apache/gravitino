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
package org.apache.gravitino.storage.relational.service;

import java.io.IOException;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.storage.relational.service.NameIdMappingService.EntityIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIdNameMappingService {

  @Test
  public void testGetInstance() throws IOException {
    NameIdMappingService instance = NameIdMappingService.getInstance();

    EntityIdentifier makeLakeIdent1 =
        EntityIdentifier.of(NameIdentifier.of("m1"), EntityType.METALAKE);
    EntityIdentifier makeLakeIdent2 =
        EntityIdentifier.of(NameIdentifier.of("m2"), EntityType.METALAKE);

    instance.put(makeLakeIdent1, 1L);
    Assertions.assertEquals(1L, instance.get(makeLakeIdent1, (EntityIdentifier key) -> 1L));

    instance.put(makeLakeIdent2, 2L);
    Assertions.assertEquals(2L, instance.get(makeLakeIdent2));

    instance.invalidate(makeLakeIdent2);
    Assertions.assertNull(instance.get(makeLakeIdent2));

    Assertions.assertEquals(makeLakeIdent1, instance.getById(1L));

    Assertions.assertEquals(makeLakeIdent2, instance.getById(2L, (Long value) -> makeLakeIdent2));

    instance.close();
  }
}
