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
package org.apache.gravitino.catalog;

import static org.apache.gravitino.file.Fileset.LOCATION_NAME_UNKNOWN;

import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.FilesetEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestEntityCombinedFileset {

  /**
   * This test has no real assertions, but it's just to use the getter methods, by calling them
   * others will not delete the getters by mistake.
   */
  @Test
  void testGetters() {
    Fileset fileset = Mockito.mock(Fileset.class);
    FilesetEntity filesetEntity =
        FilesetEntity.builder()
            .withId(1L)
            .withName("fileset")
            .withAuditInfo(AuditInfo.EMPTY)
            .withFilesetType(Fileset.Type.MANAGED)
            .withStorageLocations(ImmutableMap.of(LOCATION_NAME_UNKNOWN, "testLocation"))
            .withProperties(ImmutableMap.of())
            .build();

    EntityCombinedFileset entityCombinedFileset = EntityCombinedFileset.of(fileset, filesetEntity);
    Assertions.assertEquals(fileset, entityCombinedFileset.fileset());
    Assertions.assertEquals(filesetEntity, entityCombinedFileset.filesetEntity());
  }
}
