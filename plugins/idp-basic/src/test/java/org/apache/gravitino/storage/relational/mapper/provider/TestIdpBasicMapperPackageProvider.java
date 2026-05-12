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
package org.apache.gravitino.storage.relational.mapper.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.ServiceLoader;
import org.apache.gravitino.storage.relational.mapper.IdpGroupMetaMapper;
import org.apache.gravitino.storage.relational.mapper.IdpGroupUserRelMapper;
import org.apache.gravitino.storage.relational.mapper.IdpUserMetaMapper;
import org.junit.jupiter.api.Test;

public class TestIdpBasicMapperPackageProvider {

  @Test
  public void testGetMapperClasses() {
    MapperPackageProvider provider = new IdpBasicMapperPackageProvider();

    assertEquals(
        List.of(IdpGroupMetaMapper.class, IdpGroupUserRelMapper.class, IdpUserMetaMapper.class),
        provider.getMapperClasses());
  }

  @Test
  public void testServiceLoaderDiscoversProvider() {
    List<MapperPackageProvider> providers =
        ServiceLoader.load(MapperPackageProvider.class).stream()
            .map(ServiceLoader.Provider::get)
            .filter(provider -> provider instanceof IdpBasicMapperPackageProvider)
            .toList();

    assertEquals(1, providers.size());
    assertTrue(providers.get(0) instanceof IdpBasicMapperPackageProvider);
  }
}
