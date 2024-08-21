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

package org.apache.gravitino.storage.relational.mapper;

import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.ibatis.annotations.Param;

/** SQL Provider for Metalake Meta operations. */
public class MetalakeMetaSQLProvider {

  private final MetalakeMetaBaseProvider provider;

  public MetalakeMetaSQLProvider() {
    provider = MetalakeMetaBaseProvider.getProvider();
  }

  static class MetalakeMetaMySQLProvider extends MetalakeMetaBaseProvider {}

  static class MetalakeMetaH2Provider extends MetalakeMetaBaseProvider {}

  static class MetalakeMetaPGProvider extends MetalakeMetaBaseProvider {}

  public String listMetalakePOs() {
    return provider.listMetalakePOs();
  }

  public String selectMetalakeMetaByName(@Param("metalakeName") String metalakeName) {
    return provider.selectMetalakeMetaByName(metalakeName);
  }

  public String selectMetalakeMetaById(@Param("metalakeId") Long metalakeId) {
    return provider.selectMetalakeMetaById(metalakeId);
  }

  public String selectMetalakeIdMetaByName(@Param("metalakeName") String metalakeName) {
    return provider.selectMetalakeIdMetaByName(metalakeName);
  }

  public String insertMetalakeMeta(@Param("metalakeMeta") MetalakePO metalakePO) {
    return provider.insertMetalakeMeta(metalakePO);
  }

  public String insertMetalakeMetaOnDuplicateKeyUpdate(
      @Param("metalakeMeta") MetalakePO metalakePO) {
    return provider.insertMetalakeMetaOnDuplicateKeyUpdate(metalakePO);
  }

  public String updateMetalakeMeta(
      @Param("newMetalakeMeta") MetalakePO newMetalakePO,
      @Param("oldMetalakeMeta") MetalakePO oldMetalakePO) {
    return provider.updateMetalakeMeta(newMetalakePO, oldMetalakePO);
  }

  public String softDeleteMetalakeMetaByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return provider.softDeleteMetalakeMetaByMetalakeId(metalakeId);
  }

  public String deleteMetalakeMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return provider.deleteMetalakeMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
