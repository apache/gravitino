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

import java.util.List;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/**
 * A MyBatis Mapper for metalake meta operation SQLs.
 *
 * <p>This interface class is a specification defined by MyBatis. It requires this interface class
 * to identify the corresponding SQLs for execution. We can write SQLs in an additional XML file, or
 * write SQLs with annotations in this interface Mapper. See: <a
 * href="https://mybatis.org/mybatis-3/getting-started.html"></a>
 */
public interface MetalakeMetaMapper {
  String TABLE_NAME = "metalake_meta";

  @SelectProvider(type = MetalakeMetaSQLProviderFactory.class, method = "listMetalakePOs")
  List<MetalakePO> listMetalakePOs();

  @SelectProvider(type = MetalakeMetaSQLProviderFactory.class, method = "selectMetalakeMetaByName")
  MetalakePO selectMetalakeMetaByName(@Param("metalakeName") String name);

  @SelectProvider(type = MetalakeMetaSQLProviderFactory.class, method = "selectMetalakeMetaById")
  MetalakePO selectMetalakeMetaById(@Param("metalakeId") Long metalakeId);

  @SelectProvider(
      type = MetalakeMetaSQLProviderFactory.class,
      method = "selectMetalakeIdMetaByName")
  Long selectMetalakeIdMetaByName(@Param("metalakeName") String name);

  @InsertProvider(type = MetalakeMetaSQLProviderFactory.class, method = "insertMetalakeMeta")
  void insertMetalakeMeta(@Param("metalakeMeta") MetalakePO metalakePO);

  @InsertProvider(
      type = MetalakeMetaSQLProviderFactory.class,
      method = "insertMetalakeMetaOnDuplicateKeyUpdate")
  void insertMetalakeMetaOnDuplicateKeyUpdate(@Param("metalakeMeta") MetalakePO metalakePO);

  @UpdateProvider(type = MetalakeMetaSQLProviderFactory.class, method = "updateMetalakeMeta")
  Integer updateMetalakeMeta(
      @Param("newMetalakeMeta") MetalakePO newMetalakePO,
      @Param("oldMetalakeMeta") MetalakePO oldMetalakePO);

  @UpdateProvider(
      type = MetalakeMetaSQLProviderFactory.class,
      method = "softDeleteMetalakeMetaByMetalakeId")
  Integer softDeleteMetalakeMetaByMetalakeId(@Param("metalakeId") Long metalakeId);

  @DeleteProvider(
      type = MetalakeMetaSQLProviderFactory.class,
      method = "deleteMetalakeMetasByLegacyTimeline")
  Integer deleteMetalakeMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
