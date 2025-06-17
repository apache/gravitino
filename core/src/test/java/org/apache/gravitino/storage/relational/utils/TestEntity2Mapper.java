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

import java.util.List;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

@Mapper
public interface TestEntity2Mapper {

  @Insert("INSERT INTO test_entity2(id, name) VALUES(#{id}, #{name})")
  int insert(TestEntity2 entity);

  @Update("UPDATE test_entity2 SET name = #{name} WHERE id = #{id}")
  int update(TestEntity2 entity);

  @Delete("DELETE FROM test_entity2 WHERE id = #{id}")
  int deleteById(@Param("id") Long id);

  @Select("SELECT * FROM test_entity2 WHERE id = #{id}")
  TestEntity2 selectById(@Param("id") Long id);

  @Select("SELECT * FROM test_entity2")
  List<TestEntity2> selectAll();

  @Select("SELECT COUNT(*) FROM test_entity2")
  int count();
}
