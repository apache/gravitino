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
