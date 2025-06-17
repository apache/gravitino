package org.apache.gravitino.storage.relational.utils;

import java.util.List;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

@Mapper
public interface TestEntity1Mapper {

  @Insert("INSERT INTO test_entity1(id, name) VALUES(#{id}, #{name})")
  int insert(TestEntity1 entity);

  @Update("UPDATE test_entity1 SET name = #{name} WHERE id = #{id}")
  int update(TestEntity1 entity);

  @Delete("DELETE FROM test_entity1 WHERE id = #{id}")
  int deleteById(@Param("id") Long id);

  @Select("SELECT * FROM test_entity1 WHERE id = #{id}")
  TestEntity1 selectById(@Param("id") Long id);

  @Select("SELECT * FROM test_entity1")
  List<TestEntity1> selectAll();

  @Select("SELECT COUNT(*) FROM test_entity1")
  int count();
}
