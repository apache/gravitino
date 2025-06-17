package org.apache.gravitino.storage.relational.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.session.SqlSessions;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@Slf4j
public class TestSessionUtils extends TestJDBCBackend {

  @BeforeAll
  public void initCurrent() {
    SqlSessionFactory sqlSessionFactory =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory();
    Configuration configuration = sqlSessionFactory.getConfiguration();
    DataSource dataSource = configuration.getEnvironment().getDataSource();

    try (Connection conn = dataSource.getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.execute(
          "CREATE TABLE IF NOT EXISTS test_entity1 (id BIGINT PRIMARY KEY, name VARCHAR(255));CREATE TABLE IF NOT EXISTS test_entity2 (id BIGINT PRIMARY KEY, name VARCHAR(255));");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    configuration.addMapper(TestEntity1Mapper.class);
    configuration.addMapper(TestEntity2Mapper.class);
    log.info("init table test_entity1,test_entity2");
  }

  @Test
  public void doWithoutCommit() {
    long id = 8934353L;
    SessionUtils.doWithoutCommit(
        TestEntity2Mapper.class,
        testEntity2Mapper -> {
          TestEntity2 testEntity2 = new TestEntity2();
          testEntity2.setId(id);
          testEntity2.setName("dsfadsf");
          testEntity2Mapper.insert(testEntity2);
        });
    SqlSessions.closeSqlSession();

    new Thread(
            () -> {
              TestEntity2 withoutCommit =
                  SessionUtils.getWithoutCommit(
                      TestEntity2Mapper.class,
                      testEntity2Mapper -> testEntity2Mapper.selectById(id));

              assertNull(withoutCommit);
            })
        .start();
  }

  @Test
  public void doMultipleWithCommit() {

    SessionUtils.doMultipleWithCommit(
        SessionUtils.opWithoutCommit(
            TestEntity1Mapper.class,
            mapper -> {
              TestEntity1 testEntity1 = new TestEntity1();
              testEntity1.setId(1L);
              testEntity1.setName("test1");

              mapper.insert(testEntity1);
            }),
        SessionUtils.opWithoutCommit(
            TestEntity2Mapper.class,
            mapper -> {
              TestEntity2 testEntity2 = new TestEntity2();
              testEntity2.setId(2L);
              testEntity2.setName("test1");

              mapper.insert(testEntity2);
            }),
        SessionUtils.opWithoutCommit(
            TestEntity1Mapper.class,
            testEntity1Mapper -> {
              int count1 = testEntity1Mapper.count();
              assertEquals(1, count1);
            }),
        SessionUtils.callWithoutCommit(
            TestEntity2Mapper.class,
            testEntity2Mapper -> {
              int count1 = testEntity2Mapper.count();
              assertEquals(1, count1);
              return count1;
            }));
  }

  @Test
  public void doMultiplyWithCommitTest() {
    SessionUtils.callWithoutCommit(
        TestEntity2Mapper.class,
        mapper -> {
          System.out.println("single callWithoutCommit running ");
          return mapper.count();
        });
    SessionUtils.doMultipleWithCommit(
        SessionUtils.callWithoutCommit(
            TestEntity1Mapper.class,
            mapper -> {
              System.out.println("callWithoutCommit wrapper in doMultipleWithCommit running");
              return mapper.count();
            }));
  }
}
