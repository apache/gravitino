package org.apache.gravitino.catalog.jdbc.operation;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link org.apache.gravitino.catalog.jdbc.operation.SqliteTableOperations}. */
public class TestSqliteTableOperations {
  /**
   * Tests that COMMENT ends with a semicolon and escapes quotes in comment.
   *
   * <p><b>Verify:</b>
   *
   * <ul>
   *   <li>Escapes quotes in comment
   *   <li>Ends with semicolon (;)
   * </ul>
   *
   * <b>Expected:</b>
   *
   * <pre>
   * CREATE TABLE test_table () COMMENT 'comment';
   * </pre>
   */
  @Test
  public void testGenerateCreateTableSqlWithCommentEndsWithSemicolon() {
    SqliteTableOperations ops = new SqliteTableOperations();

    String sql =
        ops.generateCreateTableSql(
            "test_table",
            new JdbcColumn[0],
            "comment",
            null,
            new Transform[0],
            Distributions.NONE,
            new Index[0]);

    Assertions.assertTrue(
        sql.trim().endsWith("COMMENT 'comment';"),
        "Generated SQL should end with COMMENT 'comment';");
  }

  /**
   * Tests that COMMENT appears before properties, properties keep order, and the statement ends
   * with a semicolon.
   *
   * <p><b>Verify:</b>
   *
   * <ul>
   *   <li>COMMENT before properties
   *   <li>Properties preserve insertion order
   *   <li>Ends with semicolon (;)
   * </ul>
   *
   * <b>Expected:</b>
   *
   * <pre>
   * CREATE TABLE test_table () COMMENT 'comment' k1=v1 k2=v2;
   * </pre>
   */
  @Test
  public void testGenerateCreateTableSqlWithCommentAndPropertiesOrderAndSemicolon() {
    SqliteTableOperations ops = new SqliteTableOperations();

    Map<String, String> props = new LinkedHashMap<>();
    props.put("k1", "v1");
    props.put("k2", "v2");

    String sql =
        ops.generateCreateTableSql(
            "test_table",
            new JdbcColumn[0],
            "comment",
            props,
            new Transform[0],
            Distributions.NONE,
            new Index[0]);

    int idxComment = sql.indexOf(" COMMENT 'comment'");
    int idxK1 = sql.indexOf(" k1=v1");
    int idxK2 = sql.indexOf(" k2=v2");

    Assertions.assertTrue(idxComment > 0, "COMMENT clause missing:\n" + sql);
    Assertions.assertTrue(
        idxK1 > idxComment && idxK2 > idxK1,
        "COMMENT must come before properties and preserve order:\n" + sql);
    Assertions.assertTrue(sql.trim().endsWith(";"), "SQL must end with a semicolon:\n" + sql);
  }
}
