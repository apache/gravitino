package org.apache.gravitino.utils;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class TypeValidTest {

  @Test
  void testJDBCURI() {
    assertTrue(TypeValid.isJdbcURL("jdbc:postgresql://postgresql/db"));
    assertTrue(
        TypeValid.isJdbcURL(
            "jdbc:redshift://examplecluster.abc123xyz789.us-west-2.redshift.amazonaws.com:5439/dev"));
    assertTrue(TypeValid.isJdbcURL("jdbc:mysql://mysql:3306/db"));
    assertTrue(TypeValid.isJdbcURL("jdbc:sqlite:/path/to/db"));
    assertTrue(TypeValid.isJdbcURL("jdbc:hsqldb:mem:myDb"));
  }

  @Test
  void testInvalidURI() {
    assertFalse(TypeValid.isJdbcURL("invalid"));
    assertFalse(TypeValid.isJdbcURL(""));
    assertFalse(TypeValid.isJdbcURL(null));
  }
}
