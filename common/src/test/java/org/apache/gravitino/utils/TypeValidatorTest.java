package org.apache.gravitino.utils;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class TypeValidatorTest {

  @Test
  void testJDBCURI() {
    assertTrue(TypeValidator.isJdbcURL("jdbc:postgresql://postgresql/db"));
    assertTrue(
        TypeValidator.isJdbcURL(
            "jdbc:redshift://examplecluster.abc123xyz789.us-west-2.redshift.amazonaws.com:5439/dev"));
    assertTrue(TypeValidator.isJdbcURL("jdbc:mysql://mysql:3306/db"));
    assertTrue(TypeValidator.isJdbcURL("jdbc:sqlite:/path/to/db"));
    assertTrue(TypeValidator.isJdbcURL("jdbc:hsqldb:mem:myDb"));
  }

  @Test
  void testInvalidURI() {
    assertFalse(TypeValidator.isJdbcURL("invalid"));
    assertFalse(TypeValidator.isJdbcURL(""));
    assertFalse(TypeValidator.isJdbcURL(null));
  }
}
