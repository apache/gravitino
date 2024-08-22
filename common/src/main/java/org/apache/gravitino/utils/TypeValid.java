package org.apache.gravitino.utils;

public class TypeValid {

  public static final String JDBC_REGEX = "^jdbc:([a-z_A-Z-]+):[^\\s]+$";

  public static boolean isJdbcURL(String url) {
    if (url == null || url.isEmpty()) {
      return false;
    }
    return url.matches(JDBC_REGEX);
  }
}
