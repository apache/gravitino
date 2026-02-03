package org.apache.gravitino.catalog.clickhouse.converter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TypeUtils {

  private TypeUtils() {}

  public static String stripNullable(String typeName) {
    return typeName.replaceFirst("^Nullable\\((.*)\\)$", "$1");
  }

  public static Integer extractDateTimePrecision(String typeName) {
    Matcher matcher = Pattern.compile("^DateTime\\((\\d+)\\)$").matcher(typeName);
    if (matcher.matches()) {
      return Integer.parseInt(matcher.group(1));
    }
    return null;
  }
}
