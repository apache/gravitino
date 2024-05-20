/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.util;

import static com.datastrato.gravitino.dto.util.DTOConverters.toDTO;

import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.dto.rel.expressions.LiteralDTO;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.indexes.Index;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.Assertions;

public class ITUtils {
  public static final String TEST_MODE = "testMode";
  public static final String EMBEDDED_TEST_MODE = "embedded";

  public static String joinPath(String... dirs) {
    return String.join(File.separator, dirs);
  }

  public static String[] splitPath(String path) {
    return path.split(File.separator);
  }

  public static void rewriteConfigFile(
      String configTempFileName, String configFileName, Map<String, String> configMap)
      throws IOException {
    Properties props = new Properties();
    try (InputStream inputStream = Files.newInputStream(Paths.get(configTempFileName));
        OutputStream outputStream = Files.newOutputStream(Paths.get(configFileName))) {
      props.load(inputStream);
      props.putAll(configMap);
      for (String key : props.stringPropertyNames()) {
        String value = props.getProperty(key);
        // Use customized write functions to avoid escaping `:` into `\:`.
        outputStream.write((key + " = " + value + "\n").getBytes(StandardCharsets.UTF_8));
      }
    }
  }

  public static void overwriteConfigFile(String configFileName, Properties props)
      throws IOException {
    try (OutputStream outputStream = Files.newOutputStream(Paths.get(configFileName))) {
      for (String key : props.stringPropertyNames()) {
        String value = props.getProperty(key);
        // Use customized write functions to avoid escaping `:` into `\:`.
        outputStream.write((key + " = " + value + "\n").getBytes(StandardCharsets.UTF_8));
      }
    }
  }

  public static void assertionsTableInfo(
      String tableName,
      String tableComment,
      List<Column> columns,
      Map<String, String> properties,
      Index[] indexes,
      Table table) {
    Assertions.assertEquals(tableName, table.name());
    Assertions.assertEquals(tableComment, table.comment());
    Assertions.assertEquals(columns.size(), table.columns().length);
    for (int i = 0; i < columns.size(); i++) {
      assertColumn(columns.get(i), table.columns()[i]);
    }
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      Assertions.assertEquals(entry.getValue(), table.properties().get(entry.getKey()));
    }
    if (ArrayUtils.isNotEmpty(indexes)) {
      Assertions.assertEquals(indexes.length, table.index().length);

      Map<String, Index> indexByName =
          Arrays.stream(indexes).collect(Collectors.toMap(Index::name, index -> index));

      for (int i = 0; i < table.index().length; i++) {
        Assertions.assertTrue(indexByName.containsKey(table.index()[i].name()));
        Assertions.assertEquals(
            indexByName.get(table.index()[i].name()).type(), table.index()[i].type());
        for (int j = 0; j < table.index()[i].fieldNames().length; j++) {
          for (int k = 0; k < table.index()[i].fieldNames()[j].length; k++) {
            Assertions.assertEquals(
                indexByName.get(table.index()[i].name()).fieldNames()[j][k],
                table.index()[i].fieldNames()[j][k]);
          }
        }
      }
    }
  }

  public static void assertColumn(Column expected, Column actual) {
    if (!(actual instanceof ColumnDTO)) {
      actual = toDTO(actual);
    }
    if (!(expected instanceof ColumnDTO)) {
      expected = toDTO(expected);
    }

    Assertions.assertEquals(expected.name(), actual.name());
    Assertions.assertEquals(expected.dataType(), actual.dataType());
    Assertions.assertEquals(expected.nullable(), actual.nullable());
    Assertions.assertEquals(expected.comment(), actual.comment());
    Assertions.assertEquals(expected.autoIncrement(), actual.autoIncrement());
    if (expected.defaultValue().equals(Column.DEFAULT_VALUE_NOT_SET) && expected.nullable()) {
      Assertions.assertEquals(LiteralDTO.NULL, actual.defaultValue());
    } else {
      Assertions.assertEquals(expected.defaultValue(), actual.defaultValue());
    }
  }

  public static int getAvailablePort() {
    int max = 65535;
    int min = 2000;
    Random random = new Random();
    int port = random.nextInt(max) % (max - min + 1) + min;
    if (isLocalPortUsing(port)) {
      return getAvailablePort();
    } else {
      return port;
    }
  }

  private static boolean isLocalPortUsing(int port) {
    try (Socket socket = new Socket("127.0.0.1", port)) {
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  private ITUtils() {}
}
