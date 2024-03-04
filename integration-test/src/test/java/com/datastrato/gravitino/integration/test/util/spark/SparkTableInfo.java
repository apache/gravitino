/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.util.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;

/** The output of SparkSQL is parsed to SparkTableInfo to check the result in test. */
@Data
public class SparkTableInfo {
  private String name;
  private String database;
  private String comment;
  private List<SparkColumnInfo> columns = new ArrayList<>();
  private Map<String, String> tableProperties;
  private List<String> unknownItems = new ArrayList<>();

  public SparkTableInfo() {}

  public String getTableName() {
    return name;
  }

  // Include database name and table name
  public String getTableIdentifier() {
    if (StringUtils.isNotBlank(database)) {
      return String.join(".", database, name);
    } else {
      return name;
    }
  }

  @Data
  public static class SparkColumnInfo {
    private String name;
    private String type;
    private String comment;

    private SparkColumnInfo(String name, String type, String comment) {
      this.name = name;
      this.type = type;
      this.comment = comment;
    }

    public static SparkColumnInfo of(String name, String type) {
      return of(name, type, null);
    }

    public static SparkColumnInfo of(String name, String type, String comment) {
      return new SparkColumnInfo(name, type, comment);
    }
  }

  private enum ParseStage {
    COLUMN,
    DETECT,
    PARTITION,
    PARTITIONBUCKET,
    TABLE_INFO,
  }

  static SparkTableInfo getSparkTableInfo(List<Object[]> rows) {
    ParseStage stage = ParseStage.COLUMN;
    SparkTableInfo tableInfo = new SparkTableInfo();
    for (Object[] os : rows) {
      String[] items =
          Arrays.stream(os)
              .map(o -> Optional.ofNullable(o).map(Object::toString).orElse(null))
              .toArray(String[]::new);
      Assertions.assertTrue(items.length == 3);
      if (items[0].startsWith("# Detailed Table Information")) {
        stage = ParseStage.TABLE_INFO;
        continue;
      } else if (items[0].startsWith("# Partition Information")) {
        stage = ParseStage.PARTITION;
        continue;
      } else if (items[0].startsWith("# Partitioning")) {
        stage = ParseStage.PARTITIONBUCKET;
        continue;
      }
      if (items[0].isEmpty()) {
        stage = ParseStage.DETECT;
        continue;
      }
      if (stage.equals(ParseStage.COLUMN)) {
        tableInfo.addColumn(SparkColumnInfo.of(items[0], items[1], items[2]));
      } else if (stage.equals(ParseStage.DETECT)) {
        String item = items[0];
        tableInfo.addUnknownItem(item);
      } else if (stage.equals(ParseStage.PARTITION)) {
        // TO: will implement in partition PR
      } else if (stage.equals(ParseStage.PARTITIONBUCKET)) {
        // TO: will implement in partition PR
      } else if (stage.equals(ParseStage.TABLE_INFO)) {
        switch (items[0]) {
          case "Name":
            String[] identifiers = items[1].split("\\.");
            Assertions.assertTrue(
                identifiers.length <= 2, "Table name is not validate," + items[1]);
            if (identifiers.length == 2) {
              tableInfo.database = identifiers[0];
              tableInfo.name = identifiers[1];
            } else {
              tableInfo.name = items[1];
            }
            break;
          case "Table Properties":
            tableInfo.tableProperties = parseTableProperty(items[1]);
            break;
          case "Comment":
            tableInfo.comment = items[1];
            break;
          default:
            tableInfo.addUnknownItem(String.join("@", items));
        }
      }
    }
    return tableInfo;
  }

  // TODO: will implement in the property PR.
  private static Map<String, String> parseTableProperty(String str) {
    return new HashMap<>();
  }

  private void addColumn(SparkColumnInfo column) {
    columns.add(column);
  }

  private void addUnknownItem(String item) {
    unknownItems.add(item);
  }
}
