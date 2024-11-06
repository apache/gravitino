/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class ReadTableCSV {

  private enum Column {
    NAME("Name"),
    DATATYPE("Datatype"),
    COMMENT("Comment"),
    NULLABLE("Nullable"),
    AUTOINCREMENT("AutoIncrement"),
    DEFAULTVALUE("DefaultValue"),
    DEFAULTTYPE("DefaultType");

    private final String name;

    Column(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  public Map<String, List<String>> parse(String csvFile) {

    // Initialize a Map to store each column's values in a list
    HashMap<String, List<String>> tableData = new HashMap<>();
    for (Column column : Column.values()) {
      tableData.put(column.getName(), new ArrayList<>());
    }

    try (BufferedReader reader =
        Files.newBufferedReader(Paths.get(csvFile), StandardCharsets.UTF_8)) {
      CSVParser csvParser =
          new CSVParser(
              reader,
              CSVFormat.Builder.create()
                  .setHeader(
                      Arrays.stream(Column.values()).map(Column::getName).toArray(String[]::new))
                  .setIgnoreHeaderCase(true)
                  .setSkipHeaderRecord(true)
                  .setTrim(true)
                  .setIgnoreEmptyLines(true)
                  .build());
      for (CSVRecord cvsRecord : csvParser) {
        String defaultValue = null;
        String value = null;

        for (Column column : Column.values()) {
          switch (column) {
            case NULLABLE:
              defaultValue = "true";
              break;
            case AUTOINCREMENT:
              defaultValue = "false";
              break;
            default:
              defaultValue = null;
              break;
          }

          try {
            value = cvsRecord.get(column.getName());
          } catch (IllegalArgumentException exp) {
            value = defaultValue; // missing value
          }

          tableData.get(column.getName()).add(value);
        }
      }
    } catch (IOException exp) {
      System.err.println(exp.getMessage());
    }

    return tableData;
  }
}
