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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.gravitino.cli.ReadTableCSV;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestReadTableCSV {

  private ReadTableCSV readTableCSV;
  private Path tempFile;

  @BeforeEach
  public void setUp() throws IOException {
    readTableCSV = new ReadTableCSV();
    tempFile = Files.createTempFile("test-data", ".csv");

    // Write the header the CSV file
    try (BufferedWriter writer = Files.newBufferedWriter(tempFile, StandardCharsets.UTF_8)) {
      CSVPrinter csvPrinter =
          new CSVPrinter(
              writer,
              CSVFormat.Builder.create()
                  .setHeader(
                      "Name",
                      "Datatype",
                      "Comment",
                      "Nullable",
                      "AutoIncrement",
                      "DefaultValue",
                      "DefaultType")
                  .build());

      // Print records to the CSV file
      csvPrinter.printRecord("name", "String", "Sample comment");
      csvPrinter.printRecord("ID", "Integer", "Another comment", "false", "true", "0");
      csvPrinter.printRecord(
          "location", "String", "More comments", "false", "false", "Sydney", "String");
    }
  }

  @Test
  public void testParse() {
    Map<String, List<String>> tableData = readTableCSV.parse(tempFile.toString());

    // Validate the data in the tableData map
    assertEquals(
        Arrays.asList("name", "ID", "location"), tableData.get("Name"), "Name column should match");
    assertEquals(
        Arrays.asList("String", "Integer", "String"),
        tableData.get("Datatype"),
        "Datatype column should match");
    assertEquals(
        Arrays.asList("Sample comment", "Another comment", "More comments"),
        tableData.get("Comment"),
        "Comment column should match");
    assertEquals(
        Arrays.asList("true", "false", "false"),
        tableData.get("Nullable"),
        "Nullable column should match");
    assertEquals(
        Arrays.asList("false", "true", "false"),
        tableData.get("AutoIncrement"),
        "AutoIncrement column should match");
    assertEquals(
        Arrays.asList(null, "0", "Sydney"),
        tableData.get("DefaultValue"),
        "DefaultValue column should match");
    assertEquals(
        Arrays.asList(null, null, "String"),
        tableData.get("DefaultType"),
        "DefaultType column should match");
  }
}
