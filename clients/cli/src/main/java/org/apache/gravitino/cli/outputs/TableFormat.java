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
package org.apache.gravitino.cli.outputs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Metalake;

/** Table format to print a pretty table to standard out. */
public class TableFormat {
  public static void output(Object object) {
    if (object instanceof Metalake) {
      new MetalakeTableFormat().output((Metalake) object);
    } else if (object instanceof Metalake[]) {
      new MetalakesTableFormat().output((Metalake[]) object);
    } else if (object instanceof Catalog) {
      new CatalogTableFormat().output((Catalog) object);
    } else if (object instanceof Catalog[]) {
      new CatalogsTableFormat().output((Catalog[]) object);
    } else {
      throw new IllegalArgumentException("Unsupported object type");
    }
  }

  static final class MetalakeTableFormat implements OutputFormat<Metalake> {
    @Override
    public void output(Metalake metalake) {
      List<String> headers = Arrays.asList("metalake", "comment");
      List<List<String>> rows = new ArrayList<>();
      rows.add(Arrays.asList(metalake.name(), metalake.comment()));
      TableFormatImpl tableFormat = new TableFormatImpl();
      tableFormat.print(headers, rows);
    }
  }

  static final class MetalakesTableFormat implements OutputFormat<Metalake[]> {
    @Override
    public void output(Metalake[] metalakes) {
      List<String> headers = Collections.singletonList("metalake");
      List<List<String>> rows = new ArrayList<>();
      for (int i = 0; i < metalakes.length; i++) {
        rows.add(Arrays.asList(metalakes[i].name()));
      }
      TableFormatImpl tableFormat = new TableFormatImpl();
      tableFormat.print(headers, rows);
    }
  }

  static final class CatalogTableFormat implements OutputFormat<Catalog> {
    @Override
    public void output(Catalog catalog) {
      List<String> headers = Arrays.asList("catalog", "type", "provider", "comment");
      List<List<String>> rows = new ArrayList<>();
      rows.add(
          Arrays.asList(
              catalog.name(),
              catalog.type().toString(),
              catalog.provider(),
              catalog.comment() + ""));
      TableFormatImpl tableFormat = new TableFormatImpl();
      tableFormat.print(headers, rows);
    }
  }

  static final class CatalogsTableFormat implements OutputFormat<Catalog[]> {
    @Override
    public void output(Catalog[] catalogs) {
      List<String> headers = Collections.singletonList("catalog");
      List<List<String>> rows = new ArrayList<>();
      for (int i = 0; i < catalogs.length; i++) {
        rows.add(Arrays.asList(catalogs[i].name()));
      }
      TableFormatImpl tableFormat = new TableFormatImpl();
      tableFormat.print(headers, rows);
    }
  }

  static final class TableFormatImpl {
    private int[] maxElementLengths;
    // This expression is primarily used to match characters that have a display width of
    // 2, such as characters from Korean, Chinese
    private static final Pattern FULL_WIDTH_PATTERN =
        Pattern.compile(
            "[\u1100-\u115F\u2E80-\uA4CF\uAC00-\uD7A3\uF900-\uFAFF\uFE10-\uFE19\uFE30-\uFE6F\uFF00-\uFF60\uFFE0-\uFFE6]");
    private int[][] elementOutputWidths;
    private final String horizontalDelimiter = "-";
    private final String verticalDelimiter = "|";
    private final String crossDelimiter = "+";
    private final String indent = " ";

    public void debug() {
      System.out.println();
      Arrays.stream(maxElementLengths).forEach(e -> System.out.print(e + " "));
    }

    public void print(List<String> headers, List<List<String>> rows) {
      if (rows.size() > 0 && headers.size() != rows.get(0).size()) {
        throw new IllegalArgumentException("Number of columns is not equal.");
      }
      maxElementLengths = new int[headers.size()];
      elementOutputWidths = new int[rows.size()][headers.size()];
      updateMaxLengthsFromList(headers);
      updateMaxLengthsFromNestedList(rows);
      printLine();
      System.out.println();
      for (int i = 0; i < headers.size(); ++i) {
        System.out.printf(
            verticalDelimiter + indent + "%-" + maxElementLengths[i] + "s" + indent,
            headers.get(i));
      }
      System.out.println(verticalDelimiter);
      printLine();
      System.out.println();

      // print rows
      for (int i = 0; i < rows.size(); ++i) {
        List<String> columns = rows.get(i);
        for (int j = 0; j < columns.size(); ++j) {
          String column = columns.get(j);
          // Handle cases where the width and number of characters are inconsistent
          if (elementOutputWidths[i][j] != column.length()) {
            if (elementOutputWidths[i][j] > maxElementLengths[j]) {
              System.out.printf(
                  verticalDelimiter + indent + "%-" + column.length() + "s" + indent, column);
            } else {
              int paddingLength =
                  maxElementLengths[j] - (elementOutputWidths[i][j] - column.length());
              System.out.printf(
                  verticalDelimiter + indent + "%-" + paddingLength + "s" + indent, column);
            }
          } else {
            System.out.printf(
                verticalDelimiter + indent + "%-" + maxElementLengths[j] + "s" + indent, column);
          }
        }
        System.out.println(verticalDelimiter);
      }
      printLine();
      // add one more line
      System.out.println("");
    }

    private void updateMaxLengthsFromList(List<String> elements) {
      String s;
      for (int i = 0; i < elements.size(); ++i) {
        s = elements.get(i);
        if (getOutputWidth(s) > maxElementLengths[i]) maxElementLengths[i] = getOutputWidth(s);
      }
    }

    private void updateMaxLengthsFromNestedList(List<List<String>> elements) {
      int rowIdx = 0;
      for (List<String> row : elements) {
        String s;
        for (int i = 0; i < row.size(); ++i) {
          s = row.get(i);
          int consoleWidth = getOutputWidth(s);
          elementOutputWidths[rowIdx][i] = consoleWidth;
          if (consoleWidth > maxElementLengths[i]) maxElementLengths[i] = consoleWidth;
        }
        rowIdx++;
      }
    }

    private int getOutputWidth(String s) {
      int width = 0;
      for (int i = 0; i < s.length(); i++) {
        width += getCharWidth(s.charAt(i));
      }

      return width;
    }

    private static int getCharWidth(char ch) {
      String s = String.valueOf(ch);
      if (FULL_WIDTH_PATTERN.matcher(s).find()) {
        return 2;
      }

      return 1;
    }

    private void printLine() {
      System.out.print(crossDelimiter);
      for (int i = 0; i < maxElementLengths.length; ++i) {
        for (int j = 0; j < maxElementLengths[i] + indent.length() * 2; ++j) {
          System.out.print(horizontalDelimiter);
        }
        System.out.print(crossDelimiter);
      }
    }
  }
}
