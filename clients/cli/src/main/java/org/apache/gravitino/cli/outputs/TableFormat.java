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
import org.apache.gravitino.Metalake;

/** Table format to print a pretty table to standard out. */
public class TableFormat {
  public static void output(Object object) {
    if (object instanceof Metalake) {
      new MetalakeTableFormat().output((Metalake) object);
    } else if (object instanceof Metalake[]) {
      new MetalakesTableFormat().output((Metalake[]) object);
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

  static final class TableFormatImpl {
    private int[] maxElementLengths;
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
      updateMaxLengthsFromList(headers);
      updateMaxLengthsFromNestedList(rows);

      // print headers
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
          System.out.printf(
              verticalDelimiter + indent + "%-" + maxElementLengths[j] + "s" + indent,
              columns.get(j));
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
        if (s.length() > maxElementLengths[i]) maxElementLengths[i] = s.length();
      }
    }

    private void updateMaxLengthsFromNestedList(List<List<String>> elements) {
      for (List<String> row : elements) {
        String s;
        for (int i = 0; i < row.size(); ++i) {
          s = row.get(i);
          if (s.length() > maxElementLengths[i]) maxElementLengths[i] = s.length();
        }
      }
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
