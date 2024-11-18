package org.apache.gravitino.cli;

import java.util.Arrays;
import java.util.List;

@SuppressWarnings("unused")
public class TablePrinter {
  private int columns = 0;
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
    if (headers.size() != rows.get(0).size()) {
      throw new IllegalArgumentException("Number of columns is not equal.");
    }
    maxElementLengths = new int[headers.size()];
    updateMaxLengthsFromList(headers);
    updateMaxLengthsFromNestedList(rows);
    this.columns = headers.size();

    // print headers
    printLine();
    System.out.println();
    for (int i = 0; i < headers.size(); ++i) {
      System.out.printf(
          verticalDelimiter + indent + "%-" + maxElementLengths[i] + "s" + indent, headers.get(i));
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
