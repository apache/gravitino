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

import java.util.HashMap;
import java.util.Map;

/**
 * A utility class to parse a delimited list of name-value pairs into a List of key-value entries.
 *
 * <p>This class can be used to parse strings of the format "key1=value1,key2=value2" where the
 * delimiter and key-value separator can be customized.
 */
public class Properties {
  private String delimiter;
  private String keyValueSeparator;

  /** Default constructor, sets the delimiter to "," and the key-value separator to "=". */
  public Properties() {
    this.delimiter = ",";
    this.keyValueSeparator = "=";
  }

  /**
   * Constructor that allows setting custom delimiters.
   *
   * @param delimiter The delimiter used to separate pairs in the input string.
   * @param keyValueSeparator The separator used to distinguish keys from values in each pair.
   */
  public Properties(String delimiter, String keyValueSeparator) {
    this.delimiter = delimiter;
    this.keyValueSeparator = keyValueSeparator;
  }

  /**
   * Parses a delimited string of name-value pairs into a map of key-value entries.
   *
   * <p>Each pair in the input string is split by the specified delimiter, and then each pair is
   * further split by the key-value separator.
   *
   * @param inputs An arrays of input strings containing name-value pairs.
   * @return A map of entries, where each entry represents a key-value pair from the input string.
   */
  public Map<String, String> parse(String[] inputs) {
    HashMap<String, String> map = new HashMap<>();

    for (String input : inputs) {
      // Split the input by the delimiter into key-value pairs
      String[] pairs = input.split(delimiter);
      for (String pair : pairs) {
        // Split each key-value pair by the separator
        String[] keyValue = pair.split(keyValueSeparator, 2);
        if (keyValue.length == 2) {
          map.put(keyValue[0].trim(), keyValue[1].trim());
        }
      }
    }

    return map;
  }
}
