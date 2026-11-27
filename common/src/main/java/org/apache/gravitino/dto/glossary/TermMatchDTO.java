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
package org.apache.gravitino.dto.glossary;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import org.apache.gravitino.glossary.TermMatch;

/** An original lookup word and the field that matched it. */
public final class TermMatchDTO {
  private final String word;
  private final TermMatch.Field field;

  /**
   * Creates a word match for a term.
   *
   * @param word the word as supplied by the caller
   * @param field the field that matched the normalized word
   */
  @JsonCreator
  public TermMatchDTO(
      @JsonProperty("word") String word, @JsonProperty("field") TermMatch.Field field) {
    this.word = word;
    this.field = field;
  }

  /**
   * @return the original lookup word
   */
  public String getWord() {
    return word;
  }

  /**
   * @return the matching field
   */
  @JsonSerialize(using = ToStringSerializer.class)
  public TermMatch.Field getField() {
    return field;
  }
}
