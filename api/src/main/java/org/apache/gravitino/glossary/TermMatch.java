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
package org.apache.gravitino.glossary;

import com.google.common.base.Preconditions;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;

/**
 * A word that matched a glossary term in a lookup, and the field it matched. A caller groups the
 * terms of a lookup by {@link #word()}.
 */
@Evolving
public final class TermMatch {

  /** The field of a term that a word matched. */
  public enum Field {
    /** The word matched the name of the term. */
    NAME,
    /** The word matched a synonym of the term. */
    SYNONYM
  }

  private final String word;
  private final Field field;

  private TermMatch(String word, Field field) {
    this.word = word;
    this.field = field;
  }

  /**
   * Creates a match.
   *
   * @param word The word of the lookup, as the caller gave it.
   * @param field The field of the term that the word matched.
   * @return The match.
   * @throws IllegalArgumentException If the word is blank or the field is null.
   */
  public static TermMatch of(String word, Field field) {
    Preconditions.checkArgument(StringUtils.isNotBlank(word), "Matched word cannot be blank");
    Preconditions.checkArgument(field != null, "Matched field cannot be null");
    return new TermMatch(word, field);
  }

  /**
   * Returns the word of the lookup, as the caller gave it.
   *
   * @return The word.
   */
  public String word() {
    return word;
  }

  /**
   * Returns the field of the term that the word matched.
   *
   * @return The field.
   */
  public Field field() {
    return field;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TermMatch)) {
      return false;
    }
    TermMatch that = (TermMatch) o;
    return Objects.equals(word, that.word) && field == that.field;
  }

  @Override
  public int hashCode() {
    return Objects.hash(word, field);
  }

  @Override
  public String toString() {
    return "TermMatch{word=" + word + ", field=" + field + "}";
  }
}
