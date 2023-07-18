/*
* Copyright 2023 Datastrato.
* This software is licensed under the Apache License version 2.
*/

package com.datastrato.graviton;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.util.Arrays;

public class Namespace {

  private static final Namespace EMPTY = new Namespace(new String[0]);
  private static final Joiner DOT = Joiner.on('.');

  private final String[] levels;

  public static Namespace empty() {
    return EMPTY;
  }

  public static Namespace of(String... levels) {
    Preconditions.checkArgument(levels != null, "Cannot create a namespace with null levels");
    if (levels.length == 0) {
      return empty();
    }

    for (String level : levels) {
      Preconditions.checkArgument(
          level != null && !level.isEmpty(), "Cannot create a namespace with null or empty level");
    }

    return new Namespace(levels);
  }

  private Namespace(String[] levels) {
    this.levels = levels;
  }

  public String[] levels() {
    return levels;
  }

  public String level(int pos) {
    Preconditions.checkArgument(pos >= 0 && pos < levels.length, "Invalid level position");
    return levels[pos];
  }

  public int length() {
    return levels.length;
  }

  public boolean isEmpty() {
    return levels.length == 0;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof Namespace)) {
      return false;
    }

    Namespace otherNamespace = (Namespace) other;
    return Arrays.equals(levels, otherNamespace.levels);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(levels);
  }

  @Override
  public String toString() {
    return DOT.join(levels);
  }
}
