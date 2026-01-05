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
package org.apache.gravitino.function;

import java.util.Arrays;
import java.util.Objects;
import org.apache.gravitino.annotation.Evolving;

/** Represents external resources that are required by a function implementation. */
@Evolving
public class FunctionResources {
  private static final String[] EMPTY = new String[0];

  private final String[] jars;
  private final String[] files;
  private final String[] archives;

  private FunctionResources(String[] jars, String[] files, String[] archives) {
    this.jars = jars == null ? EMPTY : Arrays.copyOf(jars, jars.length);
    this.files = files == null ? EMPTY : Arrays.copyOf(files, files.length);
    this.archives = archives == null ? EMPTY : Arrays.copyOf(archives, archives.length);
  }

  /**
   * @return An empty {@link FunctionResources} instance.
   */
  public static FunctionResources empty() {
    return new FunctionResources(EMPTY, EMPTY, EMPTY);
  }

  /**
   * Create a {@link FunctionResources} instance.
   *
   * @param jars The jar resources.
   * @param files The file resources.
   * @param archives The archive resources.
   * @return A {@link FunctionResources} instance.
   */
  public static FunctionResources of(String[] jars, String[] files, String[] archives) {
    return new FunctionResources(jars, files, archives);
  }

  /**
   * @return The jar resources.
   */
  public String[] jars() {
    return jars.length == 0 ? EMPTY : Arrays.copyOf(jars, jars.length);
  }

  /**
   * @return The file resources.
   */
  public String[] files() {
    return files.length == 0 ? EMPTY : Arrays.copyOf(files, files.length);
  }

  /**
   * @return The archive resources.
   */
  public String[] archives() {
    return archives.length == 0 ? EMPTY : Arrays.copyOf(archives, archives.length);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof FunctionResources)) {
      return false;
    }
    FunctionResources that = (FunctionResources) obj;
    return Arrays.equals(jars, that.jars)
        && Arrays.equals(files, that.files)
        && Arrays.equals(archives, that.archives);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(Arrays.hashCode(jars), Arrays.hashCode(files));
    result = 31 * result + Arrays.hashCode(archives);
    return result;
  }

  @Override
  public String toString() {
    return "FunctionResources{"
        + "jars="
        + Arrays.toString(jars)
        + ", files="
        + Arrays.toString(files)
        + ", archives="
        + Arrays.toString(archives)
        + '}';
  }
}
