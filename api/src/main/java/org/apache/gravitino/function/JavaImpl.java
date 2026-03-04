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

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

/** Java implementation with class name. */
public class JavaImpl extends FunctionImpl {
  private final String className;

  JavaImpl(
      RuntimeType runtime,
      String className,
      FunctionResources resources,
      Map<String, String> properties) {
    super(Language.JAVA, runtime, resources, properties);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(className), "Java class name cannot be null or empty");
    this.className = className;
  }

  /**
   * @return The fully qualified class name.
   */
  public String className() {
    return className;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof JavaImpl)) {
      return false;
    }
    JavaImpl that = (JavaImpl) obj;
    return Objects.equals(language(), that.language())
        && Objects.equals(runtime(), that.runtime())
        && Objects.equals(resources(), that.resources())
        && Objects.equals(properties(), that.properties())
        && Objects.equals(className, that.className);
  }

  @Override
  public int hashCode() {
    return Objects.hash(language(), runtime(), resources(), properties(), className);
  }

  @Override
  public String toString() {
    return "JavaImpl{"
        + "language='"
        + language()
        + '\''
        + ", runtime='"
        + runtime()
        + '\''
        + ", className='"
        + className
        + '\''
        + ", resources="
        + resources()
        + ", properties="
        + properties()
        + '}';
  }
}
