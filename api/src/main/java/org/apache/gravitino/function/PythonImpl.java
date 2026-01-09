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

/** Python implementation with handler and optional inline code. */
public class PythonImpl extends FunctionImpl {
  private final String handler;
  private final String codeBlock;

  PythonImpl(
      RuntimeType runtime,
      String handler,
      String codeBlock,
      FunctionResources resources,
      Map<String, String> properties) {
    super(Language.PYTHON, runtime, resources, properties);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(handler), "Python handler cannot be null or empty");
    this.handler = handler;
    this.codeBlock = codeBlock;
  }

  /**
   * @return The handler entrypoint.
   */
  public String handler() {
    return handler;
  }

  /**
   * @return The Python UDF code block.
   */
  public String codeBlock() {
    return codeBlock;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof PythonImpl)) {
      return false;
    }
    PythonImpl that = (PythonImpl) obj;
    return Objects.equals(language(), that.language())
        && Objects.equals(runtime(), that.runtime())
        && Objects.equals(resources(), that.resources())
        && Objects.equals(properties(), that.properties())
        && Objects.equals(handler, that.handler)
        && Objects.equals(codeBlock, that.codeBlock);
  }

  @Override
  public int hashCode() {
    return Objects.hash(language(), runtime(), resources(), properties(), handler, codeBlock);
  }

  @Override
  public String toString() {
    return "PythonImpl{"
        + "language='"
        + language()
        + '\''
        + ", runtime='"
        + runtime()
        + '\''
        + ", handler='"
        + handler
        + '\''
        + ", codeBlock='"
        + codeBlock
        + '\''
        + ", resources="
        + resources()
        + ", properties="
        + properties()
        + '}';
  }
}
