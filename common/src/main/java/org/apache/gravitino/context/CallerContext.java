/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.context;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import java.util.Map;

/**
 * A class defining the caller context for auditing coarse granularity operations.
 *
 * <p>Reference:
 *
 * <p>hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/\ ipc/CallerContext.java
 */
public class CallerContext {
  private Map<String, String> context;

  private CallerContext() {}

  public Map<String, String> context() {
    return context;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof CallerContext)) return false;
    CallerContext context1 = (CallerContext) o;
    return Objects.equal(context, context1.context);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(context);
  }

  public static class Builder {
    private final CallerContext callerContext;

    private Builder() {
      callerContext = new CallerContext();
    }

    /**
     * Sets the context for CallerContext
     *
     * @param context The context to set.
     * @return This Builder instance for method chaining.
     */
    public CallerContext.Builder withContext(Map<String, String> context) {
      callerContext.context = context;
      return this;
    }

    private void validate() {
      Preconditions.checkArgument(callerContext.context != null, "context cannot be null");
    }

    public CallerContext build() {
      validate();
      return callerContext;
    }
  }

  /**
   * Create a new builder for the CallerContext.
   *
   * @return A new builder for the CallerContext.
   */
  public static CallerContext.Builder builder() {
    return new CallerContext.Builder();
  }

  public static class CallerContextHolder {

    private static final ThreadLocal<CallerContext> CALLER_CONTEXT = new ThreadLocal<>();

    public static CallerContext get() {
      return CALLER_CONTEXT.get();
    }

    public static void set(CallerContext context) {
      CALLER_CONTEXT.set(context);
    }

    public static void remove() {
      CALLER_CONTEXT.remove();
    }
  }
}
