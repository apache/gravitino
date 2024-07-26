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

package org.apache.gravitino;

import lombok.EqualsAndHashCode;
import org.apache.gravitino.connector.BaseFilesetContext;

@EqualsAndHashCode(callSuper = true)
public class TestFilesetContext extends BaseFilesetContext {
  public static class Builder
      extends BaseFilesetContext.BaseFilesetContextBuilder<
          TestFilesetContext.Builder, TestFilesetContext> {
    /** Creates a new instance of {@link TestFilesetContext.Builder}. */
    private Builder() {}

    @Override
    protected TestFilesetContext internalBuild() {
      TestFilesetContext context = new TestFilesetContext();
      context.fileset = fileset;
      context.actualPath = actualPath;
      return context;
    }
  }

  /**
   * Creates a new instance of {@link TestFilesetContext.Builder}.
   *
   * @return The new instance.
   */
  public static TestFilesetContext.Builder builder() {
    return new TestFilesetContext.Builder();
  }
}
