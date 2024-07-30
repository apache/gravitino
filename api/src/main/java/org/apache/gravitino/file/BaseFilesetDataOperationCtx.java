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

package org.apache.gravitino.file;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

/** Base implementation of {@link FilesetDataOperationCtx}. */
public class BaseFilesetDataOperationCtx implements FilesetDataOperationCtx {
  private String subPath;
  private String operation;
  private String clientType;

  @Override
  public String subPath() {
    return subPath;
  }

  @Override
  public String operation() {
    return operation;
  }

  @Override
  public String clientType() {
    return clientType;
  }

  /** Builder for {@link BaseFilesetDataOperationCtx}. */
  public static class Builder {
    private BaseFilesetDataOperationCtx context;
    /** Creates a new instance of {@link BaseFilesetDataOperationCtx.Builder}. */
    private Builder() {
      context = new BaseFilesetDataOperationCtx();
    }

    /**
     * Set the sub path of this data operation.
     *
     * @param subPath The subPath of this data operation.
     * @return The builder.
     */
    public BaseFilesetDataOperationCtx.Builder withSubPath(String subPath) {
      context.subPath = subPath;
      return this;
    }

    /**
     * Set the type of this data operation.
     *
     * @param operation The type of this data operation.
     * @return The builder.
     */
    public BaseFilesetDataOperationCtx.Builder withOperation(String operation) {
      context.operation = operation;
      return this;
    }

    /**
     * Set the client type of this data operation.
     *
     * @param clientType The client type of this data operation.
     * @return The builder.
     */
    public BaseFilesetDataOperationCtx.Builder withClientType(String clientType) {
      context.clientType = clientType;
      return this;
    }

    private void validate() {
      Preconditions.checkArgument(context.subPath != null, "subPath cannot be null");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(context.operation), "operation is required");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(context.clientType), "clientType is required");
    }

    /**
     * Build the {@link BaseFilesetDataOperationCtx}.
     *
     * @return The created BaseFilesetDataOperationCtx.
     */
    public BaseFilesetDataOperationCtx build() {
      validate();
      return context;
    }
  }

  /**
   * Creates a new instance of {@link Builder}.
   *
   * @return The new instance.
   */
  public static Builder builder() {
    return new Builder();
  }
}
