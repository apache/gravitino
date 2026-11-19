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
package org.apache.gravitino.iceberg.common.ops;

import java.util.Objects;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.rest.requests.RegisterTableRequest;

/**
 * Carries the metadata snapshot read for a single registration through authorization and commit.
 */
public final class RegisterTableRequestWithMetadata implements RegisterTableRequest {
  private final RegisterTableRequest request;
  private final TableMetadata metadata;

  /**
   * Wraps a registration with the metadata already read from its requested file.
   *
   * @param request original registration
   * @param metadata metadata snapshot read for this request
   */
  public RegisterTableRequestWithMetadata(RegisterTableRequest request, TableMetadata metadata) {
    this.request = Objects.requireNonNull(request, "request");
    this.metadata = Objects.requireNonNull(metadata, "metadata");
  }

  /**
   * Returns this request's metadata snapshot; this is not a cross-request cache.
   *
   * @return metadata snapshot
   */
  public TableMetadata tableMetadata() {
    return metadata;
  }

  /** {@inheritDoc} */
  @Override
  public String name() {
    return request.name();
  }

  /** {@inheritDoc} */
  @Override
  public String metadataLocation() {
    return request.metadataLocation();
  }

  /** {@inheritDoc} */
  @Override
  public boolean overwrite() {
    return request.overwrite();
  }
}
