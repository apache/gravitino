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
package org.apache.gravitino.storage.relational.po;

import javax.annotation.Nullable;
import lombok.Getter;

/**
 * A flat row from a model-version snapshot query. A row contains URI data, alias data, or only the
 * model ID when no version matches. All rows come from the same statement snapshot.
 */
@Getter
public class ModelVersionSnapshotRowPO {

  private Long modelId;

  @Nullable private Long metalakeId;

  @Nullable private Long catalogId;

  @Nullable private Long schemaId;

  @Nullable private Integer modelVersion;

  @Nullable private String modelVersionComment;

  @Nullable private String modelVersionProperties;

  @Nullable private String modelVersionUriName;

  @Nullable private String modelVersionUri;

  @Nullable private String auditInfo;

  @Nullable private Long deletedAt;

  @Nullable private Integer aliasVersion;

  @Nullable private String aliasName;

  @Nullable private Long aliasDeletedAt;
}
