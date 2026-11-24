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
package org.apache.gravitino.semantic;

import java.time.Instant;
import javax.annotation.Nullable;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.annotation.Evolving;

/** Metadata of a retained Semantic Model revision. The audit information describes its writer. */
@Evolving
public interface SemanticModelRevision extends Auditable {

  /** Returns the revision number, starting at one. */
  int revision();

  /** Returns the revision's publication status. */
  SemanticModel.Status status();

  /** Returns the publishing principal, or null for a draft. */
  @Nullable
  String publishedBy();

  /** Returns the publication time, or null for a draft. */
  @Nullable
  Instant publishedAt();
}
