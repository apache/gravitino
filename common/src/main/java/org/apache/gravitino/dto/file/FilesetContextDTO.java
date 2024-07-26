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

package org.apache.gravitino.dto.file;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.gravitino.file.FilesetContext;

/** Represents a Fileset context DTO (Data Transfer Object). */
@NoArgsConstructor(access = AccessLevel.PRIVATE, force = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@EqualsAndHashCode
public class FilesetContextDTO implements FilesetContext {
  @JsonProperty("fileset")
  private FilesetDTO fileset;

  @JsonProperty("actualPath")
  private String actualPath;

  public FilesetDTO fileset() {
    return fileset;
  }

  public String actualPath() {
    return actualPath;
  }

  @Builder(builderMethodName = "builder")
  private static FilesetContextDTO internalBuilder(FilesetDTO fileset, String actualPath) {
    Preconditions.checkNotNull(fileset, "fileset cannot be null");
    Preconditions.checkNotNull(actualPath, "actual path cannot be null");

    return new FilesetContextDTO(fileset, actualPath);
  }
}
