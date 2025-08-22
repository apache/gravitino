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
package org.apache.gravitino.dto.file;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.file.FileInfo;

/** Represents a FileInfo DTO (Data Transfer Object). */
@EqualsAndHashCode
@JsonIgnoreProperties({"dir"})
public class FileInfoDTO implements FileInfo {

  @JsonProperty("name")
  private String name;

  @JsonProperty("isDir")
  private boolean isDir;

  @JsonProperty("size")
  private long size;

  @JsonProperty("lastModified")
  private long lastModified;

  @JsonProperty("path")
  private String path;

  private FileInfoDTO() {}

  private FileInfoDTO(String name, boolean isDir, long size, long lastModified, String path) {
    this.name = name;
    this.isDir = isDir;
    this.size = size;
    this.lastModified = lastModified;
    this.path = path;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public boolean isDir() {
    return isDir;
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public long lastModified() {
    return lastModified;
  }

  @Override
  public String path() {
    return path;
  }

  /**
   * Create a new FileInfoDTO builder.
   *
   * @return A new FileInfoDTO builder.
   */
  public static FileInfoDTO.FileInfoDTOBuilder builder() {
    return new FileInfoDTO.FileInfoDTOBuilder();
  }

  /** Builder for FileInfoDTO. */
  public static class FileInfoDTOBuilder {
    private String name;
    private boolean isDir;
    private long size;
    private long lastModified;
    private String path;

    private FileInfoDTOBuilder() {}

    /**
     * Set the name of the FileInfo.
     *
     * @param name The name of the file.
     * @return The builder instance.
     */
    public FileInfoDTO.FileInfoDTOBuilder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * Set the isDir of the FileInfo.
     *
     * @param isDir The isDir of the file.
     * @return The builder instance.
     */
    public FileInfoDTO.FileInfoDTOBuilder isDir(boolean isDir) {
      this.isDir = isDir;
      return this;
    }

    /**
     * Set the size of the FileInfo.
     *
     * @param size The size of the file.
     * @return The builder instance.
     */
    public FileInfoDTO.FileInfoDTOBuilder size(long size) {
      this.size = size;
      return this;
    }

    /**
     * Set the lastModified of the FileInfo.
     *
     * @param lastModified The lastModified of the file.
     * @return The builder instance.
     */
    public FileInfoDTO.FileInfoDTOBuilder lastModified(long lastModified) {
      this.lastModified = lastModified;
      return this;
    }

    /**
     * Set the path of the FileInfo.
     *
     * @param path The path of the file.
     * @return The builder instance.
     */
    public FileInfoDTO.FileInfoDTOBuilder path(String path) {
      this.path = path;
      return this;
    }

    /**
     * Build the FileInfoDTO.
     *
     * @return The built FileInfoDTO.
     */
    public FileInfoDTO build() {
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "name cannot be null or empty");
      Preconditions.checkArgument(size >= 0, "size cannot be negative");
      // In Azure Blob Storage, it can be 0 for newly created files.
      Preconditions.checkArgument(lastModified >= 0, "lastModified must be a valid timestamp");
      Preconditions.checkArgument(StringUtils.isNotBlank(path), "path cannot be null or empty");

      return new FileInfoDTO(name, isDir, size, lastModified, path);
    }
  }
}
