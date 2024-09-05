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
package org.apache.gravitino.filesystem.hadoop;

import com.google.common.base.Objects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.shaded.com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * A context object that holds the information about the fileset and the file system which used in
 * the {@link GravitinoVirtualFileSystem}'s operations.
 */
class FilesetContext {
  private NameIdentifier identifier;
  private Fileset fileset;
  private FileSystem fileSystem;
  private Path actualPath;

  private FilesetContext() {}

  public NameIdentifier getIdentifier() {
    return identifier;
  }

  public Fileset getFileset() {
    return fileset;
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }

  public Path getActualPath() {
    return actualPath;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof FilesetContext)) return false;
    FilesetContext that = (FilesetContext) o;
    return Objects.equal(getIdentifier(), that.getIdentifier())
        && Objects.equal(getFileset(), that.getFileset())
        && Objects.equal(getFileSystem(), that.getFileSystem())
        && Objects.equal(getActualPath(), that.getActualPath());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getIdentifier(), getFileset(), getFileSystem(), getActualPath());
  }

  public static Builder builder() {
    return new Builder();
  }

  /** A builder class for {@link FilesetContext}. */
  public static class Builder {

    private final FilesetContext context;

    private Builder() {
      this.context = new FilesetContext();
    }

    public Builder withIdentifier(NameIdentifier identifier) {
      context.identifier = identifier;
      return this;
    }

    public Builder withFileSystem(FileSystem fileSystem) {
      context.fileSystem = fileSystem;
      return this;
    }

    public Builder withFileset(Fileset fileset) {
      context.fileset = fileset;
      return this;
    }

    public Builder withActualPath(Path actualPath) {
      context.actualPath = actualPath;
      return this;
    }

    public FilesetContext build() {
      Preconditions.checkArgument(context.identifier != null, "Identifier is required");
      Preconditions.checkArgument(context.fileset != null, "Fileset is required");
      Preconditions.checkArgument(context.fileSystem != null, "FileSystem is required");
      Preconditions.checkArgument(context.actualPath != null, "ActualPath is required");
      return context;
    }
  }
}
