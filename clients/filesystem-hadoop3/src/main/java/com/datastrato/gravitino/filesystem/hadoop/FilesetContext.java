/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.shaded.com.google.common.base.Preconditions;
import com.google.common.base.Objects;
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
