/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.filesystem.hadoop;

import com.datastrato.gravitino.file.Fileset;
import org.apache.curator.shaded.com.google.common.base.Objects;
import org.apache.curator.shaded.com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileSystem;

/**
 * FilesetMeta is a class that contains the metadata for a fileset. It contains the fileset and the
 * file system that the fileset storage location corresponding to.
 */
public class FilesetMeta {
  private Fileset fileset;
  private FileSystem fileSystem;

  private FilesetMeta(Fileset fileset, FileSystem fileSystem) {
    this.fileset = fileset;
    this.fileSystem = fileSystem;
  }

  public Fileset getFileset() {
    return fileset;
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FilesetMeta)) {
      return false;
    }
    FilesetMeta meta = (FilesetMeta) o;
    return Objects.equal(getFileset(), meta.getFileset())
        && Objects.equal(getFileSystem(), meta.getFileSystem());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getFileset(), getFileSystem());
  }

  /**
   * Creates a new builder for constructing a FilesetMeta.
   *
   * @return A new instance of the Builder class for constructing a FilesetMeta.
   */
  public static FilesetMeta.Builder builder() {
    return new FilesetMeta.Builder();
  }

  public static class Builder {
    private Fileset fileset;
    private FileSystem fileSystem;

    private Builder() {}

    public Builder withFileset(Fileset fileset) {
      this.fileset = fileset;
      return this;
    }

    public Builder withFileSystem(FileSystem fileSystem) {
      this.fileSystem = fileSystem;
      return this;
    }

    public FilesetMeta build() {
      Preconditions.checkArgument(
          fileset != null && fileSystem != null, "Fileset and FileSystem must be set");
      return new FilesetMeta(fileset, fileSystem);
    }
  }
}
