package com.datastrato.gravitino.integration.test.container;

public enum PGImageName {
  VERSION_12("postgres:12"),
  VERSION_13("postgres:13"),
  VERSION_14("postgres:14"),
  VERSION_15("postgres:15"),
  VERSION_16("postgres:16");

  private final String imageName;

  PGImageName(String imageName) {
    this.imageName = imageName;
  }

  @Override
  public String toString() {
    return this.imageName;
  }
}
