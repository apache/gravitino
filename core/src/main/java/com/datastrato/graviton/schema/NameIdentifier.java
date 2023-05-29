package com.datastrato.graviton.schema;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import java.util.Arrays;

public class NameIdentifier {

  private static final Splitter DOT = Splitter.on('.');

  private final Namespace namespace;

  private final String name;

  public static NameIdentifier of(String... names) {
    Preconditions.checkArgument(names != null, "Cannot create a NameIdentifier with null names");
    Preconditions.checkArgument(names.length > 0, "Cannot create a NameIdentifier with no names");

    return new NameIdentifier(
        Namespace.of(Arrays.copyOf(names, names.length - 1)), names[names.length - 1]);
  }

  public static NameIdentifier of(Namespace namespace, String name) {
    return new NameIdentifier(namespace, name);
  }

  public static NameIdentifier parse(String identifier) {
    Preconditions.checkArgument(
        identifier != null && !identifier.isEmpty(), "Cannot parse a null or empty identifier");

    Iterable<String> parts = DOT.split(identifier);
    return NameIdentifier.of(Iterables.toArray(parts, String.class));
  }

  private NameIdentifier(Namespace namespace, String name) {
    Preconditions.checkArgument(
        namespace != null, "Cannot create a NameIdentifier with null namespace");
    Preconditions.checkArgument(
        name != null && !name.isEmpty(), "Cannot create a NameIdentifier with null or empty name");

    this.namespace = namespace;
    this.name = name;
  }

  public boolean hasNamespace() {
    return !namespace.isEmpty();
  }

  public Namespace namespace() {
    return namespace;
  }

  public String name() {
    return name;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || !(other instanceof NameIdentifier)) {
      return false;
    }

    NameIdentifier otherNameIdentifier = (NameIdentifier) other;
    return namespace.equals(otherNameIdentifier.namespace) && name.equals(otherNameIdentifier.name);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(new int[]{namespace.hashCode(), name.hashCode()});
  }

  @Override
  public String toString() {
    if (hasNamespace()) {
      return namespace.toString() + "." + name;
    } else {
      return name;
    }
  }
}
