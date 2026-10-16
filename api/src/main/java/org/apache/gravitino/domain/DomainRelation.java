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
package org.apache.gravitino.domain;

import com.google.common.base.Preconditions;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.annotation.Evolving;

/**
 * How a leaf metadata object relates to a domain, always relative to the listed domain. An object
 * can relate to a domain in several ways at once, and every relation that applies is reported, with
 * no precedence among them. For example, a table whose schema carries the backing tag of "Finance"
 * and which itself carries that of its sub-domain "Trading" relates to Finance as {@link
 * Type#INHERITED} from the schema and as {@link Type#VIA_SUBDOMAIN} Trading.
 */
@Evolving
public final class DomainRelation {

  /** The kind of a relation between a metadata object and a domain. */
  public enum Type {
    /** The object itself carries the domain's backing tag. */
    DIRECT,
    /** The object inherits the domain's backing tag from its catalog or schema. */
    INHERITED,
    /** A sub-domain of the domain, visible to the caller, has the object in its scope. */
    VIA_SUBDOMAIN
  }

  private final Type type;
  @Nullable private final MetadataObject source;

  private DomainRelation(Type type, @Nullable MetadataObject source) {
    this.type = type;
    this.source = source;
  }

  /**
   * Creates a relation for an object that carries the domain's backing tag itself.
   *
   * @return The relation.
   */
  public static DomainRelation direct() {
    return new DomainRelation(Type.DIRECT, null);
  }

  /**
   * Creates a relation for an object that inherits the domain's backing tag.
   *
   * @param source The nearest catalog or schema that carries the backing tag.
   * @return The relation.
   * @throws IllegalArgumentException If the source is null, or is neither a catalog nor a schema.
   */
  public static DomainRelation inherited(MetadataObject source) {
    Preconditions.checkArgument(source != null, "source must not be null");
    Preconditions.checkArgument(
        source.type() == MetadataObject.Type.CATALOG || source.type() == MetadataObject.Type.SCHEMA,
        "An inherited relation must come from a catalog or schema, but got %s",
        source.type());
    return new DomainRelation(Type.INHERITED, source);
  }

  /**
   * Creates a relation for an object in the scope of a sub-domain of the domain.
   *
   * @param subdomain The name of the sub-domain.
   * @return The relation, whose source is the sub-domain as a {@link MetadataObject.Type#DOMAIN}
   *     metadata object.
   * @throws IllegalArgumentException If the name is blank.
   */
  public static DomainRelation viaSubdomain(String subdomain) {
    Preconditions.checkArgument(StringUtils.isNotBlank(subdomain), "subdomain must not be blank");
    return new DomainRelation(
        Type.VIA_SUBDOMAIN, MetadataObjects.of(null, subdomain, MetadataObject.Type.DOMAIN));
  }

  /**
   * Returns the kind of the relation.
   *
   * @return The kind of the relation.
   */
  public Type type() {
    return type;
  }

  /**
   * Returns the source of the relation: the catalog or schema the backing tag is inherited from for
   * {@link Type#INHERITED}, or the sub-domain for {@link Type#VIA_SUBDOMAIN}.
   *
   * @return The source of the relation, or empty for {@link Type#DIRECT}.
   */
  public Optional<MetadataObject> source() {
    return Optional.ofNullable(source);
  }

  /**
   * Compares this relation with another object.
   *
   * @param other The object to compare.
   * @return {@code true} if the object is a relation of the same kind and source.
   */
  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof DomainRelation)) {
      return false;
    }
    DomainRelation that = (DomainRelation) other;
    return type == that.type && Objects.equals(source, that.source);
  }

  /**
   * Returns the hash code of this relation.
   *
   * @return The hash code.
   */
  @Override
  public int hashCode() {
    return Objects.hash(type, source);
  }

  /**
   * Returns a string representation of this relation.
   *
   * @return The string representation.
   */
  @Override
  public String toString() {
    return source == null ? type.name() : type.name() + " " + source.fullName();
  }
}
