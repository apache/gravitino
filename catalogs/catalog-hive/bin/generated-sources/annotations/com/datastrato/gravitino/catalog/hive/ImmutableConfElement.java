package com.datastrato.gravitino.catalog.hive;

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link CachedClientPool.ConfElement}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableConfElement.builder()}.
 */
@Generated(from = "CachedClientPool.ConfElement", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
final class ImmutableConfElement
    extends CachedClientPool.ConfElement {
  private final String key;
  private final @Nullable String value;

  private ImmutableConfElement(String key, @Nullable String value) {
    this.key = key;
    this.value = value;
  }

  /**
   * @return The value of the {@code key} attribute
   */
  @Override
  String key() {
    return key;
  }

  /**
   * @return The value of the {@code value} attribute
   */
  @Override
  @Nullable String value() {
    return value;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CachedClientPool.ConfElement#key() key} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for key
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableConfElement withKey(String value) {
    String newValue = Objects.requireNonNull(value, "key");
    if (this.key.equals(newValue)) return this;
    return new ImmutableConfElement(newValue, this.value);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link CachedClientPool.ConfElement#value() value} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for value (can be {@code null})
   * @return A modified copy of the {@code this} object
   */
  public final ImmutableConfElement withValue(@Nullable String value) {
    if (Objects.equals(this.value, value)) return this;
    return new ImmutableConfElement(this.key, value);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableConfElement} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableConfElement
        && equalTo(0, (ImmutableConfElement) another);
  }

  private boolean equalTo(int synthetic, ImmutableConfElement another) {
    return key.equals(another.key)
        && Objects.equals(value, another.value);
  }

  /**
   * Computes a hash code from attributes: {@code key}, {@code value}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + key.hashCode();
    h += (h << 5) + Objects.hashCode(value);
    return h;
  }

  /**
   * Prints the immutable value {@code ConfElement} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("ConfElement")
        .omitNullValues()
        .add("key", key)
        .add("value", value)
        .toString();
  }

  /**
   * Creates an immutable copy of a {@link CachedClientPool.ConfElement} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable ConfElement instance
   */
  public static ImmutableConfElement copyOf(CachedClientPool.ConfElement instance) {
    if (instance instanceof ImmutableConfElement) {
      return (ImmutableConfElement) instance;
    }
    return ImmutableConfElement.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableConfElement ImmutableConfElement}.
   * <pre>
   * ImmutableConfElement.builder()
   *    .key(String) // required {@link CachedClientPool.ConfElement#key() key}
   *    .value(String | null) // nullable {@link CachedClientPool.ConfElement#value() value}
   *    .build();
   * </pre>
   * @return A new ImmutableConfElement builder
   */
  public static ImmutableConfElement.Builder builder() {
    return new ImmutableConfElement.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableConfElement ImmutableConfElement}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "CachedClientPool.ConfElement", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_KEY = 0x1L;
    private long initBits = 0x1L;

    private @Nullable String key;
    private @Nullable String value;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code ConfElement} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(CachedClientPool.ConfElement instance) {
      Objects.requireNonNull(instance, "instance");
      key(instance.key());
      @Nullable String valueValue = instance.value();
      if (valueValue != null) {
        value(valueValue);
      }
      return this;
    }

    /**
     * Initializes the value for the {@link CachedClientPool.ConfElement#key() key} attribute.
     * @param key The value for key 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder key(String key) {
      this.key = Objects.requireNonNull(key, "key");
      initBits &= ~INIT_BIT_KEY;
      return this;
    }

    /**
     * Initializes the value for the {@link CachedClientPool.ConfElement#value() value} attribute.
     * @param value The value for value (can be {@code null})
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder value(@Nullable String value) {
      this.value = value;
      return this;
    }

    /**
     * Builds a new {@link ImmutableConfElement ImmutableConfElement}.
     * @return An immutable instance of ConfElement
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableConfElement build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new ImmutableConfElement(key, value);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_KEY) != 0) attributes.add("key");
      return "Cannot build ConfElement, some of required attributes are not set " + attributes;
    }
  }
}
