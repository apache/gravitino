package com.datastrato.gravitino.catalog.hive;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * Immutable implementation of {@link CachedClientPool.Key}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code ImmutableKey.builder()}.
 */
@Generated(from = "CachedClientPool.Key", generator = "Immutables")
@SuppressWarnings({"all"})
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
final class ImmutableKey extends CachedClientPool.Key {
  private final ImmutableList<Object> elements;

  private ImmutableKey(ImmutableList<Object> elements) {
    this.elements = elements;
  }

  /**
   * @return The value of the {@code elements} attribute
   */
  @Override
  ImmutableList<Object> elements() {
    return elements;
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link CachedClientPool.Key#elements() elements}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableKey withElements(Object... elements) {
    ImmutableList<Object> newValue = ImmutableList.copyOf(elements);
    return new ImmutableKey(newValue);
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link CachedClientPool.Key#elements() elements}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of elements elements to set
   * @return A modified copy of {@code this} object
   */
  public final ImmutableKey withElements(Iterable<? extends Object> elements) {
    if (this.elements == elements) return this;
    ImmutableList<Object> newValue = ImmutableList.copyOf(elements);
    return new ImmutableKey(newValue);
  }

  /**
   * This instance is equal to all instances of {@code ImmutableKey} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof ImmutableKey
        && equalTo(0, (ImmutableKey) another);
  }

  private boolean equalTo(int synthetic, ImmutableKey another) {
    return elements.equals(another.elements);
  }

  /**
   * Computes a hash code from attributes: {@code elements}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + elements.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code Key} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("Key")
        .omitNullValues()
        .add("elements", elements)
        .toString();
  }

  /**
   * Creates an immutable copy of a {@link CachedClientPool.Key} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable Key instance
   */
  public static ImmutableKey copyOf(CachedClientPool.Key instance) {
    if (instance instanceof ImmutableKey) {
      return (ImmutableKey) instance;
    }
    return ImmutableKey.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link ImmutableKey ImmutableKey}.
   * <pre>
   * ImmutableKey.builder()
   *    .addElements|addAllElements(Object) // {@link CachedClientPool.Key#elements() elements} elements
   *    .build();
   * </pre>
   * @return A new ImmutableKey builder
   */
  public static ImmutableKey.Builder builder() {
    return new ImmutableKey.Builder();
  }

  /**
   * Builds instances of type {@link ImmutableKey ImmutableKey}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "CachedClientPool.Key", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private ImmutableList.Builder<Object> elements = ImmutableList.builder();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code Key} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(CachedClientPool.Key instance) {
      Objects.requireNonNull(instance, "instance");
      addAllElements(instance.elements());
      return this;
    }

    /**
     * Adds one element to {@link CachedClientPool.Key#elements() elements} list.
     * @param element A elements element
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addElements(Object element) {
      this.elements.add(element);
      return this;
    }

    /**
     * Adds elements to {@link CachedClientPool.Key#elements() elements} list.
     * @param elements An array of elements elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addElements(Object... elements) {
      this.elements.add(elements);
      return this;
    }


    /**
     * Sets or replaces all elements for {@link CachedClientPool.Key#elements() elements} list.
     * @param elements An iterable of elements elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder elements(Iterable<? extends Object> elements) {
      this.elements = ImmutableList.builder();
      return addAllElements(elements);
    }

    /**
     * Adds elements to {@link CachedClientPool.Key#elements() elements} list.
     * @param elements An iterable of elements elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addAllElements(Iterable<? extends Object> elements) {
      this.elements.addAll(elements);
      return this;
    }

    /**
     * Builds a new {@link ImmutableKey ImmutableKey}.
     * @return An immutable instance of Key
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public ImmutableKey build() {
      return new ImmutableKey(elements.build());
    }
  }
}
