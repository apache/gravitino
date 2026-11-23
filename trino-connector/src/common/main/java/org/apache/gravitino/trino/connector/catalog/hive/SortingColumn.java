/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.trino.connector.catalog.hive;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_LAST;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.gravitino.trino.connector.catalog.hive.SortingColumn.Order.ASCENDING;
import static org.apache.gravitino.trino.connector.catalog.hive.SortingColumn.Order.DESCENDING;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import io.trino.spi.connector.SortOrder;
import java.util.Objects;

// This class is referred from Trino:
// plugin/trino-gravitino/src/main/java/org/apache/gravitino/trino/connector/catalog/hive/SortingColumn.java
/**
 * Represents a column with sorting information. This class is used to define how a column should be
 * sorted in Hive tables.
 */
@Immutable
public class SortingColumn {
  /** Represents the sorting order of a column. */
  public enum Order {
    /** Ascending order with nulls first. */
    ASCENDING(ASC_NULLS_FIRST, 1),
    /** Descending order with nulls last. */
    DESCENDING(DESC_NULLS_LAST, 0);

    private final SortOrder sortOrder;
    private final int hiveOrder;

    /**
     * Constructs a new Order.
     *
     * @param sortOrder the Trino sort order
     * @param hiveOrder the Hive sort order value
     */
    Order(SortOrder sortOrder, int hiveOrder) {
      this.sortOrder = requireNonNull(sortOrder, "sortOrder is null");
      this.hiveOrder = hiveOrder;
    }

    /**
     * Gets the Trino sort order.
     *
     * @return the sort order
     */
    public SortOrder getSortOrder() {
      return sortOrder;
    }

    /**
     * Gets the Hive sort order value.
     *
     * @return the Hive order value
     */
    public int getHiveOrder() {
      return hiveOrder;
    }
  }

  private final String columnName;
  private final Order order;

  /**
   * Constructs a new SortingColumn.
   *
   * @param columnName the name of the column
   * @param order the sorting order
   */
  @JsonCreator
  public SortingColumn(
      @JsonProperty("columnName") String columnName, @JsonProperty("order") Order order) {
    this.columnName = requireNonNull(columnName, "columnName is null");
    this.order = requireNonNull(order, "order is null");
  }

  /**
   * Gets the name of the column.
   *
   * @return the column name
   */
  @JsonProperty
  public String getColumnName() {
    return columnName;
  }

  /**
   * Gets the sorting order.
   *
   * @return the order
   */
  @JsonProperty
  public Order getOrder() {
    return order;
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("columnName", columnName).add("order", order).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SortingColumn)) {
      return false;
    }

    SortingColumn that = (SortingColumn) o;
    return Objects.equals(columnName, that.columnName) && order == that.order;
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnName, order);
  }

  /**
   * Converts a SortingColumn to its string representation.
   *
   * @param column the sorting column
   * @return the string representation
   */
  public static String sortingColumnToString(SortingColumn column) {
    return column.getColumnName() + ((column.getOrder() == DESCENDING) ? " DESC" : "");
  }

  /**
   * Creates a SortingColumn from its string representation.
   *
   * @param name the string representation
   * @return the sorting column
   */
  public static SortingColumn sortingColumnFromString(String name) {
    SortingColumn.Order order = ASCENDING;
    String lower = name.toUpperCase(ENGLISH);
    if (lower.endsWith(" ASC")) {
      name = name.substring(0, name.length() - 4).trim();
    } else if (lower.endsWith(" DESC")) {
      name = name.substring(0, name.length() - 5).trim();
      order = DESCENDING;
    }
    return new SortingColumn(name, order);
  }
}
