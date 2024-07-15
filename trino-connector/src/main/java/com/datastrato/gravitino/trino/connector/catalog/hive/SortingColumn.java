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
package com.datastrato.gravitino.trino.connector.catalog.hive;

import static com.datastrato.gravitino.trino.connector.catalog.hive.SortingColumn.Order.ASCENDING;
import static com.datastrato.gravitino.trino.connector.catalog.hive.SortingColumn.Order.DESCENDING;
import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_LAST;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import io.trino.spi.connector.SortOrder;
import java.util.Objects;

// This class is referred from Trino:
// plugin/trino-gravitino/src/main/java/com/datastrato/gravitino/trino/connector/catalog/hive/SortingColumn.java
@Immutable
public class SortingColumn {
  public enum Order {
    ASCENDING(ASC_NULLS_FIRST, 1),
    DESCENDING(DESC_NULLS_LAST, 0);

    private final SortOrder sortOrder;
    private final int hiveOrder;

    Order(SortOrder sortOrder, int hiveOrder) {
      this.sortOrder = requireNonNull(sortOrder, "sortOrder is null");
      this.hiveOrder = hiveOrder;
    }

    public SortOrder getSortOrder() {
      return sortOrder;
    }

    public int getHiveOrder() {
      return hiveOrder;
    }
  }

  private final String columnName;
  private final Order order;

  @JsonCreator
  public SortingColumn(
      @JsonProperty("columnName") String columnName, @JsonProperty("order") Order order) {
    this.columnName = requireNonNull(columnName, "columnName is null");
    this.order = requireNonNull(order, "order is null");
  }

  @JsonProperty
  public String getColumnName() {
    return columnName;
  }

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

  public static String sortingColumnToString(SortingColumn column) {
    return column.getColumnName() + ((column.getOrder() == DESCENDING) ? " DESC" : "");
  }

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
