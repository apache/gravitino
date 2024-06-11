package org.apache.iceberg.rest.responses;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.rest.RESTResponse;

public class ListPartitionsResponse implements RESTResponse {

  private List<String> partitions;

  public ListPartitionsResponse() {
    // Required for Jackson deserialization
  }

  private ListPartitionsResponse(List<String> partitions) {
    this.partitions = partitions;
    validate();
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(partitions != null, "Invalid partitions list: null");
  }

  public List<String> partitions() {
    return partitions != null ? partitions : ImmutableList.of();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("partitions", partitions)
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final ImmutableList.Builder<String> partitions = ImmutableList.builder();

    private Builder() {
    }

    public ListPartitionsResponse.Builder add(String toAdd) {
      Preconditions.checkNotNull(toAdd, "Invalid table identifier: null");
      partitions.add(toAdd);
      return this;
    }

    public ListPartitionsResponse.Builder addAll(Collection<String> toAdd) {
      Preconditions.checkNotNull(toAdd, "Invalid table identifier list: null");
      Preconditions.checkArgument(!toAdd.contains(null), "Invalid table identifier: null");
      partitions.addAll(toAdd);
      return this;
    }

    public ListPartitionsResponse build() {
      return new ListPartitionsResponse(partitions.build());
    }
  }
}
