package com.datastrato.graviton.dto.requests;

import com.datastrato.graviton.dto.RESTRequest;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public class LakehouseUpdatesRequest implements RESTRequest {

  @JsonProperty("requests")
  private final List<LakehouseUpdateRequest> requests;

  public LakehouseUpdatesRequest(List<LakehouseUpdateRequest> requests) {
    this.requests = requests;
  }

  public LakehouseUpdatesRequest() {
    this(null);
  }

  @Override
  public void validate() throws IllegalArgumentException {
    requests.forEach(RESTRequest::validate);
  }
}
