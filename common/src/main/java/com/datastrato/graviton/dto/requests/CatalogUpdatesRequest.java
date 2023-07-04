package com.datastrato.graviton.dto.requests;

import com.datastrato.graviton.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@EqualsAndHashCode
@ToString
public class CatalogUpdatesRequest implements RESTRequest {

  @JsonProperty("requests")
  private final List<CatalogUpdateRequest> requests;

  public CatalogUpdatesRequest(List<CatalogUpdateRequest> requests) {
    this.requests = requests;
  }

  public CatalogUpdatesRequest() {
    this(null);
  }

  @Override
  public void validate() throws IllegalArgumentException {
    requests.forEach(RESTRequest::validate);
  }
}
