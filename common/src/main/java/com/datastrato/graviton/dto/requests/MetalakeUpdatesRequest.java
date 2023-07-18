/*·Copyright·2023·Datastrato.·This·software·is·licensed·under·the·Apache·License·version·2.·*/
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
public class MetalakeUpdatesRequest implements RESTRequest {

  @JsonProperty("requests")
  private final List<MetalakeUpdateRequest> requests;

  public MetalakeUpdatesRequest(List<MetalakeUpdateRequest> requests) {
    this.requests = requests;
  }

  public MetalakeUpdatesRequest() {
    this(null);
  }

  @Override
  public void validate() throws IllegalArgumentException {
    requests.forEach(RESTRequest::validate);
  }
}
