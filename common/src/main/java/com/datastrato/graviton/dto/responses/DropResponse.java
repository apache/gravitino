/*·Copyright·2023·Datastrato.·This·software·is·licensed·under·the·Apache·License·version·2.·*/
package com.datastrato.graviton.dto.responses;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString
@EqualsAndHashCode(callSuper = true)
public class DropResponse extends BaseResponse {

  @JsonProperty("dropped")
  private boolean dropped;

  public DropResponse(boolean dropped) {
    super(0);
    this.dropped = dropped;
  }

  public DropResponse() {
    super();
  }

  public boolean dropped() {
    return dropped;
  }
}
