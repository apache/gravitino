/*·Copyright·2023·Datastrato.·This·software·is·licensed·under·the·Apache·License·version·2.·*/

package com.datastrato.graviton.dto.requests;

import com.datastrato.graviton.Catalog;
import com.datastrato.graviton.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@EqualsAndHashCode
@ToString
public class CatalogCreateRequest implements RESTRequest {

  @JsonProperty("name")
  private String name;

  @JsonProperty("type")
  private Catalog.Type type;

  @Nullable
  @JsonProperty("comment")
  private String comment;

  @Nullable
  @JsonProperty("properties")
  private Map properties;

  public CatalogCreateRequest() {
    this(null, null, null, null);
  }

  public CatalogCreateRequest(String name, Catalog.Type type, String comment, Map properties) {
    this.name = name;
    this.type = type;
    this.comment = comment;
    this.properties = properties;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        name != null && !name.isEmpty(), "\"name\" field is required and cannot be empty");
    Preconditions.checkArgument(type != null, "\"type\" field is required and cannot be empty");
  }
}
