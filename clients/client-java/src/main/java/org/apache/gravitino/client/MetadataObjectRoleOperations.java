package org.apache.gravitino.client;

import java.util.Collections;
import java.util.Locale;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.SupportsRoles;
import org.apache.gravitino.dto.responses.NameListResponse;

class MetadataObjectRoleOperations implements SupportsRoles {

  private final RESTClient restClient;

  private final String tagRequestPath;

  MetadataObjectRoleOperations(
      String metalakeName, MetadataObject metadataObject, RESTClient restClient) {
    this.restClient = restClient;
    this.tagRequestPath =
        String.format(
            "api/metalakes/%s/objects/%s/%s/roles",
            metalakeName,
            metadataObject.type().name().toLowerCase(Locale.ROOT),
            metadataObject.fullName());
  }

  @Override
  public String[] listBindingRoleNames() {
    NameListResponse resp =
        restClient.get(
            tagRequestPath,
            NameListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.roleErrorHandler());
    resp.validate();

    return resp.getNames();
  }
}
