/*·Copyright·2023·Datastrato.·This·software·is·licensed·under·the·Apache·License·version·2.·*/

package com.datastrato.graviton.client;

import com.datastrato.graviton.dto.responses.ErrorResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.function.Consumer;

public abstract class ErrorHandler implements Consumer<ErrorResponse> {

  public abstract ErrorResponse parseResponse(int code, String json, ObjectMapper mapper);
}
