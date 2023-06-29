package com.datastrato.graviton;

import com.datastrato.graviton.client.ErrorHandler;
import com.datastrato.graviton.dto.responses.BaseResponse;
import com.datastrato.graviton.dto.responses.ErrorType;
import com.datastrato.graviton.exceptions.MetalakeAlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchMetalakeException;
import com.datastrato.graviton.exceptions.RESTException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorHandlers {

  private static final Logger LOG = LoggerFactory.getLogger(ErrorHandlers.class);

  public static Consumer<BaseResponse> metalakeErrorHandler() {
    return MetalakeErrorHandler.INSTANCE;
  }

  public static Consumer<BaseResponse> defaultErrorHandler() {
    return DefaultErrorHandler.INSTANCE;
  }

  private ErrorHandlers() {}

  private static class MetalakeErrorHandler extends DefaultErrorHandler {
    private static final ErrorHandler INSTANCE = new MetalakeErrorHandler();

    @Override
    public void accept(BaseResponse baseResponse) {
      switch (baseResponse.getCode()) {
        case 1001:
          throw new IllegalArgumentException(baseResponse.getMessage());
        case 1003:
          throw new NoSuchMetalakeException(baseResponse.getMessage());
        case 1004:
          throw new MetalakeAlreadyExistsException(baseResponse.getMessage());
      }

      super.accept(baseResponse);
    }
  }

  private static class DefaultErrorHandler extends ErrorHandler {
    private static final ErrorHandler INSTANCE = new DefaultErrorHandler();

    @Override
    public BaseResponse parseResponse(int code, String json, ObjectMapper mapper) {
      try {
        return mapper.readValue(json, BaseResponse.class);
      } catch (Exception e) {
        LOG.warn("Failed to parse response: {}", json, e);
      }

      String errorMsg = String.format("Error code: %d, message: %s", code, json);
      return BaseResponse.error(ErrorType.SYSTEM_ERROR, errorMsg);
    }

    @Override
    public void accept(BaseResponse baseResponse) {
      throw new RESTException("Unable to process: %s", baseResponse.getMessage());
    }
  }
}
