package org.apache.gravitino.dto.responses;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

/** Response for the actual file listFiles. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class ListFilesResponse extends BaseResponse {
  @JsonProperty("fileList")
  private final String fileList;

  /** Constructor for ListFilesResponse. */
  public ListFilesResponse() {
    super(0);
    this.fileList = null;
  }

  /**
   * Constructor for ListFilesResponse.
   *
   * @param fileList the actual file location.
   */
  public ListFilesResponse(String fileList) {
    super(0);
    this.fileList = fileList;
  }

  /**
   * Validates the response.
   *
   * @throws IllegalArgumentException if the response is invalid.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();
    Preconditions.checkArgument(StringUtils.isNotBlank(fileList), "fileLocation must not be null");
  }
}
