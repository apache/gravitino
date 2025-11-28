/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.job;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a job template for executing HTTP requests. This class extends the JobTemplate class
 * and provides functionality specific to HTTP job templates.
 *
 * <p>For HTTP jobs, the executable is the HTTP method (GET, POST, PUT, DELETE, etc.) that will be
 * used to make the HTTP request. The arguments are the URL and any additional parameters that will
 * be used in the HTTP request. The environments can be used to set HTTP headers. The customFields
 * can be used to store additional information related to the HTTP request.
 */
public class HttpJobTemplate extends JobTemplate {

  private final String url;
  private final Map<String, String> headers;
  private final String body;
  private final List<String> queryParams;

  /**
   * Constructs a HttpJobTemplate with the specified builder.
   *
   * @param builder the builder containing the configuration for the HTTP job template
   */
  protected HttpJobTemplate(Builder builder) {
    super(builder);
    this.url = builder.url;
    this.headers = builder.headers;
    this.body = builder.body;
    this.queryParams = builder.queryParams;
  }

  /**
   * Returns the URL for the HTTP request.
   *
   * @return the URL
   */
  public String url() {
    return url;
  }

  /**
   * Returns the headers for the HTTP request.
   *
   * @return the headers
   */
  public Map<String, String> headers() {
    return headers;
  }

  /**
   * Returns the body for the HTTP request.
   *
   * @return the body
   */
  public String body() {
    return body;
  }

  /**
   * Returns the query parameters for the HTTP request.
   *
   * @return the query parameters
   */
  public List<String> queryParams() {
    return queryParams;
  }

  @Override
  public JobType jobType() {
    return JobType.HTTP;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof HttpJobTemplate)) return false;
    if (!super.equals(o)) return false;

    HttpJobTemplate that = (HttpJobTemplate) o;
    return Objects.equals(url, that.url)
        && Objects.equals(headers, that.headers)
        && Objects.equals(body, that.body)
        && Objects.equals(queryParams, that.queryParams);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), url, headers, body, queryParams);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("  url='").append(url).append("',\n");

    if (!headers.isEmpty()) {
      sb.append("  headers={\n");
      headers.forEach((k, v) -> sb.append("    ").append(k).append(": ").append(v).append(",\n"));
      sb.append("  },\n");
    } else {
      sb.append("  headers={},\n");
    }

    if (body != null && !body.isEmpty()) {
      sb.append("  body='").append(body).append("',\n");
    } else {
      sb.append("  body=null,\n");
    }

    if (!queryParams.isEmpty()) {
      sb.append("  queryParams=[\n");
      queryParams.forEach(q -> sb.append("    ").append(q).append(",\n"));
      sb.append("  ]\n");
    } else {
      sb.append("  queryParams=[]\n");
    }

    return "\nHttpJobTemplate{\n" + super.toString() + sb + "}\n";
  }

  /**
   * Creates a new builder for constructing instances of {@link HttpJobTemplate}.
   *
   * @return a new instance of {@link Builder}
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for creating instances of {@link HttpJobTemplate}. */
  public static class Builder extends BaseBuilder<Builder, HttpJobTemplate> {

    private String url;
    private Map<String, String> headers;
    private String body;
    private List<String> queryParams;

    private Builder() {}

    /**
     * Sets the URL for the HTTP request.
     *
     * @param url the URL for the HTTP request
     * @return the builder instance for method chaining
     */
    public Builder withUrl(String url) {
      this.url = url;
      return this;
    }

    /**
     * Sets the headers for the HTTP request.
     *
     * @param headers the headers for the HTTP request
     * @return the builder instance for method chaining
     */
    public Builder withHeaders(Map<String, String> headers) {
      this.headers = headers;
      return this;
    }

    /**
     * Sets the body for the HTTP request.
     *
     * @param body the body for the HTTP request
     * @return the builder instance for method chaining
     */
    public Builder withBody(String body) {
      this.body = body;
      return this;
    }

    /**
     * Sets the query parameters for the HTTP request.
     *
     * @param queryParams the query parameters for the HTTP request
     * @return the builder instance for method chaining
     */
    public Builder withQueryParams(List<String> queryParams) {
      this.queryParams = queryParams;
      return this;
    }

    /**
     * Build the HttpJobTemplate instance.
     *
     * @return the constructed HttpJobTemplate
     */
    @Override
    public HttpJobTemplate build() {
      validate();
      return new HttpJobTemplate(this);
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    protected void validate() {
      super.validate();

      // Validate URL is not empty
      if (url == null || url.isEmpty()) {
        throw new IllegalArgumentException("URL must not be null or empty");
      }

      this.headers = headers != null ? ImmutableMap.copyOf(headers) : ImmutableMap.of();
      this.queryParams =
          queryParams != null ? ImmutableList.copyOf(queryParams) : ImmutableList.of();
    }
  }
}
