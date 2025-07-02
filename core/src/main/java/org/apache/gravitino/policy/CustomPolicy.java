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
package org.apache.gravitino.policy;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Objects;
import org.apache.gravitino.exceptions.IllegalPolicyException;

public class CustomPolicy extends BasePolicy {
  public static CustomPolicyBuilder builder() {
    return new CustomPolicyBuilder();
  }

  public static CustomContentBuilder contentBuilder() {
    return new CustomContentBuilder();
  }

  @Override
  public void validate() throws IllegalPolicyException {
    super.validate();

    Preconditions.checkArgument(
        ContentType.fromString(type) == ContentType.CUSTOM,
        "Expected non-built-in type, but got %s",
        type);

    Preconditions.checkArgument(
        content instanceof CustomContent,
        "Expected CustomContent, but got %s",
        content.getClass().getName());
  }

  public static class CustomPolicyBuilder
      extends BasePolicyBuilder<CustomPolicyBuilder, CustomPolicy> {

    @Override
    protected CustomPolicy internalBuild() {
      return new CustomPolicy();
    }
  }

  public static class CustomContent extends BaseContent {
    private Map<String, Object> customFields;

    public Map<String, Object> customFields() {
      return customFields;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CustomContent)) {
        return false;
      }
      CustomContent that = (CustomContent) o;
      return super.equals(that) && Objects.equals(customFields, that.customFields);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), customFields);
    }
  }

  public static class CustomContentBuilder
      extends BaseContentBuilder<CustomContentBuilder, CustomContent> {
    private Map<String, Object> customFields;

    public CustomContentBuilder withCustomFields(Map<String, Object> customFields) {
      this.customFields = customFields;
      return this;
    }

    @Override
    protected CustomContent internalBuild() {
      CustomContent content = new CustomContent();
      content.customFields = customFields;
      return content;
    }
  }
}
