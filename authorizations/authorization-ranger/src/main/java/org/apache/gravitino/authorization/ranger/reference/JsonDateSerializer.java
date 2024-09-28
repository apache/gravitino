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
package org.apache.gravitino.authorization.ranger.reference;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;

/**
 * Used to serialize Java.util.Date, which is not a common JSON type, so we have to create a custom
 * serialize method
 */
// apache/ranger/security-admin/src/main/java/org/apache/ranger/defines/JsonDateSerializer.java
public class JsonDateSerializer extends JsonSerializer<Date> {
  private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";

  @Override
  public void serialize(Date date, JsonGenerator gen, SerializerProvider provider)
      throws IOException {
    String formattedDate = new SimpleDateFormat(DATE_FORMAT).format(date);
    gen.writeString(formattedDate);
  }
}
