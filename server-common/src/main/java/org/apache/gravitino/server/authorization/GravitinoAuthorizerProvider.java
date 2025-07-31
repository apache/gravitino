/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.server.authorization;

import java.io.Closeable;
import java.io.IOException;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.server.ServerConfig;

/**
 * Used to initialize and store {@link GravitinoAuthorizer}. When Gravitino Server starts up, it
 * initializes GravitinoAuthorizer by reading the ServerConfig through the init method and stores it
 * in the GravitinoAuthorizerProvider. The GravitinoAuthorizer instance can then be retrieved using
 * the getGravitinoAuthorizer method.
 */
public class GravitinoAuthorizerProvider implements Closeable {

  private static final GravitinoAuthorizerProvider INSTANCE = new GravitinoAuthorizerProvider();

  private GravitinoAuthorizerProvider() {}

  private volatile GravitinoAuthorizer gravitinoAuthorizer;

  /**
   * Instantiate the {@link GravitinoAuthorizer}, and then execute the initialize method in the
   * GravitinoAuthorizer.
   *
   * @param serverConfig Gravitino server config
   */
  public void initialize(ServerConfig serverConfig) {
    if (gravitinoAuthorizer == null) {
      synchronized (this) {
        if (gravitinoAuthorizer == null) {
          boolean enableAuthorization = serverConfig.get(Configs.ENABLE_AUTHORIZATION);
          if (enableAuthorization) {
            String authorizationImpl = serverConfig.get(Configs.AUTHORIZATION_IMPL);
            try {
              gravitinoAuthorizer =
                  (GravitinoAuthorizer)
                      Class.forName(authorizationImpl).getDeclaredConstructor().newInstance();
            } catch (Exception e) {
              throw new IllegalArgumentException("Can not initialize GravitinoAuthorizer", e);
            }
          } else {
            gravitinoAuthorizer = new PassThroughAuthorizer();
          }
          gravitinoAuthorizer.initialize();
          GravitinoEnv.getInstance().setGravitinoAuthorizer(gravitinoAuthorizer);
        }
      }
    }
  }

  public static GravitinoAuthorizerProvider getInstance() {
    return INSTANCE;
  }

  /**
   * Retrieve GravitinoAuthorizer instance.
   *
   * @return GravitinoAuthorizer instance
   */
  public GravitinoAuthorizer getGravitinoAuthorizer() {
    return gravitinoAuthorizer;
  }

  @Override
  public void close() throws IOException {
    GravitinoEnv.getInstance().setGravitinoAuthorizer(null);
    if (gravitinoAuthorizer != null) {
      gravitinoAuthorizer.close();
    }
    gravitinoAuthorizer = null;
  }
}
