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

import * as yup from 'yup'

export const DEFAULT_LOGIN_DEFAULT_VALUES = {
  grant_type: 'client_credentials',
  client_id: '',
  client_secret: '',
  scope: '',
  username: ''
}

export function createDefaultLoginSchema({ isSimpleAuth }) {
  if (isSimpleAuth) {
    return yup.object().shape({
      username: yup.string().trim().required('Username is required'),
      grant_type: yup.string().notRequired(),
      client_id: yup.string().notRequired(),
      client_secret: yup.string().notRequired(),
      scope: yup.string().notRequired()
    })
  }

  return yup.object().shape({
    grant_type: yup.string().required(),
    client_id: yup.string().required(),
    client_secret: yup.string().required(),
    scope: yup.string().required(),
    username: yup.string().notRequired()
  })
}
