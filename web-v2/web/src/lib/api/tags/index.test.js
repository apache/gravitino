/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { afterEach, expect, it, vi } from 'vitest'
import { associateTagApi } from './index'
import { defHttp } from '@/lib/utils/axios'

vi.mock('@/lib/utils/axios', () => ({ defHttp: { post: vi.fn() } }))

afterEach(() => vi.clearAllMocks())

it('uses the v1 media types for legacy tag-name association requests', () => {
  associateTagApi({
    metalake: 'demo',
    metadataObjectType: 'table',
    metadataObjectFullName: 'sales.orders',
    apiVersion: 1,
    data: { tagsToRemove: ['pii'] }
  })

  expect(defHttp.post).toHaveBeenCalledWith({
    url: '/api/metalakes/demo/objects/table/sales.orders/tags',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/vnd.gravitino.v1+json'
    },
    data: { tagsToRemove: ['pii'] }
  })
})
