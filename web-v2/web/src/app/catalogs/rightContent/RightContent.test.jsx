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

import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import { act } from 'react'
import { createRoot } from 'react-dom/client'
import RightContent from './RightContent'

const navigation = vi.hoisted(() => ({ name: '', type: 'semanticModel' }))
vi.mock('next/navigation', () => ({
  useSearchParams: () =>
    new URLSearchParams({
      metalake: 'demo',
      catalog: 'sales',
      schema: 'mart',
      catalogType: 'relational',
      [navigation.type]: navigation.name
    })
}))
vi.mock('next/dynamic', () => ({ default: () => () => null }))
vi.mock('./entitiesContent/CatalogsPage', () => ({ default: () => null }))
vi.mock('use-resize-observer', () => ({ default: () => ({ width: 1000 }) }))

let root
let container

beforeEach(() => {
  vi.stubGlobal('IS_REACT_ACT_ENVIRONMENT', true)
  container = document.createElement('div')
  document.body.appendChild(container)
  root = createRoot(container)
})

afterEach(async () => {
  await act(async () => root.unmount())
  container.remove()
  vi.unstubAllGlobals()
})

it.each([
  ['semanticModel', 'sales%20model'],
  ['semanticModel', 'margin%'],
  ['function', 'sales%20model'],
  ['function', 'margin%']
])('renders the decoded %s breadcrumb %s without decoding it again', async (type, name) => {
  Object.assign(navigation, { type, name })
  await act(async () => root.render(<RightContent />))
  expect(container.querySelector('.ant-breadcrumb li:last-child [title]').textContent).toBe(name)
})
