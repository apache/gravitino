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

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act } from 'react'
import { createRoot } from 'react-dom/client'
import { Provider } from 'react-redux'
import { configureStore } from '@reduxjs/toolkit'
import { defHttp } from '@/lib/utils/axios'
import reducer, { fetchSemanticModels } from '@/lib/store/metalakes'
import SchemaDetailsPage from './SchemaDetailsPage'

vi.mock('@/lib/utils/axios', () => ({ defHttp: { get: vi.fn() } }))
vi.mock('react-hot-toast', () => ({ default: { error: vi.fn() } }))
vi.mock('next/navigation', () => ({
  useSearchParams: () =>
    new URLSearchParams({ metalake: 'demo', catalog: 'sales', schema: 'mart', catalogType: 'relational' })
}))
vi.mock('../../page', async () => {
  const { createContext } = await import('react')

  return { TreeRefContext: createContext(null) }
})
vi.mock('@/components/CustomTags', () => ({ default: () => null }))
vi.mock('@/components/PolicyTag', () => ({ default: () => null }))
vi.mock('next/dynamic', () => ({
  default: () => props => (
    <div data-testid='owner-target'>
      {props.metalake}/{props.metadataObjectType}/{props.metadataObjectFullName}
    </div>
  )
}))

let root
let container
let store

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('IS_REACT_ACT_ENVIRONMENT', true)
  const getComputedStyle = window.getComputedStyle

  // jsdom has no pseudo-element layout; Ant Design uses this to measure scrollbars.
  vi.spyOn(window, 'getComputedStyle').mockImplementation(element => getComputedStyle(element))
  vi.stubGlobal(
    'ResizeObserver',
    class {
      observe() {}
      unobserve() {}
      disconnect() {}
    }
  )
  Object.defineProperty(window, 'matchMedia', {
    configurable: true,
    value: vi.fn(() => ({ matches: false, addListener() {}, removeListener() {} }))
  })
  defHttp.get.mockResolvedValue({
    catalog: { name: 'sales', provider: 'jdbc-mysql', properties: {} },
    owner: { name: 'tester', type: 'user' }
  })
  container = document.createElement('div')
  document.body.appendChild(container)
  root = createRoot(container)
})

afterEach(async () => {
  await act(async () => root.unmount())
  container.remove()
  vi.restoreAllMocks()
  vi.unstubAllGlobals()
})

describe('Semantic Model ownership action', () => {
  it.each([true, false])('gates Set Owner on authorization (enabled=%s)', async anthEnable => {
    store = configureStore({ reducer: { metalakes: reducer, auth: () => ({ anthEnable }) } })
    const args = { metalake: 'demo', catalog: 'sales', schema: 'mart' }
    store.dispatch(fetchSemanticModels.pending('load', args))
    store.dispatch(
      fetchSemanticModels.fulfilled(
        { semanticModels: [{ name: 'orders', namespace: ['demo', 'sales', 'mart'] }] },
        'load',
        args
      )
    )
    await act(async () =>
      root.render(
        <Provider store={store}>
          <SchemaDetailsPage />
        </Provider>
      )
    )
    await act(async () =>
      [...document.querySelectorAll('[role="tab"]')].find(tab => tab.textContent === 'Semantic Models').click()
    )
    for (const [prefix, label] of [
      ['edit', 'Edit Semantic Model'],
      ['delete', 'Delete Semantic Model']
    ]) {
      const button = document.querySelector(`[data-refer="${prefix}-semantic-model-orders"]`)
      expect(button.tagName).toBe('BUTTON')
      expect(button.getAttribute('aria-label')).toBe(label)
      button.focus()
      expect(document.activeElement).toBe(button)
    }
    const action = document.querySelector('[data-refer="set-owner-semantic-model-orders"]')
    if (anthEnable) {
      expect(action).not.toBeNull()
      await act(async () => action.click())
      expect(document.querySelector('[data-testid="owner-target"]').textContent).toBe(
        'demo/semantic_model/sales.mart.orders'
      )
    } else {
      expect(action).toBeNull()
    }
  })
})
