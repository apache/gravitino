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

import { act, createElement, useEffect } from 'react'
import { createRoot } from 'react-dom/client'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import useLatestResult from './useLatestResult'

vi.mock('react-hot-toast', () => ({ default: { error: vi.fn() } }))

const deferred = () => {
  const handlers = {}
  const promise = new Promise((resolve, reject) => Object.assign(handlers, { resolve, reject }))

  return { promise, ...handlers }
}

let root

// What the hook returned at the latest render.
const latest = { current: undefined }

const Probe = ({ load, view }) => {
  const state = useLatestResult(load, view)
  useEffect(() => {
    latest.current = state
  })

  return null
}

const show = (load, view) =>
  act(async () => {
    root.render(createElement(Probe, { load, view }))
  })

beforeEach(() => {
  globalThis.IS_REACT_ACT_ENVIRONMENT = true
  root = createRoot(document.createElement('div'))
})

afterEach(() => act(() => root.unmount()))

describe('useLatestResult', () => {
  it('is the result of the latest load', async () => {
    await show(async () => ['row'], 'pending')

    expect(latest.current).toEqual({ loading: false, result: ['row'] })
  })

  it('keeps the result while the same view reloads', async () => {
    await show(async () => ['old'], 'servers')
    const reload = deferred()
    await show(() => reload.promise, 'servers')

    expect(latest.current).toEqual({ loading: true, result: ['old'] })

    await act(async () => reload.resolve(['new']))
    expect(latest.current).toEqual({ loading: false, result: ['new'] })
  })

  it("never shows one view's rows under another", async () => {
    await show(async () => ['a pending request'], 'pending')
    const granted = deferred()
    await show(() => granted.promise, 'granted')

    expect(latest.current).toEqual({ loading: true, result: undefined })

    await act(async () => granted.reject(new Error('down')))
    expect(latest.current).toEqual({ loading: false, result: undefined })
  })

  it('has no result after the same view fails to reload', async () => {
    await show(async () => ['old'], 'servers')
    await show(async () => {
      throw new Error('down')
    }, 'servers')

    expect(latest.current).toEqual({ loading: false, result: undefined })
  })

  it("drops an earlier load's late answer", async () => {
    const slow = deferred()
    await show(() => slow.promise, 'page 1')
    await show(async () => ['page 2'], 'page 2')

    await act(async () => slow.resolve(['page 1']))
    expect(latest.current).toEqual({ loading: false, result: ['page 2'] })
  })
})
