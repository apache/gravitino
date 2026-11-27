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

import { createContext, useCallback, useContext, useEffect, useRef, useState } from 'react'
import { getMetadataObjectTagsApi } from '@/lib/api/metalakes'

const ListsContext = createContext(null)
const MAX_BATCH_SIZE = 100
const loaders = { tags: getMetadataObjectTagsApi }

export default function MetadataObjectListsProvider({ metalake, children }) {
  const entries = useRef(new Map())
  const timer = useRef(null)
  const active = useRef(false)
  const [results, setResults] = useState({})

  const schedule = useCallback(() => {
    if (timer.current !== null) return

    // Batch cells mounted by the same render; each kind loads independently.
    timer.current = setTimeout(() => {
      timer.current = null
      if (!active.current) return
      Object.entries(loaders).forEach(async ([kind, load]) => {
        const pending = [...entries.current.entries()].filter(([, entry]) => entry.kind === kind && entry.pending)
        if (!pending.length) return
        pending.forEach(([, entry]) => {
          entry.pending = false
        })
        const batches = []
        for (let i = 0; i < pending.length; i += MAX_BATCH_SIZE) {
          batches.push(pending.slice(i, i + MAX_BATCH_SIZE))
        }

        const loadBatch = async batch => {
          try {
            const response = await load(
              metalake,
              batch.map(([, entry]) => entry.object)
            )

            const values = new Map(
              response.results.map(item => [
                `${kind}/${item.metadataObject.type.toUpperCase()}/${item.metadataObject.fullName}`,
                item[kind]
              ])
            )

            return batch.map(([key, entry]) => [
              key,
              entry,
              values.get(key) ? { data: values.get(key) } : { error: true }
            ])
          } catch (error) {
            // Object failures are returned in the response; only adapt the request size here.
            if (batch.length > 1 && error?.response?.status === 400) {
              const middle = Math.ceil(batch.length / 2)
              const results = await Promise.all([loadBatch(batch.slice(0, middle)), loadBatch(batch.slice(middle))])

              return results.flat()
            }

            return batch.map(([key, entry]) => [key, entry, { error: true }])
          }
        }
        const loaded = await Promise.all(batches.map(loadBatch))
        if (!active.current) return
        setResults(current => {
          const next = { ...current }
          loaded.flat().forEach(([key, entry, result]) => {
            if (entries.current.get(key) === entry) next[key] = result
          })

          return next
        })
      })
    }, 0)
  }, [metalake])

  const register = useCallback(
    (key, kind, object) => {
      let entry = entries.current.get(key)
      if (!entry) {
        entry = { kind, object, users: 0, pending: true }
        entries.current.set(key, entry)
        setResults(current => {
          const next = { ...current }
          delete next[key]

          return next
        })
      }
      entry.users++
      schedule()

      return () => {
        const current = entries.current.get(key)
        if (current && --current.users === 0) {
          entries.current.delete(key)
          setResults(current => {
            const next = { ...current }
            delete next[key]

            return next
          })
        }
      }
    },
    [schedule]
  )

  const refresh = useCallback(
    kind => {
      const keys = []
      entries.current.forEach((entry, key) => {
        if (!kind || entry.kind === kind) {
          entries.current.set(key, { ...entry, pending: true })
          keys.push(key)
        }
      })
      setResults(current => {
        const next = { ...current }
        keys.forEach(key => {
          delete next[key]
        })

        return next
      })
      schedule()
    },
    [schedule]
  )

  useEffect(() => {
    active.current = true

    return () => {
      active.current = false
      clearTimeout(timer.current)
      timer.current = null
    }
  }, [])

  return (
    <ListsContext.Provider value={metalake ? { register, refresh, results } : null}>{children}</ListsContext.Provider>
  )
}

export function useMetadataObjectList(kind, type, fullName) {
  const context = useContext(ListsContext)
  const register = context?.register
  const normalizedType = type?.toUpperCase()
  const key = `${kind}/${normalizedType}/${fullName}`
  const enabled = Boolean(register && normalizedType && fullName)
  useEffect(() => {
    if (enabled) return register(key, kind, { type: normalizedType, fullName })
  }, [register, enabled, key, kind, normalizedType, fullName])

  return enabled
    ? {
        ...context.results[key],
        refresh: () => context.refresh(),
        retry: () => context.refresh(kind)
      }
    : null
}
