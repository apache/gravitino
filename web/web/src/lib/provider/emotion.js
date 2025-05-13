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

'use client'

import { CacheProvider } from '@emotion/react'
import createCache from '@emotion/cache'
import { useServerInsertedHTML } from 'next/navigation'
import { useState } from 'react'

export default function EmotionProvider({ children }) {
  const [{ cache, flush }] = useState(() => {
    const cache = createCache({ key: 'css', prepend: true })
    cache.compat = true
    const prevInsert = cache.insert
    let inserted = []
    cache.insert = (...args) => {
      const serialized = args[1]
      if (cache.inserted[serialized.name] === undefined) {
        inserted.push({ name: serialized.name, global: !args[0] })
      }

      return prevInsert(...args)
    }

    const flush = () => {
      const prevInserted = inserted
      inserted = []

      return prevInserted
    }

    return { cache, flush }
  })

  useServerInsertedHTML(() => {
    const names = flush()
    if (names.length === 0) return null

    const nonGlobalNames = []
    const globalStyles = []
    let styles = ''
    for (const { name, global } of names) {
      if (global) {
        globalStyles.push({ name, css: cache.inserted[name] })
      } else {
        nonGlobalNames.push(name)
        styles += cache.inserted[name]
      }
    }

    return [
      ...globalStyles.map((style, i) => (
        <style
          key={style.name}
          data-emotion={`${cache.key}-global`}
          dangerouslySetInnerHTML={{
            __html: style.css
          }}
        />
      )),
      <style
        key='css'
        data-emotion={`${cache.key} ${nonGlobalNames.join(' ')}`}
        dangerouslySetInnerHTML={{
          __html: styles
        }}
      />
    ]
  })

  return <CacheProvider value={cache}>{children}</CacheProvider>
}
