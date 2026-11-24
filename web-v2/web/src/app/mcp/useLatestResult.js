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

import { useEffect, useState } from 'react'
import toast from 'react-hot-toast'
import { gatewayErrorMessage } from '@/lib/api/mcpGateway'

/**
 * What the latest call of `load` returned, as `{ loading, result }`.
 *
 * `key` names what `load` loads, such as a tab and a page. While the same key
 * reloads, its last result stays; a new key, or a failed call, has none, so a
 * page never shows one tab's rows, or acts on them, under another's. Answers to
 * earlier calls are dropped, so a slow one can't win.
 */
export default function useLatestResult(load, key) {
  const [answer, setAnswer] = useState({ load: undefined, key: undefined, result: undefined })

  useEffect(() => {
    let isStale = false
    load()
      .then(result => {
        if (!isStale) setAnswer({ load, key, result })
      })
      .catch(error => {
        if (isStale) return
        setAnswer({ load, key, result: undefined })
        toast.error(gatewayErrorMessage(error))
      })

    return () => {
      isStale = true
    }
  }, [load])

  return { loading: answer.load !== load, result: answer.key === key ? answer.result : undefined }
}
