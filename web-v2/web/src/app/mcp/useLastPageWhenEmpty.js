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

import { useEffect } from 'react'

/**
 * Goes back to the page that is now the last, when the rows of the last page
 * are gone, as when its last request is approved or its last grant revoked.
 * `rowCount` is how many rows the page has, and `total` how many there are.
 */
export default function useLastPageWhenEmpty(loading, rowCount, total, page, setPage) {
  useEffect(() => {
    const lastPage = Math.max(1, Math.ceil(total / page.pageSize))
    if (!loading && rowCount === 0 && page.current > lastPage) {
      setPage(current => ({ ...current, current: lastPage }))
    }
  }, [loading, rowCount, total, page, setPage])
}
