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

import { useState } from 'react'
import React from 'react'

import { useServerInsertedHTML } from 'next/navigation'

import {
  StyleProvider,
  createCache,
  extractStyle,
  legacyLogicalPropertiesTransformer,
  px2remTransformer
} from '@ant-design/cssinjs'
import dayjs from 'dayjs'
import 'dayjs/locale/en'

dayjs.locale('en')

// suppress useLayoutEffect warnings when running outside a browser
if (!process.browser) React.useLayoutEffect = React.useEffect

export function AntdProvider({ children }) {
  const [cache] = useState(() => createCache())

  const render = <>{children}</>

  useServerInsertedHTML(() => {
    return (
      <script
        dangerouslySetInnerHTML={{
          __html: `</script>${extractStyle(cache)}<script>`
        }}
      />
    )
  })

  if (typeof window !== 'undefined') {
    return render
  }

  const px2rem = px2remTransformer({
    rootValue: 16 //@default 16
  })

  return (
    <StyleProvider layer transformers={[legacyLogicalPropertiesTransformer, px2rem]} cache={cache}>
      {render}
    </StyleProvider>
  )
}
