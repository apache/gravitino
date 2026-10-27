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

import { useState } from 'react'
import { Button, Typography } from 'antd'
import { formatVendorJson } from '@/lib/utils/formatVendorJson'
import SemanticMetadataSection from './SemanticMetadataSection'

function ExtensionData({ data }) {
  const [raw, setRaw] = useState(false)
  const formatted = formatVendorJson(data)

  return (
    <>
      {formatted && (
        <Button size='small' className='mb-2' onClick={() => setRaw(!raw)}>
          {raw ? 'Show formatted JSON' : 'Show raw value'}
        </Button>
      )}
      <pre className='m-0 max-h-64 overflow-auto whitespace-pre-wrap break-words font-mono text-xs'>
        {formatted && !raw ? formatted : data}
      </pre>
    </>
  )
}

export default function SemanticModelExtensions({ extensions, label }) {
  if (!extensions?.length) return null

  return (
    <SemanticMetadataSection label={label}>
      <div className='space-y-3'>
        {extensions.map((extension, index) => (
          <section key={index} aria-label={extension.vendorName}>
            <Typography.Text type='secondary' className='mb-2 block text-xs font-medium'>
              {extension.vendorName}
            </Typography.Text>
            <ExtensionData data={extension.data} />
          </section>
        ))}
      </div>
    </SemanticMetadataSection>
  )
}
