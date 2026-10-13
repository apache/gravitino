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

import { Collapse, Typography } from 'antd'

export default function SemanticModelExtensions({ extensions, label }) {
  if (!extensions?.length) return null

  return (
    <section aria-label={label} className='mb-3'>
      <Typography.Title level={5}>{label}</Typography.Title>
      <Collapse
        size='small'
        items={extensions.map((extension, index) => ({
          key: String(index),
          label: extension.vendorName,
          children: (
            <pre className='m-0 max-h-64 overflow-auto whitespace-pre-wrap break-all font-mono text-xs'>
              {extension.data}
            </pre>
          )
        }))}
      />
    </section>
  )
}
