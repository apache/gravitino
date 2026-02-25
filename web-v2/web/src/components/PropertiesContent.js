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

import React from 'react'
import { Space } from 'antd'
import { sanitizeText } from '@/lib/utils/index'

const PropertiesContent = ({ properties, dataReferPrefix, contentDataRefer }) => {
  const entries = properties ? Object.entries(properties) : []

  const keyProps = key => (dataReferPrefix ? { 'data-refer': `${dataReferPrefix}-key-${key}` } : {})
  const valueProps = key => (dataReferPrefix ? { 'data-refer': `${dataReferPrefix}-value-${key}` } : {})
  const contentProps = contentDataRefer ? { 'data-refer': contentDataRefer } : {}

  return (
    <Space.Compact className='max-h-80 overflow-auto' {...contentProps}>
      <Space.Compact direction='vertical' className='divide-y border-gray-100'>
        <span className='min-w-24 bg-gray-100 p-1'>Key</span>
        {entries.map(([key]) => (
          <span className='p-1 block min-w-24 max-w-60 truncate' title={key} key={`prop-key-${key}`} {...keyProps(key)}>
            {key}
          </span>
        ))}
      </Space.Compact>
      <Space.Compact direction='vertical' className='divide-y border-gray-100'>
        <span className='min-w-24 bg-gray-100 p-1'>Value</span>
        {entries.map(([key, value]) => {
          const safeValue = sanitizeText(value) || '-'

          return (
            <span
              className='p-1 block min-w-24 max-w-60 truncate'
              title={safeValue}
              key={`prop-val-${key}`}
              {...valueProps(key)}
            >
              {safeValue}
            </span>
          )
        })}
      </Space.Compact>
    </Space.Compact>
  )
}

export default PropertiesContent
