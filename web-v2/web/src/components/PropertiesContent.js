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
import { sanitizeText } from '@/lib/utils/index'

const PropertiesContent = ({ properties, dataReferPrefix, contentDataRefer }) => {
  const entries = properties ? Object.entries(properties) : []

  const keyProps = key => (dataReferPrefix ? { 'data-refer': `${dataReferPrefix}-key-${key}` } : {})
  const valueProps = key => (dataReferPrefix ? { 'data-refer': `${dataReferPrefix}-value-${key}` } : {})
  const contentProps = contentDataRefer ? { 'data-refer': contentDataRefer } : {}

  return (
    <div className='max-h-80 overflow-auto' {...contentProps}>
      <div className='min-w-[28rem]'>
        <div className='sticky top-0 z-10 grid grid-cols-2 bg-gray-100'>
          <span className='p-1 font-medium'>Key</span>
          <span className='p-1 font-medium'>Value</span>
        </div>
        {entries.map(([key, value]) => {
          const safeValue = sanitizeText(value) || '-'

          return (
            <div key={`prop-row-${key}`} className='grid grid-cols-2 border-t border-gray-100'>
              <span
                className='block min-w-24 max-w-60 truncate p-1 leading-6'
                title={key}
                key={`prop-key-${key}`}
                {...keyProps(key)}
              >
                {key}
              </span>
              <span
                className='block min-w-24 max-w-60 truncate p-1 leading-6'
                title={safeValue}
                key={`prop-val-${key}`}
                {...valueProps(key)}
              >
                {safeValue}
              </span>
            </div>
          )
        })}
      </div>
    </div>
  )
}

export default PropertiesContent
