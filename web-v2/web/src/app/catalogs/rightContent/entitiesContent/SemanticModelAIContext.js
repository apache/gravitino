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

import { Collapse, Tag } from 'antd'

export default function SemanticModelAIContext({ context, label = 'AI Context' }) {
  if (!context || (typeof context === 'object' && !Object.keys(context).length)) return null

  const additional =
    typeof context === 'object'
      ? Object.fromEntries(
          Object.entries(context).filter(([key]) => !['instructions', 'synonyms', 'examples'].includes(key))
        )
      : {}

  return (
    <Collapse
      size='small'
      className='mb-3'
      items={[
        {
          key: 'context',
          label,
          children: (
            <div className='max-h-64 overflow-auto whitespace-pre-wrap break-words'>
              {typeof context === 'string' ? (
                <p className='m-0'>{context}</p>
              ) : (
                <dl className='m-0 space-y-3'>
                  {context.instructions && (
                    <div>
                      <dt className='font-semibold'>Instructions</dt>
                      <dd className='m-0'>{context.instructions}</dd>
                    </div>
                  )}
                  {context.synonyms?.length > 0 && (
                    <div>
                      <dt className='font-semibold'>Synonyms</dt>
                      <dd className='m-0'>
                        {context.synonyms.map((value, index) => (
                          <Tag key={index} className='mb-1 whitespace-pre-wrap'>
                            {value}
                          </Tag>
                        ))}
                      </dd>
                    </div>
                  )}
                  {context.examples?.length > 0 && (
                    <div>
                      <dt className='font-semibold'>Examples</dt>
                      <dd className='m-0'>
                        <ul className='list-disc pl-5'>
                          {context.examples.map((value, index) => (
                            <li key={index}>{value}</li>
                          ))}
                        </ul>
                      </dd>
                    </div>
                  )}
                  {Object.keys(additional).length > 0 && (
                    <div>
                      <dt className='font-semibold'>Additional context</dt>
                      <dd className='m-0'>
                        <pre className='m-0 whitespace-pre-wrap break-all font-mono text-xs'>
                          {JSON.stringify(additional, null, 2)}
                        </pre>
                      </dd>
                    </div>
                  )}
                </dl>
              )}
            </div>
          )
        }
      ]}
    />
  )
}
