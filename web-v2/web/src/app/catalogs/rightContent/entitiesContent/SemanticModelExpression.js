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
import { Button, Modal, Select, Tag, theme } from 'antd'

export default function SemanticModelExpression({ expression, name }) {
  const { token } = theme.useToken()
  const [open, setOpen] = useState(false)
  const [selected, setSelected] = useState(0)
  const [all, setAll] = useState(false)
  const dialects = expression?.dialects || []
  if (!dialects.length) return <span>-</span>
  const current = dialects[selected] ? selected : 0
  const visible = all ? dialects : [dialects[current]]

  return (
    <div className='min-w-0'>
      {dialects.length > 1 ? (
        <Select
          aria-label={`Preview dialect for ${name}`}
          size='small'
          className='mb-1 min-w-28'
          value={current}
          onChange={setSelected}
          options={dialects.map((dialect, index) => ({ value: index, label: dialect.dialect }))}
        />
      ) : (
        <Tag>{dialects[0].dialect}</Tag>
      )}
      <pre className='m-0 max-h-8 overflow-hidden whitespace-pre-wrap break-words font-mono text-xs [overflow-wrap:anywhere]'>
        {dialects[current].expression}
      </pre>
      <Button
        size='small'
        type='link'
        aria-label={`View expression for ${name}`}
        onClick={() => {
          setAll(false)
          setOpen(true)
        }}
      >
        View SQL ({dialects.length} {dialects.length === 1 ? 'dialect' : 'dialects'})
      </Button>
      <Modal open={open} title={`${name} SQL expressions`} width={900} footer={null} onCancel={() => setOpen(false)}>
        <Select
          aria-label='SQL dialect'
          className='mb-3 w-full'
          value={all ? 'all' : current}
          onChange={value => {
            setAll(value === 'all')
            if (value !== 'all') setSelected(value)
          }}
          options={[
            ...dialects.map((dialect, index) => ({ value: index, label: dialect.dialect })),
            { value: 'all', label: 'All dialects' }
          ]}
        />
        <div className='space-y-3 overflow-auto' style={{ maxHeight: '60vh' }}>
          {visible.map((dialect, index) => (
            <div key={index}>
              <Tag className='mb-2'>{dialect.dialect}</Tag>
              <pre
                style={{
                  background: token.colorFillQuaternary,
                  borderColor: token.colorBorderSecondary,
                  borderStyle: 'solid'
                }}
                className='m-0 overflow-auto whitespace-pre-wrap break-words rounded border border-borderColor p-3 font-mono text-sm leading-6 [overflow-wrap:anywhere]'
              >
                {dialect.expression}
              </pre>
            </div>
          ))}
        </div>
      </Modal>
    </div>
  )
}
