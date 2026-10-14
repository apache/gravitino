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
import { Alert, Button, Form, Input, Modal, Select, Space, Typography } from 'antd'
import { isEqual } from 'lodash-es'
import { useAppDispatch } from '@/lib/hooks/useStore'
import { updateSemanticModel } from '@/lib/store/metalakes'
import { validateSemanticModelDefinition } from '@/lib/utils/semanticModel'
import {
  getSemanticMetadataEntity,
  semanticMetadataFormValues,
  updateSemanticMetadata
} from '@/lib/utils/semanticMetadata'

export default function SemanticMetadataEditor({
  definition,
  target,
  metalake,
  catalog,
  schema,
  semanticModel,
  onClose,
  onSaved
}) {
  const dispatch = useAppDispatch()
  const [form] = Form.useForm()
  const [saving, setSaving] = useState(false)
  const [errors, setErrors] = useState([])

  // Keep the editing snapshot stable through list/detail refreshes while the dialog is open.
  const [original] = useState(definition)
  const entity = getSemanticMetadataEntity(original, target)
  const [initial] = useState(() => semanticMetadataFormValues(entity))
  const contextFormat = Form.useWatch('contextFormat', form) || initial.contextFormat
  const hasExpression = ['field', 'metric'].includes(target.kind)
  const title = target.kind === 'model' ? 'Edit Model AI Context' : `Edit ${target.kind} ${target.name}`

  const save = async () => {
    try {
      await form.validateFields()
    } catch {
      return
    }
    const updated = updateSemanticMetadata(original, target, form.getFieldsValue(true))
    const problems = validateSemanticModelDefinition(updated)
    setErrors(problems)
    if (problems.length) return
    if (isEqual(original, updated)) {
      onClose()

      return
    }
    setSaving(true)
    try {
      const result = await dispatch(
        updateSemanticModel({
          metalake,
          catalog,
          schema,
          semanticModel,
          data: { updates: [{ '@type': 'replaceDefinition', definition: updated }] }
        })
      )
      if (result.error || result.payload?.err || !result.payload) {
        setErrors(['Unable to save metadata. Your edits are retained; please try again.'])

        return
      }
      await onSaved(result.payload)
      onClose()
    } finally {
      setSaving(false)
    }
  }

  return (
    <Modal
      open
      title={title}
      width={760}
      onCancel={onClose}
      onOk={save}
      okText='Save'
      confirmLoading={saving}
      cancelButtonProps={{ disabled: saving }}
      closable={!saving}
      maskClosable={!saving}
      keyboard={!saving}
    >
      <div className='overflow-auto pr-2' style={{ maxHeight: '65vh' }}>
        {errors.length > 0 && (
          <Alert
            className='mb-3'
            type='error'
            showIcon
            message='Could not save metadata'
            description={
              <ul className='m-0 pl-5'>
                {errors.map(error => (
                  <li key={error}>{error}</li>
                ))}
              </ul>
            }
          />
        )}
        <Form form={form} initialValues={initial} layout='vertical' disabled={saving} name='semanticMetadata'>
          {target.kind === 'field' && (
            <Form.Item name='label' label='Label'>
              <Input />
            </Form.Item>
          )}
          {['dataset', 'field', 'metric'].includes(target.kind) && (
            <Form.Item name='description' label='Description'>
              <Input.TextArea autoSize={{ minRows: 2, maxRows: 6 }} />
            </Form.Item>
          )}
          {hasExpression && (
            <>
              <Form.Item name='datatype' label='Data type'>
                <Input />
              </Form.Item>
              <Typography.Title level={5}>Dialect expressions</Typography.Title>
              <Form.List name='dialects'>
                {(fields, { add, remove }) => (
                  <>
                    {fields.map(({ key, name, ...rest }) => (
                      <div key={key} className='mb-3 rounded border border-borderColor p-3'>
                        <Form.Item
                          {...rest}
                          name={[name, 'dialect']}
                          label='Dialect'
                          rules={[{ required: true, whitespace: true }]}
                        >
                          <Input />
                        </Form.Item>
                        <Form.Item
                          {...rest}
                          name={[name, 'expression']}
                          label='SQL expression'
                          rules={[{ required: true, whitespace: true }]}
                        >
                          <Input.TextArea className='font-mono' autoSize={{ minRows: 3, maxRows: 8 }} />
                        </Form.Item>
                        <Button onClick={() => remove(name)} aria-label={`Remove dialect ${name + 1}`}>
                          Remove dialect
                        </Button>
                      </div>
                    ))}
                    <Button className='mb-3' onClick={() => add({ dialect: '', expression: '' })}>
                      Add dialect
                    </Button>
                  </>
                )}
              </Form.List>
            </>
          )}
          {target.kind === 'relationship' && (
            <>
              <Space align='start' className='w-full'>
                {['from', 'to'].map(side => (
                  <Form.Item
                    key={side}
                    name={side}
                    label={side === 'from' ? 'From dataset' : 'To dataset'}
                    rules={[{ required: true }]}
                  >
                    <Select
                      className='min-w-48'
                      options={original.datasets.map(dataset => ({ value: dataset.name, label: dataset.name }))}
                    />
                  </Form.Item>
                ))}
              </Space>
              {['fromColumns', 'toColumns'].map(side => (
                <Form.Item key={side} name={side} label={side === 'fromColumns' ? 'From columns' : 'To columns'}>
                  <Select mode='tags' />
                </Form.Item>
              ))}
            </>
          )}
          <Typography.Title level={5}>AI Context</Typography.Title>
          <Form.Item name='contextFormat' label='Guidance format'>
            <Select
              options={[
                { value: 'text', label: 'Text', disabled: !!entity.aiContext && typeof entity.aiContext === 'object' },
                { value: 'structured', label: 'Structured' }
              ]}
              onChange={value => {
                if (value === 'structured' && initial.contextFormat === 'text')
                  form.setFieldValue('instructions', form.getFieldValue('contextText'))
              }}
            />
          </Form.Item>
          {contextFormat === 'text' ? (
            <Form.Item name='contextText' label='Guidance'>
              <Input.TextArea autoSize={{ minRows: 3, maxRows: 8 }} />
            </Form.Item>
          ) : (
            <>
              <Form.Item name='instructions' label='Instructions'>
                <Input.TextArea autoSize={{ minRows: 3, maxRows: 8 }} />
              </Form.Item>
              <Form.Item name='synonyms' label='Synonyms'>
                <Select mode='tags' />
              </Form.Item>
              <Form.List name='examples'>
                {(fields, { add, remove }) => (
                  <>
                    {fields.map(({ key, name, ...rest }) => (
                      <div key={key} className='mb-3'>
                        <Form.Item {...rest} name={name} label={`Example ${name + 1}`}>
                          <Input.TextArea autoSize={{ minRows: 2, maxRows: 5 }} />
                        </Form.Item>
                        <Button onClick={() => remove(name)} aria-label={`Remove example ${name + 1}`}>
                          Remove example
                        </Button>
                      </div>
                    ))}
                    <Button onClick={() => add('')}>Add example</Button>
                  </>
                )}
              </Form.List>
            </>
          )}
        </Form>
      </div>
    </Modal>
  )
}
