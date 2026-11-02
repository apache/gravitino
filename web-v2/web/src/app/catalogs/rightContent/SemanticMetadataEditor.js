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

import { useEffect, useRef, useState } from 'react'
import { Alert, Button, Form, Input, Modal, Select, Space, Tabs, theme } from 'antd'
import { isEqual } from 'lodash-es'
import { useAppDispatch } from '@/lib/hooks/useStore'
import { updateSemanticModel } from '@/lib/store/metalakes'
import { semanticDataTypes, validateSemanticModelDefinition } from '@/lib/utils/semanticModel'
import {
  getSemanticMetadataEntity,
  semanticMetadataFormValues,
  updateSemanticMetadata
} from '@/lib/utils/semanticMetadata'

export default function SemanticMetadataEditor(props) {
  try {
    getSemanticMetadataEntity(props.definition, props.target)
  } catch (error) {
    return (
      <Modal
        open
        title='Metadata unavailable'
        onCancel={props.onClose}
        footer={<Button onClick={props.onClose}>Close</Button>}
      >
        <Alert type='error' showIcon message={error.message} />
      </Modal>
    )
  }

  return <SemanticMetadataForm {...props} />
}

function SemanticMetadataForm({ definition, target, metalake, catalog, schema, semanticModel, onClose, onSaved }) {
  const { token } = theme.useToken()
  const dispatch = useAppDispatch()
  const [form] = Form.useForm()
  const [saving, setSaving] = useState(false)
  const [errors, setErrors] = useState([])
  const [section, setSection] = useState(target.kind === 'model' ? 'context' : target.section || 'basic')
  const [focusField, setFocusField] = useState(null)
  const [confirmDiscard, setConfirmDiscard] = useState(false)
  const errorRef = useRef(null)
  const expressionRef = useRef(null)

  useEffect(() => {
    if (!focusField) return

    // Tabs reveal their panel after the active key changes; focus once it is visible.
    const frame = requestAnimationFrame(() => {
      form.scrollToField(focusField, { block: 'nearest' })
      const input = form.getFieldInstance(focusField)
      if (input?.focus) input.focus()
      else if (focusField[0] === 'dialects') expressionRef.current?.focus()
      setFocusField(null)
    })

    return () => cancelAnimationFrame(frame)
  }, [section, focusField, form])

  useEffect(() => {
    if (errors.length) errorRef.current?.focus()
  }, [errors])

  // Keep the editing snapshot stable through list/detail refreshes while the dialog is open.
  const [original] = useState(definition)
  const entity = getSemanticMetadataEntity(original, target)
  const [initial] = useState(() => semanticMetadataFormValues(entity))
  const structuredContextInitialized = useRef(initial.contextFormat === 'structured')
  const contextFormat = Form.useWatch('contextFormat', form) || initial.contextFormat
  const hasExpression = ['field', 'metric'].includes(target.kind)
  const title = target.kind === 'model' ? 'Edit Model AI Context' : `Edit ${target.kind} ${target.name}`

  const close = () => {
    if (saving) return
    if (isEqual(initial, form.getFieldsValue(true))) onClose()
    else setConfirmDiscard(true)
  }

  const save = async () => {
    try {
      await form.validateFields()
    } catch (failure) {
      const name = failure.errorFields?.[0]?.name
      if (name) {
        setSection(
          name[0] === 'dialects'
            ? 'expressions'
            : ['contextFormat', 'contextText', 'instructions', 'synonyms', 'examples'].includes(name[0])
              ? 'context'
              : 'basic'
        )
        setFocusField(name)
      }

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
          expectedDefinition: original,
          data: { updates: [{ '@type': 'replaceDefinition', definition: updated }] }
        })
      )
      if (result.error || result.payload?.err || !result.payload) {
        setErrors([result.payload?.message || result.error?.message || 'Unable to save metadata.'])

        return
      }
      await onSaved(result.payload)
      onClose()
    } finally {
      setSaving(false)
    }
  }

  const basic = (
    <>
      {target.kind === 'field' && (
        <>
          <Form.Item name='label' label='Label'>
            <Input />
          </Form.Item>
          <Form.Item name='timeDimension' label='Time dimension'>
            <Select
              allowClear
              placeholder='Not specified'
              options={[
                { value: true, label: 'Yes' },
                { value: false, label: 'No' }
              ]}
            />
          </Form.Item>
        </>
      )}
      {['dataset', 'field', 'metric'].includes(target.kind) && (
        <Form.Item name='description' label='Description'>
          <Input.TextArea autoSize={{ minRows: 2, maxRows: 6 }} />
        </Form.Item>
      )}
      {hasExpression && (
        <>
          <Form.Item name='datatype' label='Data type'>
            <Select allowClear placeholder='Not specified' options={semanticDataTypes.map(value => ({ value }))} />
          </Form.Item>
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
            <Form.Item
              key={side}
              name={side}
              label={side === 'fromColumns' ? 'From columns' : 'To columns'}
              dependencies={[side === 'fromColumns' ? 'toColumns' : 'fromColumns']}
              rules={[
                {
                  validator: (_, values) => {
                    if (!values?.length || values.some(value => !value.trim()))
                      return Promise.reject(new Error('Select at least one non-empty column.'))
                    const other = form.getFieldValue(side === 'fromColumns' ? 'toColumns' : 'fromColumns')
                    if (values.length !== other?.length)
                      return Promise.reject(new Error('From and to columns must have matching counts.'))

                    return Promise.resolve()
                  }
                }
              ]}
            >
              <Select mode='tags' />
            </Form.Item>
          ))}
        </>
      )}
    </>
  )

  const expressions = (
    <div ref={expressionRef} tabIndex={-1}>
      <Form.List
        name='dialects'
        rules={[
          {
            validator: (_, values) => {
              if (!values?.length) return Promise.reject(new Error('Add at least one dialect expression.'))
              const names = values.map(value => value?.dialect).filter(Boolean)
              if (new Set(names).size !== names.length)
                return Promise.reject(new Error('Dialect names must be unique.'))

              return Promise.resolve()
            }
          }
        ]}
      >
        {(fields, { add, remove }, { errors: listErrors }) => (
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
            <Form.ErrorList errors={listErrors} />
            <Button className='mb-3' onClick={() => add({ dialect: '', expression: '' })}>
              Add dialect
            </Button>
          </>
        )}
      </Form.List>
    </div>
  )

  const context = (
    <>
      <Form.Item name='contextFormat' label='Guidance format'>
        <Select
          options={[
            { value: 'text', label: 'Text', disabled: !!entity.aiContext && typeof entity.aiContext === 'object' },
            { value: 'structured', label: 'Structured' }
          ]}
          onChange={value => {
            if (value === 'structured' && !structuredContextInitialized.current) {
              form.setFieldValue('instructions', form.getFieldValue('contextText'))
              structuredContextInitialized.current = true
            }
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
    </>
  )

  return (
    <>
      <Modal
        open
        title={title}
        style={{ top: 24 }}
        width={760}
        styles={{ body: { maxHeight: 'calc(100vh - 160px)', overflowY: 'auto' } }}
        onCancel={close}
        onOk={save}
        okText='Save'
        okButtonProps={{ 'aria-label': 'Save' }}
        confirmLoading={saving}
        cancelButtonProps={{ disabled: saving }}
        closable={!saving}
        maskClosable={!saving}
        keyboard={!saving}
      >
        {errors.length > 0 && (
          <div ref={errorRef} tabIndex={-1} className='mb-3'>
            <Alert
              type='error'
              showIcon
              message='Could not save metadata'
              description={
                <>
                  <ul className='m-0 pl-5'>
                    {errors.map(error => (
                      <li key={error}>{error}</li>
                    ))}
                  </ul>
                  <p className='mb-0'>Your edits are retained.</p>
                </>
              }
            />
          </div>
        )}
        <Form
          form={form}
          initialValues={initial}
          layout='vertical'
          disabled={saving}
          name='semanticMetadata'
          onValuesChange={changed => {
            // Form.List does not revalidate its own rules when a nested field changes.
            if (changed.dialects) form.validateFields([['dialects']]).catch(() => {})
          }}
        >
          <Tabs
            tabBarStyle={{ position: 'sticky', top: 0, zIndex: 1, background: token.colorBgContainer }}
            activeKey={section}
            onChange={setSection}
            items={[
              ...(target.kind !== 'model' ? [{ key: 'basic', label: 'Basic information', children: basic }] : []),
              ...(hasExpression ? [{ key: 'expressions', label: 'Expressions', children: expressions }] : []),
              { key: 'context', label: 'AI Context', children: context }
            ].map(item => ({
              ...item,
              forceRender: true,
              children: <div className='pr-2'>{item.children}</div>
            }))}
          />
        </Form>
      </Modal>
      <Modal
        open={confirmDiscard}
        title='Discard unsaved changes?'
        okText='Discard changes'
        cancelText='Keep editing'
        okButtonProps={{ danger: true }}
        onOk={onClose}
        onCancel={() => setConfirmDiscard(false)}
      >
        Your metadata edits have not been saved.
      </Modal>
    </>
  )
}
