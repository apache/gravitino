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

import React, { useEffect, useState, useRef } from 'react'
import { PlusOutlined } from '@ant-design/icons'
import { Button, Flex, Form, Input, Modal, Select, Switch, Typography } from 'antd'
import Icons from '@/components/Icons'
import RenderPropertiesFormItem from '@/components/EntityPropertiesFormItem'
import { validateMessages, supportedObjectTypesMap, mismatchName } from '@/config'
import { nameRegex } from '@/lib/utils/regex'
import { useResetFormOnCloseModal } from '@/lib/hooks/use-reset'
import { genUpdates } from '@/lib/utils'
import { cn } from '@/lib/utils/tailwind'
import { useAppDispatch } from '@/lib/hooks/useStore'
import { getPolicyDetails, createPolicy, updatePolicy } from '@/lib/store/policies'

const { Paragraph } = Typography
const { TextArea } = Input

const defaultValues = {
  name: '',
  comment: '',
  enabled: true,
  policyType: 'custom',
  supportedObjectTypes: [],
  rules: [],
  properties: []
}

export default function CreatePolicyDialog({ ...props }) {
  const { open, setOpen, metalake, editPolicy } = props
  const [confirmLoading, setConfirmLoading] = useState(false)
  const [cacheData, setCacheData] = useState()
  const loadedRef = useRef(false)
  const [form] = Form.useForm()
  const values = Form.useWatch([], form)
  const dispatch = useAppDispatch()

  useResetFormOnCloseModal({
    form,
    open
  })

  useEffect(() => {
    if (open && editPolicy && !loadedRef.current) {
      loadedRef.current = true

      const init = async () => {
        const { payload: policy } = await dispatch(getPolicyDetails({ metalake, policy: editPolicy }))
        setCacheData(policy)
        form.setFieldValue('name', policy.name)
        form.setFieldValue('enabled', policy.enabled)
        form.setFieldValue('comment', policy.comment)
        form.setFieldValue('policyType', policy.policyType)
        form.setFieldValue('supportedObjectTypes', policy.content?.supportedObjectTypes || [])
        let ruleIndex = 0
        Object.entries(policy.content?.customRules || {}).forEach(([key, value]) => {
          if (key !== 'color') {
            form.setFieldValue(['rules', ruleIndex, 'ruleName'], key)
            form.setFieldValue(['rules', ruleIndex, 'ruleContent'], value)
            ruleIndex++
          }
        })
        let index = 0
        Object.entries(policy.content?.properties || {}).forEach(([key, value]) => {
          if (key !== 'color') {
            form.setFieldValue(['properties', index, 'key'], key)
            form.setFieldValue(['properties', index, 'value'], value)
            index++
          }
        })
      }
      init()
    }

    // Reset loadedRef when dialog closes
    if (!open) {
      loadedRef.current = false
    }
  }, [open, editPolicy, metalake])

  const handleSubmit = e => {
    e.preventDefault()
    form
      .validateFields()
      .then(async () => {
        setConfirmLoading(true)

        const submitData = {
          name: values.name.trim(),
          comment: values.comment,
          enabled: values.enabled,
          policyType: values.policyType,
          content: {
            customRules: values.rules.reduce((acc, item) => {
              if (item.ruleName) {
                acc[item.ruleName] = item.ruleContent
              }

              return acc
            }, {}),
            supportedObjectTypes: values.supportedObjectTypes,
            properties: values.properties.reduce((acc, item) => {
              acc[item.key] = values[item.key] || item.value

              return acc
            }, {})
          }
        }
        if (editPolicy) {
          // update policy
          const reqData = { updates: genUpdates(cacheData, submitData) }
          if (reqData.updates.length) {
            await dispatch(updatePolicy({ metalake, policy: editPolicy, data: reqData }))
          }
        } else {
          await dispatch(createPolicy({ metalake, data: submitData }))
        }
        setConfirmLoading(false)
        setOpen(false)
      })
      .catch(info => {
        console.error(info)
        form.scrollToField(info?.errorFields?.[0]?.name?.[0])
      })
  }

  const handleCancel = () => {
    setOpen(false)
  }

  return (
    <>
      <Modal
        title={!editPolicy ? 'Create Policy' : 'Edit Policy'}
        open={open}
        onOk={handleSubmit}
        okText='Submit'
        maskClosable={false}
        keyboard={false}
        width={800}
        confirmLoading={confirmLoading}
        onCancel={handleCancel}
      >
        <Paragraph type='secondary'>{!editPolicy ? 'Create a new policy' : `Edit the policy ${editPolicy}.`}</Paragraph>
        <Form
          form={form}
          initialValues={defaultValues}
          layout='vertical'
          name='policyForm'
          validateMessages={validateMessages}
        >
          <Form.Item
            name='name'
            label='Policy Name'
            rules={[{ required: true }, { type: 'string', max: 64 }, { pattern: new RegExp(nameRegex) }]}
          >
            <Input placeholder={mismatchName} />
          </Form.Item>
          <Form.Item name='enabled' label='Enabled'>
            <Switch disabled={!!editPolicy} />
          </Form.Item>
          <Form.Item name='policyType' label='Policy Type'>
            <Select>
              <Select.Option value='custom'>Custom</Select.Option>
            </Select>
          </Form.Item>
          <Form.Item
            name='supportedObjectTypes'
            rules={[{ required: true, message: 'Please select supported object types!' }]}
            label='Supported Object Types'
          >
            <Select mode='multiple'>
              {Object.values(supportedObjectTypesMap).map(item => (
                <Select.Option key={item} value={item}>
                  {item}
                </Select.Option>
              ))}
            </Select>
          </Form.Item>
          <Form.Item label='Rule(s)'>
            <div className='flex flex-col gap-2'>
              <Form.List name='rules'>
                {(fields, { add, remove }) => (
                  <>
                    {fields.map(({ key, name, ...restField }) => (
                      <Form.Item label={null} className='align-items-center mb-1' key={key}>
                        <Flex gap='small' align='start'>
                          <Form.Item
                            {...restField}
                            name={[name, 'ruleName']}
                            rules={[{ required: true, message: 'Please enter the rule name!' }]}
                            className='mb-0 w-full grow'
                          >
                            <Input placeholder='Rule Name' />
                          </Form.Item>
                          <Form.Item
                            {...restField}
                            name={[name, 'ruleContent']}
                            rules={[{ required: true, message: 'Please enter the rule content!' }]}
                            className='mb-0 w-full grow'
                          >
                            <Input placeholder='Rule Content' />
                          </Form.Item>
                          <Form.Item className='mb-0 grow-0'>
                            <Icons.Minus
                              className='size-8 cursor-pointer text-gray-400 hover:text-defaultPrimary'
                              onClick={() => remove(name)}
                            />
                          </Form.Item>
                        </Flex>
                      </Form.Item>
                    ))}
                    <Button className='w-fit' type='dashed' onClick={() => add()} icon={<PlusOutlined />}>
                      Add Rule
                    </Button>
                  </>
                )}
              </Form.List>
            </div>
          </Form.Item>
          <Form.Item name='comment' label='Comment'>
            <TextArea />
          </Form.Item>
          <Form.Item label='Properties'>
            <Form.List name='properties'>
              {(fields, subOpt) => (
                <RenderPropertiesFormItem fields={fields} subOpt={subOpt} form={form} isEdit={!!editPolicy} />
              )}
            </Form.List>
          </Form.Item>
        </Form>
      </Modal>
    </>
  )
}
