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

import React, { useEffect, useState } from 'react'
import { PlusOutlined } from '@ant-design/icons'
import { Button, Flex, Form, Input, Modal, Select, Spin, Switch, Typography } from 'antd'
import Icons from '@/components/Icons'
import RenderPropertiesFormItem from '@/components/EntityPropertiesFormItem'
import { validateMessages, supportedObjectTypesMap, mismatchName } from '@/config'
import { nameRegex } from '@/lib/utils/regex'
import { useResetFormOnCloseModal } from '@/lib/hooks/use-reset'
import { genUpdates } from '@/lib/utils'
import { useAppDispatch, useAppSelector } from '@/lib/hooks/useStore'
import { getPolicyDetails, createPolicy, updatePolicy } from '@/lib/store/policies'
import { fetchRoles } from '@/lib/store/roles'
import { accessControlPrivilegeOptions } from './constants'

const { Paragraph } = Typography
const { TextArea } = Input

const defaultValues = {
  name: '',
  comment: '',
  enabled: true,
  policyType: 'custom',
  privileges: [],
  applicableRoles: [],
  supportedObjectTypes: [],
  rules: [],
  properties: []
}

export default function CreatePolicyDialog({ ...props }) {
  const { open, setOpen, metalake, editPolicy } = props
  const [confirmLoading, setConfirmLoading] = useState(false)
  const [isLoading, setIsLoading] = useState(false)
  const [cacheData, setCacheData] = useState()
  const [form] = Form.useForm()
  const values = Form.useWatch([], form)
  const dispatch = useAppDispatch()
  const roles = useAppSelector(state => state.roles)
  const anthEnable = useAppSelector(state => state.auth.anthEnable)
  const isAccessControl = values?.policyType === 'system_access_control'

  useResetFormOnCloseModal({
    form,
    open
  })

  useEffect(() => {
    setCacheData(undefined)
    setIsLoading(open && !!editPolicy)
    if (!open || !editPolicy) return
    let cancelled = false

    const init = async () => {
      const { payload: policy } = await dispatch(getPolicyDetails({ metalake, policy: editPolicy }))
      if (cancelled) return
      setIsLoading(false)
      if (!policy) return
      setCacheData(policy)
      form.setFieldsValue({
        name: policy.name,
        enabled: policy.enabled,
        comment: policy.comment,
        policyType: policy.policyType,
        privileges: policy.content?.privileges || [],
        applicableRoles: policy.content?.applicableRoles || [],
        supportedObjectTypes: policy.content?.supportedObjectTypes || [],
        rules: Object.entries(policy.content?.customRules || {})
          .filter(([key]) => key !== 'color')
          .map(([ruleName, ruleContent]) => ({ ruleName, ruleContent })),
        properties: Object.entries(policy.content?.properties || {})
          .filter(([key]) => key !== 'color')
          .map(([key, value]) => ({ key, value }))
      })
    }
    init()

    return () => {
      cancelled = true
    }
  }, [open, editPolicy, metalake, dispatch, form])

  useEffect(() => {
    if (open && isAccessControl && anthEnable) dispatch(fetchRoles({ metalake }))
  }, [open, isAccessControl, anthEnable, metalake, dispatch])

  const handleSubmit = async e => {
    e.preventDefault()
    try {
      const values = await form.validateFields()
      setConfirmLoading(true)

      const submitData = {
        name: values.name.trim(),
        comment: values.comment,
        enabled: values.enabled,
        policyType: values.policyType,
        content:
          values.policyType === 'system_access_control'
            ? { privileges: values.privileges, applicableRoles: values.applicableRoles }
            : {
                customRules: values.rules.reduce((acc, item) => {
                  if (item.ruleName) acc[item.ruleName] = item.ruleContent

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
        const reqData = { updates: genUpdates(cacheData, submitData) }
        if (reqData.updates.length) {
          await dispatch(updatePolicy({ metalake, policy: editPolicy, data: reqData })).unwrap()
        }
      } else {
        await dispatch(createPolicy({ metalake, data: submitData })).unwrap()
      }
      setOpen(false)
    } catch (info) {
      if (info?.errorFields?.length) form.scrollToField(info.errorFields[0].name)
    } finally {
      setConfirmLoading(false)
    }
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
        okButtonProps={{ disabled: !!editPolicy && !cacheData }}
        onCancel={handleCancel}
      >
        <Paragraph type='secondary'>{!editPolicy ? 'Create a new policy' : `Edit the policy ${editPolicy}.`}</Paragraph>
        <Spin spinning={isLoading}>
          <Form
            disabled={!!editPolicy && !cacheData}
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
              messageVariables={{ label: 'policy name' }}
            >
              <Input placeholder={mismatchName} />
            </Form.Item>
            <Form.Item name='enabled' label='Enabled'>
              <Switch disabled={!!editPolicy} />
            </Form.Item>
            <Form.Item name='policyType' label='Policy Type'>
              <Select disabled={!!editPolicy}>
                <Select.Option value='custom'>Custom</Select.Option>
                <Select.Option value='system_access_control' disabled={!anthEnable}>
                  System Access Control
                </Select.Option>
              </Select>
            </Form.Item>
            {isAccessControl ? (
              <>
                <Paragraph type='secondary'>
                  Grants the selected privileges when any applicable role matches. Associate this policy with a tag; it
                  cannot be assigned directly to a table, schema, or other metadata object.
                </Paragraph>
                <Form.Item
                  name='applicableRoles'
                  label='Applicable Roles'
                  rules={[{ type: 'array', required: true, min: 1, message: 'Please select at least one role.' }]}
                >
                  <Select
                    mode='multiple'
                    showSearch
                    optionFilterProp='label'
                    loading={roles.rolesLoading}
                    placeholder='Select roles'
                    options={roles.rolesData?.map(role => ({ value: role, label: role }))}
                  />
                </Form.Item>
                <Form.Item
                  name='privileges'
                  label='Privileges'
                  rules={[{ type: 'array', required: true, min: 1, message: 'Please select at least one privilege.' }]}
                >
                  <Select
                    mode='multiple'
                    showSearch
                    optionFilterProp='label'
                    placeholder='Select privileges'
                    options={accessControlPrivilegeOptions}
                  />
                </Form.Item>
              </>
            ) : (
              <>
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
              </>
            )}
            <Form.Item name='comment' label='Comment'>
              <TextArea />
            </Form.Item>
            {!isAccessControl && (
              <Form.Item label='Properties'>
                <Form.List name='properties'>
                  {(fields, subOpt) => (
                    <RenderPropertiesFormItem fields={fields} subOpt={subOpt} form={form} isEdit={!!editPolicy} />
                  )}
                </Form.List>
              </Form.Item>
            )}
          </Form>
        </Spin>
      </Modal>
    </>
  )
}
