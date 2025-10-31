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
import { Button, Form, Input, Modal, Spin, Typography } from 'antd'
import Icons from '@/components/Icons'
import CascaderObjectComponent from '@/components/CascaderObjectComponent'
import RenderPropertiesFormItem from '@/components/EntityPropertiesFormItem'
import { validateMessages, mismatchName } from '@/config'
import { nameRegex } from '@/config/regex'
import { useResetFormOnCloseModal } from '@/lib/hooks/use-reset'
import { cn } from '@/lib/utils/tailwind'
import { createRole, getRoleDetails, updateRolePrivileges } from '@/lib/store/roles'
import { useAppDispatch } from '@/lib/hooks/useStore'

const { Paragraph } = Typography

const defaultValues = {
  name: '',
  securableObjects: [{ fullName: '', type: '', allowPrivileges: [], denyPrivileges: [], isShowDeny: false }],
  properties: []
}

export default function CreateRoleDialog({ ...props }) {
  const { open, setOpen, editRole, mutateRoles, metalake } = props
  const [confirmLoading, setConfirmLoading] = useState(false)
  const [isLoading, setIsLoading] = useState(false)
  const [privilegeErrorTips, setPrivilegeErrorTips] = useState('')
  const [cacheData, setCacheData] = useState({})
  const loadedRef = useRef(false)
  const dispatch = useAppDispatch()

  const [form] = Form.useForm()
  const values = Form.useWatch([], form)

  useResetFormOnCloseModal({
    form,
    open
  })

  useEffect(() => {
    if (open && editRole && !loadedRef.current) {
      loadedRef.current = true

      const init = async () => {
        setIsLoading(true)
        try {
          const { payload: role } = await dispatch(getRoleDetails({ metalake, role: editRole }))
          form.setFieldValue('name', role.name)
          let index = 0
          Object.entries(role.properties || {}).forEach(([key, value]) => {
            form.setFieldValue(['properties', index, 'key'], key)
            form.setFieldValue(['properties', index, 'value'], value)
            index++
          })
          const securableObjectsMap = {}
          role.securableObjects.forEach((object, index) => {
            let fullName = object.fullName.split('.')
            if (object.type !== 'metalake') {
              fullName.unshift(metalake)
            }
            form.setFieldValue(['securableObjects', index, 'fullName'], fullName)
            form.setFieldValue(['securableObjects', index, 'type'], object.type)
            form.setFieldValue(
              ['securableObjects', index, 'allowPrivileges'],
              object.privileges.filter(p => p.condition === 'allow').map(p => p.name)
            )
            form.setFieldValue(
              ['securableObjects', index, 'denyPrivileges'],
              object.privileges.filter(p => p.condition !== 'allow').map(p => p.name)
            )
            securableObjectsMap[object.fullName] = [
              object.privileges.filter(p => p.condition === 'allow').map(p => p.name),
              object.privileges.filter(p => p.condition !== 'allow').map(p => p.name)
            ]
          })
          setCacheData(securableObjectsMap)
          setIsLoading(false)
        } catch (e) {
          console.error(e)
          setIsLoading(false)
        }
      }
      init()
    }
    setPrivilegeErrorTips('')

    // Reset loadedRef when dialog closes
    if (!open) {
      loadedRef.current = false
    }
  }, [open, editRole, metalake])

  useEffect(() => {
    setPrivilegeErrorTips('')
  }, [values?.securableObjects])

  const handleSubmit = e => {
    e.preventDefault()
    form
      .validateFields()
      .then(async () => {
        setConfirmLoading(true)

        const submitData = {
          name: values.name.trim(),
          properties: values.properties.reduce((acc, item) => {
            acc[item.key] = values[item.key] || item.value

            return acc
          }, {}),
          securableObjects: values.securableObjects
            .filter(object => object.fullName)
            .map(object => {
              const allowPrivileges = object.allowPrivileges.map(privilege => ({ name: privilege, condition: 'ALLOW' }))
              const denyPrivileges = object.denyPrivileges.map(privilege => ({ name: privilege, condition: 'DENY' }))
              const privileges = [...allowPrivileges, ...denyPrivileges]
              if (privileges.length === 0) {
                throw new Error('At least one privilege is required for each securable object.')
              }
              const type = object.type
              let fullName = object.fullName.at(-1)
              if (type === 'schema') {
                fullName = `${object.fullName.at(-2)}.${object.fullName.at(-1)}`
              }
              if (['table', 'fileset', 'topic', 'model'].includes(type)) {
                fullName = `${object.fullName.at(-3)}.${object.fullName.at(-2)}.${object.fullName.at(-1)}`
              }

              return {
                fullName: fullName,
                type: type,
                privileges: privileges
              }
            })
        }
        if (editRole) {
          const reqData = { updates: submitData.securableObjects }
          await dispatch(updateRolePrivileges({ metalake, role: editRole, data: reqData }))
        } else {
          await dispatch(createRole({ metalake, data: submitData }))
        }
        setConfirmLoading(false)
        setOpen(false)
      })
      .catch(info => {
        console.error(info)
        setConfirmLoading(false)
        const formItem = info?.errorFields?.[0]?.name?.[0]
        if (formItem !== 'name') {
          setPrivilegeErrorTips(info.message)
        }
        form.scrollToField(formItem)
      })
  }

  const handleCancel = () => {
    setOpen(false)
  }

  const renderPrivileges = (fields, subOpt) => {
    return (
      <>
        <div className='flex flex-col divide-y divide-solid border-b border-solid '>
          <div className='grid grid-cols-8 divide-x divide-solid '>
            <div className='col-span-3 bg-gray-100 p-1 text-center'>Object</div>
            <div className='col-span-4 bg-gray-100 p-1 text-center'>Privileges</div>
            <div className='bg-gray-100 p-1 text-center'>Actions</div>
          </div>
          {fields.map(subField => (
            <div key={subField.key}>
              <div className='grid grid-cols-8'>
                <CascaderObjectComponent
                  form={form}
                  subField={subField}
                  metalake={metalake}
                  editRole={editRole}
                  cacheData={cacheData}
                />
                <div className='flex items-center px-2 py-1'>
                  <Icons.Minus
                    className='size-4 cursor-pointer text-gray-400 hover:text-defaultPrimary'
                    onClick={() => {
                      subOpt.remove(subField.name)
                    }}
                  />
                </div>
              </div>
            </div>
          ))}
          <div className='text-center'>
            <Button
              type='link'
              icon={<PlusOutlined />}
              onClick={() => {
                subOpt.add()
              }}
            >
              Add Securable Object
            </Button>
          </div>
        </div>
        {privilegeErrorTips && <span className='text-red-500'>{privilegeErrorTips}</span>}
      </>
    )
  }

  return (
    <>
      <Modal
        title={!editRole ? 'Create Role' : `Edit Role ${editRole}`}
        open={open}
        onOk={handleSubmit}
        okText='Submit'
        maskClosable={false}
        keyboard={false}
        width={1000}
        confirmLoading={confirmLoading}
        onCancel={handleCancel}
      >
        <Paragraph type='secondary'>{!editRole ? 'Create a new role' : `Update Role ${editRole} Privileges`}</Paragraph>
        <Spin spinning={isLoading}>
          <Form
            form={form}
            initialValues={defaultValues}
            layout='vertical'
            name='roleForm'
            validateMessages={validateMessages}
          >
            <Form.Item
              name='name'
              label='Role Name'
              rules={[{ required: true }, { type: 'string', max: 64 }, { pattern: new RegExp(nameRegex) }]}
            >
              <Input placeholder={mismatchName} disabled={!!editRole} />
            </Form.Item>
            <Form.Item label='Securable Objects' name='securableObjects'>
              <Form.List name='securableObjects'>{(fields, subOpt) => renderPrivileges(fields, subOpt)}</Form.List>
            </Form.Item>
            <Form.Item label='Properties'>
              <Form.List name='properties'>
                {(fields, subOpt) => (
                  <RenderPropertiesFormItem
                    fields={fields}
                    subOpt={subOpt}
                    form={form}
                    editRole={editRole}
                    isDisable={!!editRole}
                  />
                )}
              </Form.List>
            </Form.Item>
          </Form>
        </Spin>
      </Modal>
    </>
  )
}
