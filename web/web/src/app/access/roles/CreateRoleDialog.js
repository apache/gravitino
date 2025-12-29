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
import { Button, Form, Input, Modal, Spin, Typography, Select, Collapse } from 'antd'
import Icons from '@/components/Icons'
import SecurableObjectFormFields from '@/components/SecurableObjectFormFields'
import RenderPropertiesFormItem from '@/components/EntityPropertiesFormItem'
import { validateMessages, mismatchName } from '@/config'
import { nameRegex } from '@/config/regex'
import { useResetFormOnCloseModal } from '@/lib/hooks/use-reset'
import { useScrolling } from 'react-use'
import { dialogContentMaxHeigth } from '@/config'
import { cn } from '@/lib/utils/tailwind'
import { createRole, getRoleDetails, updateRolePrivileges } from '@/lib/store/roles'
import { to } from '@/lib/utils'
import { useAppDispatch } from '@/lib/hooks/useStore'

const { Paragraph } = Typography

const defaultValues = {
  name: '',
  securableObjects: [{}],
  properties: []
}

export default function CreateRoleDialog({ ...props }) {
  const { open, setOpen, editRole, mutateRoles, metalake } = props
  const [confirmLoading, setConfirmLoading] = useState(false)
  const [isLoading, setIsLoading] = useState(false)
  const [privilegeErrorTips, setPrivilegeErrorTips] = useState('')
  const loadedRef = useRef(false)
  const scrollRef = useRef(null)
  const scrolling = useScrolling(scrollRef)
  const [bottomShadow, setBottomShadow] = useState(false)
  const [topShadow, setTopShadow] = useState(false)
  const dispatch = useAppDispatch()

  const [form] = Form.useForm()
  const values = Form.useWatch([], form)

  const handScroll = () => {
    if (scrollRef.current) {
      const { scrollTop, scrollHeight, clientHeight } = scrollRef.current
      if (scrollHeight > clientHeight + scrollTop) {
        setTopShadow(true)
        setBottomShadow(scrollTop > 0)
      } else if (scrollHeight === clientHeight + scrollTop) {
        setTopShadow(false)
        setBottomShadow(false)
      } else {
        setTopShadow(false)
        setBottomShadow(false)
      }
    }
  }

  useEffect(() => {
    scrollRef.current && handScroll()
  }, [scrolling])

  useResetFormOnCloseModal({
    form,
    open
  })

  useEffect(() => {
    if (open && editRole && !loadedRef.current) {
      loadedRef.current = true

      const init = async () => {
        setIsLoading(true)

        // Mark initialization in progress immediately so child rows
        // skip clearing side-effects while we fetch and populate data.
        form.setFieldsValue({ __init_in_progress: true })
        try {
          const { payload: role } = await dispatch(getRoleDetails({ metalake, role: editRole }))
          form.setFieldValue('name', role.name)
          let index = 0
          Object.entries(role.properties || {}).forEach(([key, value]) => {
            form.setFieldValue(['properties', index, 'key'], key)
            form.setFieldValue(['properties', index, 'value'], value)
            index++
          })
          role.securableObjects.forEach((object, index) => {
            let fullName = object.fullName
            form.setFieldValue(['securableObjects', index, 'type'], object.type)
            form.setFieldValue(['securableObjects', index, 'fullName'], fullName)
            form.setFieldValue(
              ['securableObjects', index, 'allowPrivileges'],
              object.privileges.filter(p => p.condition === 'allow').map(p => p.name)
            )
            form.setFieldValue(
              ['securableObjects', index, 'denyPrivileges'],
              object.privileges.filter(p => p.condition !== 'allow').map(p => p.name)
            )
          })

          // Mark initialization finished so child fields can run their
          // normal side-effects now.
          setTimeout(() => {
            form.setFieldsValue({ __init_in_progress: false })
          }, 100)
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
          name: String(values.name || '').trim(),
          properties: (values.properties || []).reduce((acc, item) => {
            if (!item?.key) return acc
            acc[item.key] = values[item.key] || item.value

            return acc
          }, {}),
          securableObjects: (values.securableObjects || [])
            .filter(object => object.fullName)
            .map(object => {
              const allowPrivileges = object.allowPrivileges.map(privilege => ({ name: privilege, condition: 'ALLOW' }))
              const denyPrivileges = object.denyPrivileges.map(privilege => ({ name: privilege, condition: 'DENY' }))
              const privileges = [...allowPrivileges, ...denyPrivileges]
              if (privileges.length === 0) {
                throw new Error('At least one privilege is required for each securable object.')
              }
              const type = String(object.type || '').toLowerCase()
              const fullName = object.fullName

              return {
                fullName: Array.isArray(fullName) ? fullName.join('.') : String(fullName || ''),
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

  const handleChangeCollapse = () => {
    // Mark initialization in progress immediately so child rows
    // skip clearing side-effects while we fetch and populate data.
    form.setFieldsValue({ __init_in_progress: true })
    setTimeout(() => {
      form.setFieldsValue({ __init_in_progress: false })
    }, 100)
  }

  const renderSecurableObjectItems = (fields, subOpt) => {
    const activeKeys = fields.map(f => String(f.key))

    const items = fields.map(field => {
      const fname = field.name
      const fkey = String(field.key)

      // Lightweight internal check: avoid emitting debug logs in production
      const titleValue = form.getFieldValue(['securableObjects', fname, 'fullName'])
      const fullNameStr = titleValue || ''
      const indexLabel = Number(fname) + 1
      const title = fullNameStr ? `Securable Object - ${fullNameStr}` : `Securable Object - ${indexLabel}`

      return {
        key: fkey,
        label: (
          <div className='flex items-center justify-between w-full'>
            <div className='truncate pr-2'>{title}</div>
            <div
              onClick={e => {
                // prevent collapse toggle when clicking the wrapper
                e.stopPropagation()
              }}
            >
              <Icons.Minus
                className='size-4 cursor-pointer text-gray-400 hover:text-defaultPrimary'
                onClick={e => {
                  e.stopPropagation()
                  subOpt.remove(fname)
                }}
              />
            </div>
          </div>
        ),
        children: <SecurableObjectFormFields fieldName={fname} fieldKey={fkey} metalake={metalake} />
      }
    })

    return (
      <>
        <Collapse defaultActiveKey={activeKeys} accordion={false} items={items} onChange={handleChangeCollapse} />

        <div className='text-center mt-2'>
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
        <div
          className={cn('relative', {
            'after:absolute after:-bottom-10 after:left-0 after:right-0 after:h-10 after:shadow-[0px_-10px_8px_-8px_rgba(5,5,5,0.1)]':
              topShadow,
            'before:absolute before:-top-10 before:left-0 before:right-0 before:h-10 before:z-10 before:shadow-[0px_10px_8px_-8px_rgba(5,5,5,0.1)]':
              bottomShadow
          })}
        >
          <div className='overflow-auto' style={{ maxHeight: `${dialogContentMaxHeigth}px` }} ref={scrollRef}>
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
                  <Form.List name='securableObjects'>
                    {(fields, subOpt) => renderSecurableObjectItems(fields, subOpt)}
                  </Form.List>
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
          </div>
        </div>
      </Modal>
    </>
  )
}
