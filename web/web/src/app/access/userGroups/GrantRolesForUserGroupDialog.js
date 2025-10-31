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

import React, { useState, useEffect } from 'react'
import { Form, Modal, Select, Typography } from 'antd'
import { validateMessages } from '@/config'
import { useResetFormOnCloseModal } from '@/lib/hooks/use-reset'
import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import { fetchRoles } from '@/lib/store/roles'
import { fetchUserGroups, grantRolesForUserGroup } from '@/lib/store/userGroups'

const { Paragraph } = Typography
const { Option } = Select

export default function CreateRoleDialog({ ...props }) {
  const { open, setOpen, userGroup, metalake } = props
  const [confirmLoading, setConfirmLoading] = useState(false)

  const [form] = Form.useForm()
  const values = Form.useWatch([], form)
  const dispatch = useAppDispatch()
  const store = useAppSelector(state => state.roles)

  const defaultValues = {
    roles: userGroup.roles
  }

  useEffect(() => {
    open && dispatch(fetchRoles({ metalake }))
  }, [open, dispatch, metalake])

  useResetFormOnCloseModal({
    form,
    open
  })

  const handleSubmit = e => {
    e.preventDefault()
    form
      .validateFields()
      .then(async () => {
        setConfirmLoading(true)
        const grantRoleNams = values.roles.filter(role => !userGroup.roles.includes(role))
        const revokeRoleNames = userGroup.roles.filter(role => !values.roles.includes(role))
        if (grantRoleNams.length) {
          await dispatch(
            grantRolesForUserGroup({ metalake, group: userGroup.name, data: { roleNames: grantRoleNams } })
          )
        }
        if (revokeRoleNames.length) {
          await dispatch(
            revokeRolesForUserGroup({ metalake, group: userGroup.name, data: { roleNames: revokeRoleNames } })
          )
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
        title='Grant Role'
        open={open}
        onOk={handleSubmit}
        okText='Submit'
        maskClosable={false}
        keyboard={false}
        width={400}
        confirmLoading={confirmLoading}
        onCancel={handleCancel}
      >
        <Paragraph type='secondary'>Grant the role to user {userGroup.name}.</Paragraph>
        <Form
          form={form}
          initialValues={defaultValues}
          layout='vertical'
          name='grantRolesForm'
          validateMessages={validateMessages}
        >
          <Form.Item name='roles' label='Roles' rules={[{ required: userGroup.roles.length === 0 }]}>
            <Select mode='multiple' placeholder='roles'>
              {store.rolesData?.map(role => (
                <Option key={role} value={role}>
                  {role}
                </Option>
              ))}
            </Select>
          </Form.Item>
        </Form>
      </Modal>
    </>
  )
}
