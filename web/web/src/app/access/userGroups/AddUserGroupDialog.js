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

import React, { useState } from 'react'
import { Form, Input, Modal, Typography } from 'antd'
import { validateMessages, mismatchName } from '@/config'
import { nameRegex } from '@/config/regex'
import { useResetFormOnCloseModal } from '@/lib/hooks/use-reset'
import { createUserGroup } from '@/lib/store/userGroups'
import { useAppDispatch } from '@/lib/hooks/useStore'

const { Paragraph } = Typography

const defaultValues = {
  name: ''
}

export default function AddUserGroupDialog({ ...props }) {
  const { open, setOpen, metalake } = props
  const [confirmLoading, setConfirmLoading] = useState(false)
  const dispatch = useAppDispatch()

  const [form] = Form.useForm()
  const values = Form.useWatch([], form)

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

        const submitData = {
          name: values.name.trim()
        }

        await dispatch(createUserGroup({ metalake, data: submitData }))
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
        title='Add User Group'
        open={open}
        onOk={handleSubmit}
        okText='Submit'
        maskClosable={false}
        keyboard={false}
        width={400}
        confirmLoading={confirmLoading}
        onCancel={handleCancel}
      >
        <Paragraph type='secondary'>Add a user group.</Paragraph>
        <Form
          form={form}
          initialValues={defaultValues}
          layout='vertical'
          name='roleForm'
          validateMessages={validateMessages}
        >
          <Form.Item
            name='name'
            label='User Group Name'
            rules={[{ required: true }, { type: 'string', max: 64 }, { pattern: new RegExp(nameRegex) }]}
          >
            <Input placeholder={mismatchName} />
          </Form.Item>
        </Form>
      </Modal>
    </>
  )
}
