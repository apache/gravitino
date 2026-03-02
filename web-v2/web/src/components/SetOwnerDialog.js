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

import React, { useRef, useState } from 'react'
import { Form, Modal, Typography } from 'antd'
import UserAndGroupCascader from '@/components/UserAndGroupCascader'
import { validateMessages } from '@/config'
import { cn } from '@/lib/utils/tailwind'
import { useResetFormOnCloseModal } from '@/lib/hooks/use-reset'
import { setEntityOwner, getCurrentEntityOwner } from '@/lib/store/metalakes'
import { useAppDispatch } from '@/lib/hooks/useStore'
import { useEffect } from 'react'

const { Paragraph } = Typography

export default function SetOwnerDialog({ ...props }) {
  const { open, setOpen, metalake, metadataObjectType, metadataObjectFullName, mutateOwner } = props
  const [confirmLoading, setConfirmLoading] = useState(false)
  const cascaderOwnerRef = useRef(null)
  const dispatch = useAppDispatch()
  const [ownerData, setOwnerData] = useState(null)

  const [form] = Form.useForm()
  const values = Form.useWatch([], form)

  useResetFormOnCloseModal({
    form,
    open
  })

  useEffect(() => {
    const initLoad = async () => {
      const { payload } = await dispatch(
        getCurrentEntityOwner({ metalake, metadataObjectType, metadataObjectFullName })
      )
      setOwnerData(payload?.owner)
    }
    if (open) {
      initLoad()
    }
  }, [open])

  const defaultValues = {
    name: ''
  }

  const handleSubmit = e => {
    e.preventDefault()
    form
      .validateFields()
      .then(async () => {
        setConfirmLoading(true)
        const [type, name] = values.name

        const submitData = {
          name: name,
          type: type
        }

        await dispatch(setEntityOwner({ metalake, metadataObjectType, metadataObjectFullName, data: submitData }))
        setConfirmLoading(false)
        mutateOwner && mutateOwner()
        setOpen(false, true)
      })
      .catch(info => {
        console.error(info)
        form.scrollToField(info?.errorFields?.[0]?.name?.[0])
      })
  }

  const handleCancel = () => {
    setOpen(false, false)
  }

  return (
    <>
      <Modal
        title='Set Owner'
        open={open}
        onOk={handleSubmit}
        okText='Submit'
        maskClosable={false}
        keyboard={false}
        width={400}
        confirmLoading={confirmLoading}
        onCancel={handleCancel}
      >
        <Paragraph type='secondary' style={{ marginBottom: '2px' }}>
          {`Set owner for ${metadataObjectFullName}`}
        </Paragraph>
        <Form
          form={form}
          initialValues={defaultValues}
          layout='vertical'
          name='roleForm'
          validateMessages={validateMessages}
        >
          <div
            ref={cascaderOwnerRef}
            className={cn([
              '[&_.ant-cascader-menu-item-content]:relative',
              '[&_.ant-cascader-menu-item-content]:h-[22px]',
              '[&_.ant-cascader-menu-item]:w-full',
              '[&_.ant-cascader-menu-item]:min-w-40',
              '[&_.ant-select-selection-item]:h-[28px]'
            ])}
          >
            <UserAndGroupCascader
              cascaderOwnerRef={cascaderOwnerRef}
              metalake={metalake}
              multiple={false}
              data={ownerData}
              form={form}
            />
          </div>
        </Form>
      </Modal>
    </>
  )
}
