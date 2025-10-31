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

import { Form, Input, Modal, Select, Spin, Typography } from 'antd'
import RenderPropertiesFormItem from '@/components/EntityPropertiesFormItem'
import { validateMessages } from '@/config'
import { nameRegex } from '@/config/regex'
import { useResetFormOnCloseModal } from '@/lib/hooks/use-reset'
import { createMetalake, getMetalakeDetails, updateMetalake } from '@/lib/store/metalakes'
import { genUpdates } from '@/lib/utils'
import { useAppDispatch } from '@/lib/hooks/useStore'

const { Paragraph } = Typography
const { TextArea } = Input

const defaultValues = {
  name: '',
  comment: '',
  properties: []
}

export default function CreateMetalakeDialog({ ...props }) {
  const { open, setOpen, editMetalake, mutateMetalakes } = props
  const [confirmLoading, setConfirmLoading] = useState(false)
  const [isLoading, setIsLoading] = useState(false)
  const [cacheData, setCacheData] = useState()
  const dispatch = useAppDispatch()
  const [form] = Form.useForm()
  const values = Form.useWatch([], form)

  useResetFormOnCloseModal({
    form,
    open
  })

  useEffect(() => {
    if (open && editMetalake) {
      const init = async () => {
        setIsLoading(true)
        try {
          const { payload: metalake } = await dispatch(getMetalakeDetails({ metalake: editMetalake }))
          setCacheData(metalake)
          form.setFieldValue('name', metalake.name)
          form.setFieldValue('comment', metalake.comment)
          let index = 0
          Object.entries(metalake.properties || {}).forEach(([key, value]) => {
            form.setFieldValue(['properties', index, 'key'], key)
            form.setFieldValue(['properties', index, 'value'], value)
            index++
          })
          setIsLoading(false)
        } catch (e) {
          setIsLoading(false)
        }
      }
      init()
    }
  }, [open, editMetalake])

  // const { trigger: updateMetalake, isMutating: isUpdateMutating } = useUpdateMetalakeAsync()

  const handleSubmit = e => {
    e.preventDefault()
    form
      .validateFields()
      .then(async () => {
        setConfirmLoading(true)

        const submitData = {
          name: values.name.trim(),
          comment: values.comment,
          properties: values.properties.reduce((acc, item) => {
            acc[item.key] = values[item.key] || item.value

            return acc
          }, {})
        }
        if (editMetalake) {
          // update metalake
          const reqData = { updates: genUpdates(cacheData, submitData) }
          await dispatch(updateMetalake({ name: cacheData.name, data: reqData }))
        } else {
          await dispatch(createMetalake({ ...submitData }))
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
        title={!editMetalake ? 'Create Metalake' : 'Edit Metalake'}
        open={open}
        onOk={handleSubmit}
        okText='Submit'
        okButtonProps={{ loading: false, 'data-refer': 'submit-handle-metalake' }}
        maskClosable={false}
        keyboard={false}
        width={800}
        confirmLoading={confirmLoading}
        onCancel={handleCancel}
        cancelButtonProps={{ 'data-refer': 'cancel-handle-metalake' }}
      >
        <Paragraph type='secondary'>
          {!editMetalake ? 'Create a new metalake' : `Edit metalake ${editMetalake}`}
        </Paragraph>
        <Spin spinning={isLoading}>
          <Form
            form={form}
            initialValues={defaultValues}
            layout='vertical'
            name='metalakeForm'
            validateMessages={validateMessages}
          >
            <Form.Item
              name='name'
              label='Name'
              data-refer='metalake-name-field'
              rules={[{ required: true }, { type: 'string', max: 64 }, { pattern: new RegExp(nameRegex) }]}
            >
              <Input placeholder={validateMessages.pattern?.mismatch} />
            </Form.Item>
            <Form.Item name='comment' label='Comment' data-refer='metalake-comment-field'>
              <TextArea />
            </Form.Item>
            <Form.Item label='Properties'>
              <Form.List name='properties'>
                {(fields, subOpt) => (
                  <RenderPropertiesFormItem fields={fields} subOpt={subOpt} form={form} isEdit={!!editMetalake} />
                )}
              </Form.List>
            </Form.Item>
          </Form>
        </Spin>
      </Modal>
    </>
  )
}
