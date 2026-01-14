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
import { ColorPicker, Form, Input, Modal, Space, Typography } from 'antd'
import Icons from '@/components/Icons'
import RenderPropertiesFormItem from '@/components/EntityPropertiesFormItem'
import { validateMessages, mismatchName } from '@/config'
import { nameRegex } from '@/lib/utils/regex'
import { useResetFormOnCloseModal } from '@/lib/hooks/use-reset'
import { genUpdates, generateRandomColor } from '@/lib/utils'
import { useAppDispatch } from '@/lib/hooks/useStore'
import { getTagDetails, createTag, updateTag } from '@/lib/store/tags'

const { Paragraph } = Typography
const { TextArea } = Input

const defaultValues = {
  name: '',
  comment: '',
  color: '#0369a1',
  properties: []
}

export default function CreateTagDialog({ ...props }) {
  const { open, setOpen, metalake, editTag } = props
  const [confirmLoading, setConfirmLoading] = useState(false)
  const [cacheData, setCacheData] = useState()
  const loadedRef = useRef(false)
  const dispatch = useAppDispatch()
  const [form] = Form.useForm()
  const values = Form.useWatch([], form)

  useResetFormOnCloseModal({
    form,
    open
  })

  useEffect(() => {
    if (open && editTag && !loadedRef.current) {
      loadedRef.current = true

      const init = async () => {
        const { payload: tag } = await dispatch(getTagDetails({ metalake, tag: editTag }))
        setCacheData(tag)
        form.setFieldValue('name', tag.name)
        form.setFieldValue('comment', tag.comment)
        form.setFieldValue('color', tag.properties?.color)
        let index = 0
        Object.entries(tag.properties || {}).forEach(([key, value]) => {
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
  }, [open, editTag, metalake])

  const handlePickerClick = () => {
    const randomColor = generateRandomColor()
    form.setFieldValue(['color'], randomColor)
  }

  const handlePickerChange = color => {
    form.setFieldValue(['color'], color.toHexString())
  }

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
        submitData.properties.color = typeof values.color !== 'string' ? values.color.toHexString() : values.color
        if (editTag) {
          // update tag
          const reqData = { updates: genUpdates(cacheData, submitData) }
          if (reqData.updates.length) {
            await dispatch(updateTag({ metalake, tag: editTag, data: reqData }))
          }
        } else {
          await dispatch(createTag({ metalake, data: submitData }))
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
        title={!editTag ? 'Create Tag' : 'Edit Tag'}
        open={open}
        onOk={handleSubmit}
        okText='Submit'
        maskClosable={false}
        keyboard={false}
        width={800}
        confirmLoading={confirmLoading}
        onCancel={handleCancel}
      >
        <Paragraph type='secondary'>{!editTag ? 'Create a new tag' : `Edit the tag ${editTag}.`}</Paragraph>
        <Form
          form={form}
          initialValues={defaultValues}
          layout='vertical'
          name='tagForm'
          validateMessages={validateMessages}
        >
          <Form.Item
            name='name'
            label='Tag Name'
            rules={[{ required: true }, { type: 'string', max: 64 }, { pattern: new RegExp(nameRegex) }]}
          >
            <Input placeholder={mismatchName} />
          </Form.Item>
          <Form.Item name='comment' label='Comment'>
            <TextArea />
          </Form.Item>
          <Form.Item name='color' label='Color'>
            <Space>
              <ColorPicker value={values?.color} onChange={handlePickerChange} />
              <Input value={values?.color} readOnly />
              <Icons.RefreshCcw className='size-4 cursor-pointer' onClick={handlePickerClick} />
            </Space>
          </Form.Item>
          <Form.Item label='Properties'>
            <Form.List name='properties'>
              {(fields, subOpt) => (
                <RenderPropertiesFormItem fields={fields} subOpt={subOpt} form={form} isEdit={!!editTag} />
              )}
            </Form.List>
          </Form.Item>
        </Form>
      </Modal>
    </>
  )
}
