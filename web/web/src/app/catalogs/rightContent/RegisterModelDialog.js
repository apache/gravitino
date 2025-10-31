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

'use client'

import React, { useContext, useEffect, useRef, useState } from 'react'

import { Form, Input, Modal, Spin, Typography } from 'antd'
import { useScrolling } from 'react-use'

import { TreeRefContext } from '../page'
import RenderPropertiesFormItem from '@/components/EntityPropertiesFormItem'
import { validateMessages, mismatchName } from '@/config'
import { dialogContentMaxHeigth } from '@/config'
import { nameRegex } from '@/config/regex'
import { useResetFormOnCloseModal } from '@/lib/hooks/use-reset'
import { genUpdates } from '@/lib/utils'
import { cn } from '@/lib/utils/tailwind'
import { registerModel, updateModel, getModelDetails } from '@/lib/store/metalakes'
import { useAppDispatch } from '@/lib/hooks/useStore'

const { Paragraph } = Typography
const { TextArea } = Input

const defaultValues = {
  name: '',
  comment: '',
  properties: []
}

export default function RegisterModelDialog({ ...props }) {
  const { open, setOpen, metalake, catalog, catalogType, schema, editModel, init } = props
  const [confirmLoading, setConfirmLoading] = useState(false)
  const [isLoading, setIsLoading] = useState(false)
  const [cacheData, setCacheData] = useState()
  const treeRef = useContext(TreeRefContext)
  const scrollRef = useRef(null)
  const loadedRef = useRef(false)
  const scrolling = useScrolling(scrollRef)
  const [bottomShadow, setBottomShadow] = useState(false)
  const [topShadow, setTopShadow] = useState(false)
  const dispatch = useAppDispatch()

  const [form] = Form.useForm()
  const values = Form.useWatch([], form)

  useResetFormOnCloseModal({
    form,
    open
  })

  const handScroll = () => {
    if (scrollRef.current) {
      const { scrollTop, scrollHeight, clientHeight } = scrollRef.current
      if (scrollHeight > clientHeight + scrollTop) {
        setTopShadow(true)
        setBottomShadow(scrollTop > 0)
      } else if (scrollHeight === clientHeight + scrollTop) {
        setTopShadow(false)
        setBottomShadow(scrollTop > 0)
      }
    }
  }

  useEffect(() => {
    scrollRef.current && handScroll()
  }, [scrolling])

  useEffect(() => {
    if (open && editModel && !loadedRef.current) {
      loadedRef.current = true

      const initLoad = async () => {
        setIsLoading(true)
        try {
          const {
            payload: { model }
          } = await dispatch(getModelDetails({ metalake, catalog, schema, model: editModel }))
          setCacheData(model)
          form.setFieldValue('name', model.name)
          form.setFieldValue('comment', model.comment)
          let index = 0
          Object.entries(model.properties || {}).forEach(([key, value]) => {
            form.setFieldValue(['properties', index, 'key'], key)
            form.setFieldValue(['properties', index, 'value'], value)
            index++
          })
          setIsLoading(false)
        } catch (e) {
          setIsLoading(false)
        }
      }
      initLoad()
    }

    // Reset loadedRef when dialog closes
    if (!open) {
      loadedRef.current = false
    }
  }, [open, editModel, metalake, catalog, schema])

  const handleSubmit = e => {
    e.preventDefault()
    form
      .validateFields()
      .then(async () => {
        setConfirmLoading(true)

        const submitData = {
          name: values.name.trim(),
          comment: values.comment,
          tagsToAdd: values.tags,
          properties: values.properties.reduce((acc, item) => {
            acc[item.key] = values[item.key] || item.value

            return acc
          }, {})
        }
        if (editModel) {
          // update model
          const reqData = { updates: genUpdates(cacheData, submitData) }
          if (reqData.updates.length) {
            await dispatch(updateModel({ metalake, catalog, catalogType, schema, model: editModel, data: reqData }))
          }
        } else {
          await dispatch(registerModel({ data: submitData, metalake, catalog, schema, catalogType }))
        }
        !editModel && treeRef.current.onLoadData({ key: `${catalog}/${schema}`, nodeType: 'schema' })
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
        title='Register Model'
        open={open}
        onOk={handleSubmit}
        okText='Submit'
        maskClosable={false}
        keyboard={false}
        width={800}
        confirmLoading={confirmLoading}
        onCancel={handleCancel}
      >
        <Paragraph type='secondary'>Register a new model.</Paragraph>
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
                name='modelForm'
                validateMessages={validateMessages}
              >
                <Form.Item
                  name='name'
                  label='Model Name'
                  rules={[{ required: true }, { type: 'string', max: 64 }, { pattern: new RegExp(nameRegex) }]}
                >
                  <Input placeholder={mismatchName} disabled={!init} />
                </Form.Item>
                <Form.Item name='comment' label='Comment'>
                  <TextArea />
                </Form.Item>
                <Form.Item label='Properties'>
                  <Form.List name='properties'>
                    {(fields, subOpt) => (
                      <RenderPropertiesFormItem
                        fields={fields}
                        subOpt={subOpt}
                        form={form}
                        isEdit={false}
                        isDisable={false}
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
