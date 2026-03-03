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
import { validateMessages, dialogContentMaxHeigth, mismatchName } from '@/config'
import { nameRegex } from '@/lib/utils/regex'
import { useResetFormOnCloseModal } from '@/lib/hooks/use-reset'
import { genUpdates } from '@/lib/utils'
import { cn } from '@/lib/utils/tailwind'
import { useAppDispatch } from '@/lib/hooks/useStore'
import { createTopic, updateTopic, getTopicDetails } from '@/lib/store/metalakes'

const { Paragraph } = Typography
const { TextArea } = Input

const defaultValues = {
  name: '',
  comment: '',
  properties: []
}

export default function CreateTopicDialog({ ...props }) {
  const { open, setOpen, metalake, catalog, catalogType, schema, editTopic, init } = props
  const [confirmLoading, setConfirmLoading] = useState(false)
  const [isLoading, setIsLoading] = useState(false)
  const [cacheData, setCacheData] = useState()
  const treeRef = useContext(TreeRefContext)
  const scrollRef = useRef(null)
  const loadedRef = useRef(false)
  const scrolling = useScrolling(scrollRef)
  const [bottomShadow, setBottomShadow] = useState(false)
  const [topShadow, setTopShadow] = useState(false)
  const [form] = Form.useForm()
  const values = Form.useWatch([], form)
  const dispatch = useAppDispatch()

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
    if (open && editTopic && !loadedRef.current) {
      loadedRef.current = true

      const initLoad = async () => {
        setIsLoading(true)
        try {
          const {
            payload: { topic }
          } = await dispatch(getTopicDetails({ metalake, catalog, schema, topic: editTopic }))
          setCacheData(topic)
          form.setFieldValue('name', topic.name)
          form.setFieldValue('comment', topic.comment)
          let index = 0
          Object.entries(topic.properties || {}).forEach(([key, value]) => {
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
  }, [open, editTopic, metalake, catalog, schema])

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
        if (editTopic) {
          // update topic
          const reqData = { updates: genUpdates(cacheData, submitData) }
          if (reqData.updates.length) {
            await dispatch(
              updateTopic({ init, metalake, catalog, catalogType, schema, topic: editTopic, data: reqData })
            )
          }
        } else {
          await dispatch(createTopic({ data: submitData, metalake, catalog, schema, catalogType }))
        }
        !editTopic && treeRef.current.onLoadData({ key: `${catalog}/${schema}`, nodeType: 'schema' })
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
        title={!editTopic ? 'Create Topic' : 'Edit Topic'}
        open={open}
        onOk={handleSubmit}
        okText='Submit'
        okButtonProps={{ 'data-refer': 'handle-submit-topic' }}
        maskClosable={false}
        keyboard={false}
        width={800}
        confirmLoading={confirmLoading}
        onCancel={handleCancel}
      >
        <Paragraph type='secondary'>{!editTopic ? 'Create a new topic.' : `Edit the topic ${editTopic}.`}</Paragraph>
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
                name='topicForm'
                validateMessages={validateMessages}
              >
                <Form.Item
                  name='name'
                  label='Topic Name'
                  rules={[{ required: true }, { type: 'string', max: 64 }, { pattern: new RegExp(nameRegex) }]}
                  messageVariables={{ label: 'topic name' }}
                >
                  <Input data-refer='topic-name-field' placeholder={mismatchName} disabled={!!editTopic} />
                </Form.Item>
                <Form.Item name='comment' label='Comment'>
                  <TextArea data-refer='topic-comment-field' />
                </Form.Item>
                <Form.Item label='Properties'>
                  <Form.List name='properties'>
                    {(fields, subOpt) => (
                      <RenderPropertiesFormItem
                        fields={fields}
                        subOpt={subOpt}
                        form={form}
                        isEdit={!!editTopic}
                        isDisable={false}
                        provider={'kafka'}
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
