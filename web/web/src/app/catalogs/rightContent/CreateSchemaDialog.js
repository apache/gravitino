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
import { dialogContentMaxHeigth, validateMessages, mismatchName } from '@/config'
import { filesetLocationProviders } from '@/config/catalog'
import { nameRegex } from '@/lib/utils/regex'
import { useResetFormOnCloseModal } from '@/lib/hooks/use-reset'
import { genUpdates } from '@/lib/utils'
import { cn } from '@/lib/utils/tailwind'
import { useAppDispatch } from '@/lib/hooks/useStore'
import { createSchema, updateSchema, getSchemaDetails } from '@/lib/store/metalakes'

const { Paragraph } = Typography
const { TextArea } = Input

const defaultValues = {
  name: '',
  comment: '',
  properties: []
}

export default function CreateSchemaDialog({ ...props }) {
  const { open, setOpen, metalake, catalog, catalogType, provider, locationProviders, editSchema, init } = props
  const [confirmLoading, setConfirmLoading] = useState(false)
  const [isLoading, setIsLoading] = useState(false)
  const [cacheData, setCacheData] = useState()
  const treeRef = useContext(TreeRefContext)
  const scrollRef = useRef(null)
  const loadedRef = useRef(false)
  const scrolling = useScrolling(scrollRef)
  const [bottomShadow, setBottomShadow] = useState(false)
  const [topShadow, setTopShadow] = useState(false)
  const currentSelectBefore = locationProviders?.map(p => filesetLocationProviders[p])
  const selectBefore = [...new Set(['file:/', 'hdfs://', ...currentSelectBefore])]
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
    if (open && editSchema && !loadedRef.current) {
      loadedRef.current = true

      const initLoad = async () => {
        setIsLoading(true)
        try {
          const {
            payload: { schema }
          } = await dispatch(getSchemaDetails({ metalake, catalog, schema: editSchema }))
          setCacheData(schema)
          form.setFieldValue('name', schema.name)
          form.setFieldValue('comment', schema.comment)
          let index = 0
          Object.entries(schema.properties || {}).forEach(([key, value]) => {
            form.setFieldValue(['properties', index, 'key'], key)
            form.setFieldValue(['properties', index, 'value'], value)
            index++
          })
          handScroll()
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
  }, [open, editSchema, metalake, catalog])

  useEffect(() => {
    if (open && !editSchema && locationProviders?.length) {
      form.setFieldValue(['properties', 0, 'key'], 'location')
      form.setFieldValue(['properties', 0, 'description'], 'Hadoop catalog storage location')
      form.setFieldValue(['properties', 0, 'selectBefore'], selectBefore)
      form.setFieldValue(['properties', 0, 'prefix'], selectBefore?.[0])
    }
  }, [open, editSchema, provider, locationProviders])

  const handleSubmit = e => {
    e.preventDefault()
    form
      .validateFields()
      .then(async () => {
        setConfirmLoading(true)

        const submitData = {
          name: values.name.trim(),
          comment: values.comment,
          properties:
            values.properties &&
            values.properties.reduce((acc, item) => {
              if (item.key === 'location' || item.key.startsWith('location-')) {
                if (item.value) {
                  acc[item.key] = item.prefix ? item.prefix + item.value : item.value
                }
              } else {
                acc[item.key] = values[item.key] || item.value
              }

              return acc
            }, {})
        }
        if (editSchema) {
          // update schema
          const reqData = { updates: genUpdates(cacheData, submitData) }
          if (reqData.updates.length) {
            await dispatch(
              updateSchema({ init, metalake, catalog, catalogType, schema: cacheData.name, data: reqData })
            )
          }
        } else {
          await dispatch(createSchema({ data: submitData, metalake, catalog, catalogType }))
        }
        !editSchema && treeRef.current.onLoadData({ key: catalog, nodeType: 'catalog', inUse: 'true' }, true)
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
        title={!editSchema ? 'Create Schema' : 'Edit Schema'}
        open={open}
        onOk={handleSubmit}
        okText='Submit'
        okButtonProps={{ 'data-refer': 'handle-submit-schema' }}
        maskClosable={false}
        keyboard={false}
        width={800}
        confirmLoading={confirmLoading}
        onCancel={handleCancel}
      >
        <Paragraph type='secondary'>
          {!editSchema ? `Create a new schema in ${catalog}.` : `Edit the schema ${editSchema} in ${catalog}.`}
        </Paragraph>
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
                name='schemaForm'
                validateMessages={validateMessages}
              >
                <Form.Item
                  name='name'
                  label='Schema Name'
                  data-refer='schema-name-field'
                  rules={[{ required: true }, { type: 'string', max: 64 }, { pattern: new RegExp(nameRegex) }]}
                  messageVariables={{ label: 'schema name' }}
                >
                  <Input placeholder={mismatchName} disabled={editSchema} />
                </Form.Item>
                {!['jdbc-mysql', 'lakehouse-paimon'].includes(provider) && (
                  <Form.Item name='comment' label='Comment' data-refer='schema-comment-field'>
                    <TextArea disabled={editSchema} />
                  </Form.Item>
                )}
                {!['jdbc-postgresql', 'lakehouse-paimon', 'kafka', 'jdbc-mysql'].includes(provider) && (
                  <Form.Item label='Properties'>
                    <Form.List name='properties'>
                      {(fields, subOpt) => (
                        <RenderPropertiesFormItem
                          fields={fields}
                          subOpt={subOpt}
                          form={form}
                          isEdit={!!editSchema}
                          isDisable={['jdbc-doris'].includes(provider) && !!editSchema}
                          selectBefore={selectBefore}
                        />
                      )}
                    </Form.List>
                  </Form.Item>
                )}
              </Form>
            </Spin>
          </div>
        </div>
      </Modal>
    </>
  )
}
