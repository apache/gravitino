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

import React, { useEffect, useRef, useState } from 'react'
import { Form, Input, Modal, Spin, Typography } from 'antd'
import { useScrolling } from 'react-use'
import RenderPropertiesFormItem from '@/components/EntityPropertiesFormItem'
import { validateMessages, dialogContentMaxHeigth, mismatchName } from '@/config'
import { nameRegex } from '@/lib/utils/regex'
import { useResetFormOnCloseModal } from '@/lib/hooks/use-reset'
import { cn } from '@/lib/utils/tailwind'
import { useAppDispatch } from '@/lib/hooks/useStore'
import {
  formatSemanticModelDefinition,
  genSemanticModelUpdates,
  parseSemanticModelDefinition
} from '@/lib/utils/semanticModel'
import { createSemanticModel, updateSemanticModel, getSemanticModelDetails } from '@/lib/store/metalakes'

const { Paragraph } = Typography
const { TextArea } = Input

const definitionPlaceholder = `{
  "datasets": [
    {
      "name": "orders",
      "source": { "namespace": ["sales", "mart"], "name": "orders" },
      "fields": []
    }
  ]
}`

const defaultValues = {
  name: '',
  comment: '',
  definition: '',
  properties: []
}

export default function CreateSemanticModelDialog({ ...props }) {
  const { open, setOpen, metalake, catalog, schema, editSemanticModel } = props
  const [confirmLoading, setConfirmLoading] = useState(false)
  const [isLoading, setIsLoading] = useState(false)
  const [cacheData, setCacheData] = useState()
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
    if (open && editSemanticModel && !loadedRef.current) {
      loadedRef.current = true

      const initLoad = async () => {
        setIsLoading(true)
        try {
          const { payload } = await dispatch(
            getSemanticModelDetails({ metalake, catalog, schema, semanticModel: editSemanticModel })
          )
          const loaded = payload?.semanticModel
          setCacheData(loaded)
          form.setFieldValue('name', loaded?.name)
          form.setFieldValue('comment', loaded?.comment)
          form.setFieldValue('definition', formatSemanticModelDefinition(loaded?.definition))
          let index = 0
          Object.entries(loaded?.properties || {}).forEach(([key, value]) => {
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
  }, [open, editSemanticModel, metalake, catalog, schema])

  const validateDefinition = (rule, value) => {
    const { errors } = parseSemanticModelDefinition(value)

    return errors.length ? Promise.reject(new Error(errors.join(' '))) : Promise.resolve()
  }

  const handleSubmit = e => {
    e.preventDefault()
    form
      .validateFields()
      .then(async () => {
        const { definition } = parseSemanticModelDefinition(values.definition)

        setConfirmLoading(true)

        const submitData = {
          name: values.name.trim(),
          comment: values.comment,
          definition,
          properties: (values.properties || []).reduce((acc, item) => {
            acc[item.key] = values[item.key] || item.value

            return acc
          }, {})
        }

        if (editSemanticModel) {
          const updates = genSemanticModelUpdates(cacheData, submitData)
          if (updates.length) {
            await dispatch(
              updateSemanticModel({
                metalake,
                catalog,
                schema,
                semanticModel: editSemanticModel,
                data: { updates }
              })
            )
          }
        } else {
          await dispatch(createSemanticModel({ data: submitData, metalake, catalog, schema }))
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
        title={!editSemanticModel ? 'Create Semantic Model' : 'Edit Semantic Model'}
        open={open}
        onOk={handleSubmit}
        okText='Submit'
        okButtonProps={{ 'data-refer': 'handle-submit-semantic-model' }}
        maskClosable={false}
        keyboard={false}
        width={800}
        confirmLoading={confirmLoading}
        onCancel={handleCancel}
      >
        <Paragraph type='secondary'>
          {!editSemanticModel
            ? 'Create a new semantic model.'
            : `Edit the semantic model ${editSemanticModel}. The definition is replaced as a whole.`}
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
                name='semanticModelForm'
                validateMessages={validateMessages}
              >
                <Form.Item
                  name='name'
                  label='Semantic Model Name'
                  rules={[{ required: true }, { type: 'string', max: 64 }, { pattern: new RegExp(nameRegex) }]}
                  messageVariables={{ label: 'semantic model name' }}
                >
                  <Input data-refer='semantic-model-name-field' placeholder={mismatchName} />
                </Form.Item>
                <Form.Item name='comment' label='Comment'>
                  <TextArea data-refer='semantic-model-comment-field' />
                </Form.Item>
                <Form.Item
                  name='definition'
                  label='Definition'
                  extra='The datasets, relationships and metrics of the semantic model, as JSON. The server validates the sources.'
                  rules={[{ required: true }, { validator: validateDefinition }]}
                  messageVariables={{ label: 'definition' }}
                >
                  <TextArea
                    data-refer='semantic-model-definition-field'
                    autoSize={{ minRows: 12, maxRows: 24 }}
                    className='font-mono text-xs'
                    placeholder={definitionPlaceholder}
                  />
                </Form.Item>
                <Form.Item label='Properties'>
                  <Form.List name='properties'>
                    {(fields, subOpt) => (
                      <RenderPropertiesFormItem
                        fields={fields}
                        subOpt={subOpt}
                        form={form}
                        isEdit={!!editSemanticModel}
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
