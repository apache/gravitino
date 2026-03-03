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

import { InfoCircleOutlined } from '@ant-design/icons'
import { Flex, Form, Input, Modal, Radio, Select, Space, Spin, Switch, Tooltip, Typography } from 'antd'
import { useScrolling } from 'react-use'
import { TreeRefContext } from '../page'
import Icons from '@/components/Icons'
import RenderPropertiesFormItem from '@/components/EntityPropertiesFormItem'
import { dialogContentMaxHeigth, validateMessages, mismatchName } from '@/config'
import { filesetLocationProviders } from '@/config/catalog'
import { nameRegex } from '@/lib/utils/regex'
import { useResetFormOnCloseModal } from '@/lib/hooks/use-reset'
import { genUpdates } from '@/lib/utils'
import { cn } from '@/lib/utils/tailwind'
import { useAppDispatch } from '@/lib/hooks/useStore'
import { createFileset, updateFileset, getFilesetDetails } from '@/lib/store/metalakes'

const { Paragraph } = Typography
const { TextArea } = Input

export default function CreateFilesetDialog({ ...props }) {
  const { open, setOpen, metalake, catalog, catalogType, schema, editFileset, locationProviders, init } = props
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

  const defaultLocationName = Form.useWatch(['properties'], form)?.filter(
    item => item?.key === 'default-location-name'
  )[0]
  const storageLocationsItems = Form.useWatch(['storageLocations'], form)
  const currentSelectBefore = locationProviders?.map(p => filesetLocationProviders[p]) || []
  const selectBefore = [...new Set(['file:/', 'hdfs://', ...currentSelectBefore])]

  const defaultValues = {
    name: '',
    type: 'managed',
    storageLocations: [{ name: '', location: '', prefix: selectBefore[0], defaultLocation: true }],
    comment: '',
    properties: []
  }

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

  // const { trigger: getEditFileset } = useFilesetAsync()

  useEffect(() => {
    if (open && editFileset && !loadedRef.current) {
      loadedRef.current = true

      const initLoad = async () => {
        setIsLoading(true)
        try {
          const {
            payload: { fileset }
          } = await dispatch(getFilesetDetails({ metalake, catalog, schema, fileset: editFileset }))
          setCacheData(fileset)
          form.setFieldValue('name', fileset.name)
          form.setFieldValue('type', fileset.type)
          form.setFieldValue('comment', fileset.comment)
          let locationIndex = 0
          Object.entries(fileset.storageLocations || {}).forEach(([name, location]) => {
            form.setFieldValue(['storageLocations', locationIndex, 'name'], name)
            const prefixs = Object.values(filesetLocationProviders)
            const prefix = prefixs.find(pre => location.startsWith(pre))
            form.setFieldValue(['storageLocations', locationIndex, 'prefix'], prefix)
            form.setFieldValue(['storageLocations', locationIndex, 'location'], location.replace(prefix, ''))
            if (fileset.properties['default-location-name'] === name) {
              form.setFieldValue(['storageLocations', locationIndex, 'defaultLocation'], true)
            } else {
              form.setFieldValue(['storageLocations', locationIndex, 'defaultLocation'], false)
            }
            locationIndex++
          })
          let index = 0
          Object.entries(fileset.properties || {}).forEach(([key, value]) => {
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
  }, [open, editFileset, metalake, catalog, schema])

  const handleSubmit = e => {
    e.preventDefault()
    form
      .validateFields()
      .then(async () => {
        setConfirmLoading(true)
        let defaultLocation = ''

        const submitData = {
          name: values.name.trim(),
          type: values.type,
          storageLocations: values.storageLocations.reduce((acc, item) => {
            if (item.name && item.location) {
              acc[item.name] = item.prefix && item.location ? item.prefix + item.location : item.location
              if (item.defaultLocation && !values.properties['default-location-name']) {
                defaultLocation = item.name
              }
            }

            return acc
          }, {}),
          comment: values.comment,
          tagsToAdd: values.tags,
          properties: values.properties.reduce((acc, item) => {
            acc[item.key] = values[item.key] || item.value

            return acc
          }, {})
        }
        defaultLocation && (submitData.properties['default-location-name'] = defaultLocation)
        if (editFileset) {
          // update fileset
          const reqData = { updates: genUpdates(cacheData, submitData) }
          if (reqData.updates.length) {
            await dispatch(
              updateFileset({ init, data: reqData, metalake, catalog, catalogType, schema, fileset: cacheData.name })
            )
          }
        } else {
          await dispatch(createFileset({ data: submitData, metalake, catalog, catalogType, schema }))
        }
        !editFileset && treeRef.current.onLoadData({ key: `${catalog}/${schema}`, nodeType: 'schema' })
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

  const onChangeDefaultLocation = (checked, index) => {
    const fields = form.getFieldValue('storageLocations')
    fields.forEach((item, i) => {
      if (i !== index) {
        item.defaultLocation = false
      } else {
        item.defaultLocation = checked
      }
    })
    form.setFieldValue(['storageLocations'], fields)
  }

  return (
    <>
      <Modal
        title={!editFileset ? 'Create Fileset' : 'Edit Fileset'}
        open={open}
        onOk={handleSubmit}
        okText='Submit'
        okButtonProps={{ 'data-refer': 'handle-submit-fileset' }}
        maskClosable={false}
        keyboard={false}
        width={800}
        confirmLoading={confirmLoading}
        onCancel={handleCancel}
      >
        <Paragraph type='secondary'>
          {!editFileset ? 'Create a new fileset.' : `Edit the fileset ${editFileset}.`}
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
                name='filesetForm'
                validateMessages={validateMessages}
              >
                <Form.Item
                  name='name'
                  label='Fileset Name'
                  rules={[{ required: true }, { type: 'string', max: 64 }, { pattern: new RegExp(nameRegex) }]}
                  messageVariables={{ label: 'fileset name' }}
                >
                  <Input data-refer='fileset-name-field' placeholder={mismatchName} disabled={init} />
                </Form.Item>
                <Form.Item
                  name='type'
                  label='Type'
                  tooltip={{
                    title: (
                      <>
                        <div>
                          When the fileset type is set to 'Managed', the underlying storage location associated with it
                          will be deleted upon the deletion of the fileset. Conversely, if the fileset type is set to
                          'External', the storage location linked to it will not be deleted when the fileset is removed.
                        </div>
                      </>
                    ),
                    placement: 'right',
                    icon: <InfoCircleOutlined />
                  }}
                >
                  <Radio.Group disabled={!!editFileset}>
                    <Radio value={'managed'}>Managed</Radio>
                    <Radio value={'external'}>External</Radio>
                  </Radio.Group>
                </Form.Item>
                <Form.Item
                  label='Storage Locations'
                  tooltip={{
                    title: (
                      <>
                        <div>
                          1. The 'Storage Location' refers to the physical location where the fileset is stored.
                        </div>
                        <div>
                          2. It is optional if the fileset is 'Managed' type and a storage location is already specified
                          at the parent catalog or schema level.
                        </div>
                        <div>
                          3. It becomes mandatory if the fileset type is 'External' or no storage location is defined at
                          the parent level.
                        </div>
                      </>
                    ),
                    icon: <InfoCircleOutlined />
                  }}
                >
                  <Form.List name='storageLocations'>
                    {(fields, subOpt) => (
                      <div className='flex w-full flex-col gap-2'>
                        {fields.map(({ key, name, ...restField }) => (
                          <Form.Item label={null} className='align-items-center mb-1' key={key}>
                            <Flex gap='small' align='start' justify='space-between' key={key}>
                              <Form.Item
                                name={[name, 'name']}
                                className='mb-0 w-full grow'
                                rules={[
                                  {
                                    required: values?.type === 'external',
                                    whitespace: true,
                                    message: 'The storage location name is required!'
                                  }
                                ]}
                                {...restField}
                              >
                                <Input
                                  data-refer={`storageLocations-name-${name}`}
                                  placeholder='Name'
                                  disabled={!!editFileset}
                                />
                              </Form.Item>
                              <Form.Item
                                name={[name, 'location']}
                                className='mb-0 w-full grow'
                                rules={[
                                  {
                                    required: values?.type === 'external',
                                    whitespace: true,
                                    message: 'The storage location is required!'
                                  }
                                ]}
                                {...restField}
                              >
                                <Input
                                  data-refer={`storageLocations-location-${name}`}
                                  addonBefore={
                                    selectBefore.length ? (
                                      <Form.Item noStyle name={[name, 'prefix']} label=''>
                                        <Select
                                          data-refer={`storageLocations-prefix-${name}`}
                                          defaultValue={selectBefore[0]}
                                          disabled={!!editFileset}
                                        >
                                          {selectBefore.map(item => (
                                            <Select.Option key={item} value={item} data-refer={`prefix-option-${item}`}>
                                              {item}
                                            </Select.Option>
                                          ))}
                                        </Select>
                                      </Form.Item>
                                    ) : null
                                  }
                                  placeholder='Location'
                                  disabled={!!editFileset}
                                />
                              </Form.Item>
                              {!defaultLocationName &&
                                storageLocationsItems?.length > 1 &&
                                storageLocationsItems[0].name &&
                                storageLocationsItems[0].location && (
                                  <Form.Item name={[name, 'defaultLocation']} className='mb-0 grow-0' {...restField}>
                                    <Tooltip title='Default location'>
                                      <Switch
                                        size='small'
                                        onChange={checked => onChangeDefaultLocation(checked, name)}
                                        disabled={!!editFileset}
                                        checked={storageLocationsItems?.[name]?.defaultLocation === true}
                                      />
                                    </Tooltip>
                                  </Form.Item>
                                )}
                              {name === 0 ? (
                                <Icons.Plus
                                  className={cn(
                                    'size-8 grow-0 cursor-pointer text-gray-400 hover:text-defaultPrimary',
                                    {
                                      'text-gray-100 hover:text-gray-200 cursor-not-allowed': !!editFileset
                                    }
                                  )}
                                  onClick={() => {
                                    if (!!editFileset) return
                                    subOpt.add({
                                      name: '',
                                      location: '',
                                      prefix: selectBefore[0],
                                      defaultLocation: false
                                    })
                                  }}
                                />
                              ) : (
                                <Icons.Minus
                                  className={cn(
                                    'size-8 grow-0 cursor-pointer text-gray-400 hover:text-defaultPrimary',
                                    {
                                      'text-gray-100 hover:text-gray-200 cursor-not-allowed': !!editFileset
                                    }
                                  )}
                                  onClick={() => {
                                    if (!!editFileset) return
                                    subOpt.remove(name)
                                  }}
                                />
                              )}
                            </Flex>
                          </Form.Item>
                        ))}
                      </div>
                    )}
                  </Form.List>
                </Form.Item>
                <Form.Item name='comment' label='Comment'>
                  <TextArea data-refer='fileset-comment-field' />
                </Form.Item>
                <Form.Item label='Properties'>
                  <Form.List name='properties'>
                    {(fields, subOpt) => (
                      <RenderPropertiesFormItem
                        fields={fields}
                        subOpt={subOpt}
                        form={form}
                        isEdit={!!editFileset}
                        isDisable={false}
                        provider={'fileset'}
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
