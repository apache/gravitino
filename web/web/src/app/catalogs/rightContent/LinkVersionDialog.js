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
import { Flex, Form, Input, Modal, Spin, Switch, Tooltip, Typography } from 'antd'
import { useScrolling } from 'react-use'
import Icons from '@/components/Icons'
import RenderPropertiesFormItem from '@/components/EntityPropertiesFormItem'
import { validateMessages, dialogContentMaxHeigth } from '@/config'
import { useResetFormOnCloseModal } from '@/lib/hooks/use-reset'
import { genUpdates } from '@/lib/utils'
import { cn } from '@/lib/utils/tailwind'
import { linkVersion, updateVersion, getVersionDetails } from '@/lib/store/metalakes'
import { useAppDispatch } from '@/lib/hooks/useStore'

const { Paragraph } = Typography
const { TextArea } = Input

const defaultValues = {
  uris: [{ name: '', uri: '', defaultUri: true }],
  aliases: [{ name: '' }],
  comment: '',
  properties: []
}

export default function LinkVersionDialog({ ...props }) {
  const { open, setOpen, metalake, catalog, catalogType, schema, model, editVersion } = props
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
  const urisItems = Form.useWatch(['uris'], form)
  const aliasesItems = Form.useWatch(['aliases'], form)
  const defaultUriName = Form.useWatch(['properties'], form)?.filter(item => item?.key === 'default-uri-name')[0]
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
    if (open && editVersion !== '' && !loadedRef.current) {
      loadedRef.current = true

      const initLoad = async () => {
        setIsLoading(true)
        try {
          const { payload: modelVersion } = await dispatch(
            getVersionDetails({ metalake, catalog, catalogType, schema, model, version: editVersion })
          )
          setCacheData(modelVersion)
          let uriIndex = 0
          Object.entries(modelVersion.uris || {}).forEach(([name, uri]) => {
            form.setFieldValue(['uris', uriIndex, 'name'], name)
            form.setFieldValue(['uris', uriIndex, 'uri'], uri)
            if (modelVersion.properties['default-uri-name'] === name) {
              form.setFieldValue(['uris', uriIndex, 'defaultUri'], true)
            } else {
              form.setFieldValue(['uris', uriIndex, 'defaultUri'], false)
            }
            uriIndex++
          })
          form.setFieldValue('comment', modelVersion.comment)
          modelVersion.aliases.forEach((alias, index) => {
            form.setFieldValue(['aliases', index, 'name'], alias)
          })
          let index = 0
          Object.entries(modelVersion.properties || {}).forEach(([key, value]) => {
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
  }, [open, editVersion, metalake, catalog, schema])

  const handleSubmit = e => {
    e.preventDefault()
    let defaultUri = ''
    form
      .validateFields()
      .then(async () => {
        setConfirmLoading(true)

        const submitData = {
          uris: values.uris.reduce((acc, item) => {
            if (item.name && item.uri) {
              acc[item.name] = item.uri
              if (item.defaultUri && !values.properties.find(prop => prop.key === 'default-uri-name')) {
                defaultUri = item.name
              }
            }

            return acc
          }, {}),
          aliases: values.aliases.map(alias => alias.name).filter(aliasName => aliasName),
          comment: values.comment,
          properties: values.properties.reduce((acc, item) => {
            acc[item.key] = values[item.key] || item.value

            return acc
          }, {})
        }
        defaultUri && (submitData.properties['default-uri-name'] = defaultUri)
        if (editVersion !== '') {
          // update version
          const reqData = { updates: genUpdates(cacheData, submitData) }
          if (reqData.updates.length) {
            dispatch(
              updateVersion({
                metalake,
                catalog,
                catalogType,
                schema,
                model,
                version: cacheData.version,
                data: reqData
              })
            )
          }
        } else {
          await dispatch(linkVersion({ data: submitData, metalake, catalog, schema, catalogType, model }))
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

  const onChangeDefaultUri = (checked, index) => {
    const fields = form.getFieldValue('uris') || []
    fields.forEach((item, i) => {
      if (i !== index) {
        item.defaultUri = false
      } else {
        item.defaultUri = checked
      }
    })
    form.setFieldValue(['uris'], fields)
  }

  return (
    <>
      <Modal
        title='Link Version'
        open={open}
        onOk={handleSubmit}
        okText='Submit'
        maskClosable={false}
        keyboard={false}
        width={800}
        confirmLoading={confirmLoading}
        onCancel={handleCancel}
      >
        <Paragraph type='secondary'>Link a new version.</Paragraph>
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
                name='versionForm'
                validateMessages={validateMessages}
              >
                <Form.Item label='URI(s)'>
                  <Form.List name='uris'>
                    {(fields, subOpt) => (
                      <div className='flex flex-col gap-2'>
                        {fields.map(({ key, name, ...restField }) => (
                          <Form.Item label={null} className='align-items-center mb-1' key={key}>
                            <Flex gap='small' align='center'>
                              <Form.Item
                                className='mb-0 w-full grow'
                                name={[name, 'name']}
                                rules={[
                                  { required: name === 0, message: 'The URI name is required!' },
                                  ({ getFieldValue }) => ({
                                    validator(_, uriName) {
                                      if (getFieldValue('uris').length) {
                                        if (uriName) {
                                          let names = getFieldValue('uris').map(p => p?.name)
                                          names.splice(name, 1)
                                          if (names.includes(uriName)) {
                                            return Promise.reject(new Error('The URI name already exists!'))
                                          }
                                        }
                                      }

                                      return Promise.resolve()
                                    }
                                  })
                                ]}
                                {...restField}
                              >
                                <Input placeholder='URI name' />
                              </Form.Item>
                              <Form.Item
                                className='mb-0 w-full grow'
                                name={[name, 'uri']}
                                rules={[{ required: !!values?.uris[name].name, message: 'URI is required' }]}
                                {...restField}
                              >
                                <Input placeholder='URI' />
                              </Form.Item>
                              {!defaultUriName && urisItems?.length > 1 && urisItems[0].name && urisItems[0].uri && (
                                <Form.Item className='mb-0 grow-0' name={[name, 'defaultUri']} {...restField}>
                                  <Tooltip title='Default URI'>
                                    <Switch
                                      size='small'
                                      onChange={checked => onChangeDefaultUri(checked, name)}
                                      disabled={!!editVersion}
                                      checked={urisItems?.[name]?.defaultUri === true}
                                    />
                                  </Tooltip>
                                </Form.Item>
                              )}
                              <div className='flex w-10 shrink-0 items-center justify-start gap-2'>
                                {name === 0 ? (
                                  <>
                                    <Icons.Plus
                                      className='size-4 cursor-pointer text-gray-400 hover:text-defaultPrimary'
                                      onClick={() => {
                                        subOpt.add({ name: '', uri: '', defaultUri: false })
                                      }}
                                    />
                                    {urisItems?.length > 1 && (
                                      <Icons.Minus
                                        className='size-4 cursor-pointer text-gray-400 hover:text-defaultPrimary'
                                        onClick={() => {
                                          subOpt.remove(name)
                                        }}
                                      />
                                    )}
                                  </>
                                ) : (
                                  <>
                                    <span className='inline-block size-4' />
                                    <Icons.Minus
                                      className='size-4 cursor-pointer text-gray-400 hover:text-defaultPrimary'
                                      onClick={() => {
                                        subOpt.remove(name)
                                      }}
                                    />
                                  </>
                                )}
                              </div>
                            </Flex>
                          </Form.Item>
                        ))}
                      </div>
                    )}
                  </Form.List>
                </Form.Item>
                <Form.Item label='Aliases'>
                  <Form.List name='aliases'>
                    {(fields, subOpt) => (
                      <div className='flex flex-col gap-2'>
                        {fields.map(({ key, name, ...restField }) => (
                          <Form.Item label={null} className='align-items-center mb-1' key={key}>
                            <Flex gap='small' align='center'>
                              <Form.Item
                                {...restField}
                                className='mb-0 w-full grow'
                                name={[name, 'name']}
                                rules={[
                                  ({ getFieldValue }) => ({
                                    validator(_, aliasName) {
                                      if (getFieldValue('aliases').length) {
                                        if (aliasName) {
                                          if (!isNaN(Number(aliasName))) {
                                            return Promise.reject(
                                              new Error('Aliase cannot be a number or numeric string')
                                            )
                                          }
                                          let names = getFieldValue('aliases').map(p => p?.name)
                                          names.splice(name, 1)
                                          if (names.includes(aliasName)) {
                                            return Promise.reject(new Error('The alias already exists!'))
                                          }
                                        }
                                      }

                                      return Promise.resolve()
                                    }
                                  })
                                ]}
                              >
                                <Input placeholder={`Alias ${name + 1}`} />
                              </Form.Item>
                              <div className='flex w-10 shrink-0 items-center justify-start gap-2'>
                                {name === 0 ? (
                                  <>
                                    <Icons.Plus
                                      className='size-4 cursor-pointer text-gray-400 hover:text-defaultPrimary'
                                      onClick={() => {
                                        subOpt.add()
                                      }}
                                    />
                                    {aliasesItems?.length > 1 && (
                                      <Icons.Minus
                                        className='size-4 cursor-pointer text-gray-400 hover:text-defaultPrimary'
                                        onClick={() => {
                                          subOpt.remove(name)
                                        }}
                                      />
                                    )}
                                  </>
                                ) : (
                                  <>
                                    <span className='inline-block size-4' />
                                    <Icons.Minus
                                      className='size-4 cursor-pointer text-gray-400 hover:text-defaultPrimary'
                                      onClick={() => {
                                        subOpt.remove(name)
                                      }}
                                    />
                                  </>
                                )}
                              </div>
                            </Flex>
                          </Form.Item>
                        ))}
                      </div>
                    )}
                  </Form.List>
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
