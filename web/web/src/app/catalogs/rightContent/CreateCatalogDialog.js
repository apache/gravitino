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

import {
  CheckCircleFilled,
  ExclamationCircleFilled,
  InfoCircleOutlined,
  LeftOutlined,
  RightOutlined
} from '@ant-design/icons'
import { Button, Cascader, Flex, Form, Input, Modal, Select, Spin, Steps, Tooltip, Typography, theme } from 'antd'
import { useScrolling } from 'react-use'

import Icons from '@/components/Icons'
import RenderPropertiesFormItem from '@/components/EntityPropertiesFormItem'
import { dialogContentMaxHeigth, validateMessages, mismatchName } from '@/config'
import {
  checkCatalogIcon,
  entityDefaultProps,
  messagingProviders,
  providerBase,
  rangerDefaultProps,
  regionOptions,
  relationalProviders
} from '@/config/catalog'
import { nameRegex } from '@/lib/utils/regex'
import { useResetFormOnCloseModal } from '@/lib/hooks/use-reset'
import { genUpdates } from '@/lib/utils'
import { cn } from '@/lib/utils/tailwind'
import { useAppDispatch } from '@/lib/hooks/useStore'
import { createCatalog, updateCatalog, getCatalogDetails, testCatalogConnection } from '@/lib/store/metalakes'

const { Paragraph } = Typography
const { TextArea } = Input

export default function CreateCatalogDialog({ ...props }) {
  const { open, setOpen, metalake, catalogType, editCatalog, init } = props
  const [step, setCurrentStep] = useState(0)
  const [cacheData, setCacheData] = useState()
  const [providers, setProviders] = useState([])
  const [isTestLoading, setIsTestLoading] = useState(false)
  const [isLoading, setIsLoading] = useState(false)
  const [confirmLoading, setConfirmLoading] = useState(false)
  const [testResult, setTestResult] = useState({ code: -1, message: '' })
  const { token } = theme.useToken()
  const testRef = useRef(null)
  const scrollRef = useRef(null)
  const loadedRef = useRef(false)
  const scrolling = useScrolling(scrollRef)
  const [bottomShadow, setBottomShadow] = useState(false)
  const [topShadow, setTopShadow] = useState(false)

  const [form] = Form.useForm()
  const currentProvider = Form.useWatch('provider', form)
  const catalogBackend = Form.useWatch('catalog-backend', { form, preserve: true })
  const authType = Form.useWatch('authentication.type', { form, preserve: true })
  const values = Form.useWatch([], form)
  const dispatch = useAppDispatch()
  const isShowTestConnect = ['fileset', 'model'].includes(catalogType) || currentProvider === 'lakehouse-generic'

  const defaultValues = {
    name: '',
    type: catalogType,
    provider: '',
    comment: '',
    properties: []
  }

  const getDialogDescription = () => {
    if (editCatalog) {
      return `Edit the catalog ${editCatalog}.`
    }

    if (step === 0 && !['model', 'fileset'].includes(catalogType)) {
      return 'Select a data source provider to create the data catalog.'
    }

    return 'Fill-in the information into the following fields to create a data catalog.'
  }

  useResetFormOnCloseModal(
    {
      form,
      open
    },
    setCurrentStep
  )

  const isHidden = prop => {
    const { parentField, hide, required, key } = prop
    switch (parentField) {
      case 'catalog-backend':
        return catalogBackend && hide && hide.includes(catalogBackend)
      case 'authentication.type':
        return !authType || (hide && hide.includes(authType))
      default:
        return !(!editCatalog || ['region', 'location'].includes(key) || required)
    }
  }

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
    if (open && editCatalog && metalake && !loadedRef.current) {
      loadedRef.current = true

      const initLoad = async () => {
        setIsLoading(true)
        try {
          if (!['fileset', 'model'].includes(catalogType)) {
            setCurrentStep(1)
          }

          const { payload } = await dispatch(getCatalogDetails({ metalake, catalog: editCatalog }))
          const catalog = payload?.catalog || {}
          setCacheData(catalog)
          form.setFieldValue('name', catalog.name)
          form.setFieldValue('provider', catalog.provider)
          form.setFieldValue('comment', catalog.comment)
          let index = 0
          Object.entries(catalog.properties || {}).forEach(([key, value]) => {
            if (providerBase[catalog.provider]?.defaultProps?.find(item => item.key === key)) {
              form.setFieldValue(`${key}`, value)
            } else {
              form.setFieldValue(['properties', index, 'key'], key)
              form.setFieldValue(['properties', index, 'value'], value)
              const disabled = rangerDefaultProps.find(item => item.key === key)?.disabled
              form.setFieldValue(['properties', index, 'disabled'], disabled)
              const select = rangerDefaultProps.find(item => item.key === key)?.select
              form.setFieldValue(['properties', index, 'select'], select)
              index++
            }
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
  }, [open, editCatalog, metalake])

  useEffect(() => {
    switch (catalogType) {
      case 'relational':
        setProviders(relationalProviders)
        break
      case 'messaging':
        setProviders(messagingProviders)
        break
      case 'fileset':
        form.setFieldValue('provider', 'hadoop')
      default:
        return
    }
  }, [catalogType])

  useEffect(() => {
    if (open && !editCatalog && currentProvider === 'hadoop') {
      const defaultOptionalProps = entityDefaultProps[currentProvider] || []
      if (defaultOptionalProps.length) {
        defaultOptionalProps.forEach((prop, index) => {
          form.setFieldValue(['properties', index, 'key'], prop.key)
          form.setFieldValue(['properties', index, 'description'], prop.description)
          form.setFieldValue(['properties', index, 'select'], prop.select)
          form.setFieldValue(['properties', index, 'multiple'], prop.multiple)
          form.setFieldValue(['properties', index, 'value'], prop.defaultValue)
          if (prop.selectBefore?.length) {
            form.setFieldValue(['properties', index, 'selectBefore'], prop.selectBefore)
            form.setFieldValue(['properties', index, 'prefix'], prop.selectBefore[0])
          }
        })
      }
    }
  }, [open, editCatalog, currentProvider, entityDefaultProps])

  const nextStep = value => {
    setCurrentStep(value)
  }

  const handleNext = () => {
    if (step === 1) return
    nextStep(step + 1)
  }

  const handlePre = () => {
    if (step === 0) return
    nextStep(step - 1)
  }

  const getSubmitData = () => {
    const defaultProps =
      (catalogType === 'model'
        ? providerBase['model'].defaultProps
        : providerBase[currentProvider]?.defaultProps?.filter(item => !isHidden(item))) || []

    return {
      name: values.name.trim(),
      type: catalogType,
      provider: values.provider,
      comment: values.comment,
      properties: [...values.properties, ...defaultProps].reduce((acc, item) => {
        if (item.key === 'location' || item.key.startsWith('location-')) {
          if (item.value) {
            acc[item.key] = item.prefix ? item.prefix + item.value : item.value
          }
        } else {
          acc[item.key] = values[item.key] || (item.value instanceof Array ? item.value.join(',') : item.value)
        }

        return acc
      }, {})
    }
  }

  const handleTestConnect = async () => {
    setTestResult({ code: -1, message: '' })
    form
      .validateFields()
      .then(async () => {
        setIsTestLoading(true)
        try {
          const submitData = getSubmitData()

          const {
            payload: { code, message }
          } = await dispatch(testCatalogConnection({ data: submitData, metalake }))
          setIsTestLoading(false)
          setTestResult({ code, message })
        } catch {
          setIsTestLoading(false)
        }
      })
      .catch(info => {
        console.error(info)
      })
  }

  useEffect(() => {
    if (testRef.current) {
      testRef.current.scrollIntoView({
        behavior: 'smooth',
        block: 'start',
        inline: 'start'
      })
    }
  }, [])

  const handleSubmit = e => {
    e.preventDefault()
    form
      .validateFields()
      .then(async () => {
        setConfirmLoading(true)
        const submitData = getSubmitData()
        if (editCatalog) {
          // update catalog
          const reqData = { updates: genUpdates(cacheData, submitData) }
          if (reqData.updates.length) {
            await dispatch(updateCatalog({ init, metalake, catalog: cacheData.name, data: reqData }))
          }
        } else {
          await dispatch(createCatalog({ data: submitData, metalake }))
        }
        setOpen(false)
        setConfirmLoading(false)
      })
      .catch(info => {
        console.error(info)
        setConfirmLoading(false)
        form.scrollToField(info?.errorFields?.[0]?.name?.[0])
      })
  }

  const handleCancel = () => {
    setOpen(false)
  }

  const renderStepIcons = iconType => {
    const calalogIcon = currentProvider ? (
      renderIcon({ type: catalogType, provider: currentProvider, small: true })
    ) : (
      <Icons.iconify icon='healthicons:provider-fst-outline' className='size-6' />
    )

    return (
      <div className='step-icon flex size-8 items-center justify-center rounded bg-gray-100 text-gray-400 hover:text-defaultPrimary step-icon-default-theme'>
        {iconType === 'provider' && calalogIcon}
        {iconType === 'configure' && <Icons.iconify icon='uil:setting' className='size-6' />}
      </div>
    )
  }

  const renderIcon = ({ type, provider, small = false }) => {
    const calalogIcon = checkCatalogIcon({ type: type, provider: provider })
    if (calalogIcon.startsWith('custom-icons-')) {
      switch (calalogIcon) {
        case 'custom-icons-hive':
          return <Icons.hive className={small ? 'size-6' : 'size-12'}></Icons.hive>
        case 'custom-icons-doris':
          return <Icons.doris className={small ? 'size-6' : 'size-12'}></Icons.doris>
        case 'custom-icons-paimon':
          return <Icons.paimon className={small ? 'size-6' : 'size-12'}></Icons.paimon>
        case 'custom-icons-hudi':
          return <Icons.hudi className={small ? 'size-6' : 'size-12'}></Icons.hudi>
        case 'custom-icons-oceanbase':
          return <Icons.oceanbase className={small ? 'size-6' : 'size-12'}></Icons.oceanbase>
        case 'custom-icons-starrocks':
          return <Icons.starrocks className={small ? 'size-6' : 'size-12'}></Icons.starrocks>
        case 'custom-icons-lakehouse':
          return <Icons.lakehouse className={small ? 'size-6' : 'size-12'}></Icons.lakehouse>
      }
    } else {
      return <Icons.iconify icon={calalogIcon} className={small ? 'size-6' : 'size-12'} />
    }
  }

  const renderProvider = (provider, idx) => {
    return (
      <div
        key={idx}
        data-refer={`catalog-provider-${provider.value}`}
        className={cn('provider-card flex items-center justify-between', {
          'actived-default': provider.value === currentProvider,
          disabled: editCatalog
        })}
        onClick={() => handleSelectProvider(provider.value)}
      >
        <div className='flex space-x-3'>
          <div className='flex size-16 items-center justify-center rounded bg-gray-100 text-gray-400 hover:text-defaultPrimary'>
            {renderIcon({ type: catalogType, provider: provider.value })}
          </div>
          <div className='default-theme-text'>
            <Paragraph className={cn('text-base', { '!text-white': provider.value === currentProvider })}>
              {provider.label}
            </Paragraph>
            <Paragraph
              type='secondary'
              className={cn('text-sm', { '!text-white': provider.value === currentProvider })}
            >
              {provider.description}
            </Paragraph>
          </div>
        </div>
        {provider.value === currentProvider ? (
          <Icons.CircleCheckBig className='provider-radio size-4 text-white default-theme-radio' />
        ) : (
          <Icons.Circle className='provider-radio size-4 text-gray-400 default-theme-radio' />
        )}
      </div>
    )
  }

  const handleSelectProvider = provider => {
    if (editCatalog) return
    setTestResult({ code: -1, message: '' })
    form.resetFields()
    form.setFieldValue('provider', provider)
    if (provider === 'lakehouse-paimon') {
      form.setFieldValue('catalog-backend', 'filesystem')
    } else if (provider === 'lakehouse-iceberg') {
      form.setFieldValue('catalog-backend', 'hive')
    } else if (provider === 'lakehouse-hudi') {
      form.setFieldValue('catalog-backend', 'hms')
    } else {
      form.setFieldValue('catalog-backend', '')
    }
  }

  const filter = (inputValue, path) =>
    path.some(option => option.label.toLowerCase().indexOf(inputValue.toLowerCase()) > -1)

  return (
    <>
      <Modal
        title={
          !editCatalog
            ? `Create Catalog - ${catalogType.charAt(0).toUpperCase() + catalogType.slice(1)}`
            : `Edit Catalog - ${catalogType.charAt(0).toUpperCase() + catalogType.slice(1)}`
        }
        open={open}
        onOk={handleSubmit}
        width={1000}
        maskClosable={false}
        keyboard={false}
        onCancel={handleCancel}
        footer={null}
      >
        <Paragraph type='secondary'>{getDialogDescription()}</Paragraph>
        <Flex gap='middle' align='start'>
          <Steps
            current={step}
            direction='vertical'
            className='w-1/5 grow-0'
            items={
              ['model', 'fileset'].includes(catalogType)
                ? [
                    {
                      title: 'Configure',
                      icon: renderStepIcons('configure'),
                      description: 'Catalog Configure'
                    }
                  ]
                : [
                    {
                      title: 'Provider',
                      icon: renderStepIcons('provider'),
                      description: providerBase[currentProvider]?.label || 'Select Provider'
                    },
                    {
                      title: 'Configure',
                      icon: renderStepIcons('configure'),
                      description: 'Catalog Configure'
                    }
                  ]
            }
          />
          <div
            className={cn('relative w-4/5 grow', {
              'after:pointer-events-none after:absolute after:bottom-[10px] after:left-0 after:right-0 after:h-10 after:shadow-[0px_-10px_8px_-8px_rgba(5,5,5,0.1)]':
                topShadow,
              'before:pointer-events-none before:absolute before:-top-10 before:left-0 before:right-0 before:h-10 before:z-10 before:shadow-[0px_10px_8px_-8px_rgba(5,5,5,0.1)]':
                bottomShadow
            })}
          >
            <div className='overflow-auto' style={{ maxHeight: `${dialogContentMaxHeigth}px` }} ref={scrollRef}>
              <Spin spinning={isLoading}>
                <Form
                  form={form}
                  initialValues={defaultValues}
                  layout='vertical'
                  name='catalogForm'
                  validateMessages={validateMessages}
                >
                  <Form.Item
                    name='provider'
                    rules={[{ required: catalogType !== 'model' }]}
                    className={
                      step === 1 || (step === 0 && ['fileset', 'model'].includes(catalogType)) ? 'hidden' : 'block'
                    }
                  >
                    <div>
                      <Paragraph className='text-base'>Select Provider</Paragraph>
                      {providers.map((provider, idx) => renderProvider(provider, idx))}
                    </div>
                  </Form.Item>
                  <div className={step === 0 && !['fileset', 'model'].includes(catalogType) ? 'hidden' : 'mb-4 block'}>
                    <Form.Item
                      name='name'
                      label='Catalog Name'
                      rules={[{ required: true }, { type: 'string', max: 64 }, { pattern: new RegExp(nameRegex) }]}
                      data-refer='catalog-name-field'
                    >
                      <Input placeholder={mismatchName} disabled={!init} />
                    </Form.Item>
                    {providerBase[catalogType === 'model' ? 'model' : currentProvider]?.defaultProps
                      ?.filter(p => !isHidden(p))
                      .map((prop, idx) => {
                        return (
                          <Form.Item
                            name={prop.key}
                            label={prop.label}
                            key={idx}
                            rules={[{ required: prop.required }]}
                            initialValue={prop.value}
                          >
                            {prop.select ? (
                              <Select
                                data-refer={`catalog-props-${prop.key}`}
                                disabled={editCatalog && prop.required}
                                placeholder={prop.description ? prop.description : ''}
                              >
                                {prop.select?.map(item => (
                                  <Select.Option value={item} key={item}>
                                    {item}
                                  </Select.Option>
                                ))}
                              </Select>
                            ) : (
                              <Input
                                data-refer={`catalog-props-${prop.key}`}
                                placeholder={prop.description ? prop.description : ''}
                                disabled={prop.disabled}
                                type={prop.key === 'jdbc-password' ? 'password' : 'text'}
                              />
                            )}
                          </Form.Item>
                        )
                      })}
                    <Form.Item name='comment' label='Comment' data-refer='catalog-comment-field'>
                      <TextArea />
                    </Form.Item>
                    <Form.Item label='Properties'>
                      <Form.List name='properties'>
                        {(fields, subOpt) => (
                          <RenderPropertiesFormItem
                            fields={fields}
                            subOpt={subOpt}
                            form={form}
                            isEdit={!!editCatalog}
                            theme={theme}
                            isDisable={false}
                            provider={currentProvider}
                          />
                        )}
                      </Form.List>
                    </Form.Item>
                    {!editCatalog && !isShowTestConnect && (
                      <Tooltip
                        title='Test the connection to the source system with the given information.'
                        placement='right'
                      >
                        <Button
                          type='primary'
                          onClick={handleTestConnect}
                          loading={isTestLoading}
                          icon={<InfoCircleOutlined />}
                          iconPosition='end'
                        >
                          Test Connection
                        </Button>
                      </Tooltip>
                    )}
                    {testResult.code !== -1 && !isShowTestConnect && (
                      <p
                        ref={testRef}
                        style={{ color: testResult.code === 0 ? token.colorSuccessText : token.colorWarningText }}
                      >
                        {testResult.code === 0 ? <CheckCircleFilled /> : <ExclamationCircleFilled />}{' '}
                        {testResult.code === 0 ? 'Test Success' : testResult.message}
                      </p>
                    )}
                  </div>
                </Form>
              </Spin>
            </div>
            <div className='mt-4 flex justify-between'>
              <div className='grow'>
                {(step === 1 || (step === 0 && ['fileset', 'model'].includes(catalogType))) && (
                  <div className='flex justify-between'>
                    {editCatalog || ['fileset', 'model'].includes(catalogType) ? (
                      <div></div>
                    ) : (
                      <Button onClick={handlePre} icon={<LeftOutlined />}>
                        Previous
                      </Button>
                    )}
                    <Button
                      type='primary'
                      onClick={handleSubmit}
                      loading={confirmLoading}
                      data-refer='handle-submit-catalog'
                    >
                      Submit
                    </Button>
                  </div>
                )}
              </div>
              {step === 0 && !['fileset', 'model'].includes(catalogType) && (
                <Button
                  type='primary'
                  onClick={handleNext}
                  icon={<RightOutlined />}
                  iconPosition='end'
                  disabled={!currentProvider}
                  data-refer='handle-next-catalog'
                >
                  Next
                </Button>
              )}
            </div>
          </div>
        </Flex>
      </Modal>
    </>
  )
}
