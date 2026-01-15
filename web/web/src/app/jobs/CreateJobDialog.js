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
import dynamic from 'next/dynamic'
import { ArrowRightOutlined, PlusOutlined } from '@ant-design/icons'
import { Button, Divider, Flex, Form, Input, Modal, Select, Spin, Typography } from 'antd'
import Icons from '@/components/Icons'
import { validateMessages } from '@/config'
import { getJobParamValues } from '@/lib/utils'
import { cn } from '@/lib/utils/tailwind'
import RegisterJobTemplateDialog from './RegisterJobTemplateDialog'
import { useResetFormOnCloseModal } from '@/lib/hooks/use-reset'
import { useAppDispatch } from '@/lib/hooks/useStore'
import { fetchJobTemplates, getJobTemplate, createJob } from '@/lib/store/jobs'

const { Paragraph } = Typography

const defaultValues = {
  jobTemplateName: '',
  jobConf: []
}

export default function CreateJobDialog({ ...props }) {
  const { open, setOpen, metalake, router, jobTemplateNames } = props
  const [confirmLoading, setConfirmLoading] = useState(false)
  const [isLoading, setIsLoading] = useState(false)
  const [jobTemplateDetail, setJobTemplateDetail] = useState(null)
  const [form] = Form.useForm()
  const values = Form.useWatch([], form)
  const currentJobTemplate = Form.useWatch('jobTemplateName', form)
  const jobConfValues = Form?.useWatch('jobConf', form)
  const [openTemplate, setOpenTemplate] = useState(false)
  const dispatch = useAppDispatch()

  useResetFormOnCloseModal({
    form,
    open
  })

  useEffect(() => {
    open && dispatch(fetchJobTemplates({ metalake }))
  }, [dispatch, metalake, open])

  useEffect(() => {
    if (open && jobTemplateNames && jobTemplateNames.length > 0 && !currentJobTemplate) {
      form.setFieldValue('jobTemplateName', jobTemplateNames[0])
    }
  }, [open, jobTemplateNames, currentJobTemplate])

  const updateOnChange = (index, newValue) => {
    const changedKey = jobConfValues?.[index]?.confKey
    if (!changedKey) return

    const updatedJobConf = (jobConfValues || []).map(conf =>
      conf?.confKey === changedKey ? { ...conf, value: newValue } : conf
    )

    form.setFieldsValue({ jobConf: updatedJobConf })
  }

  const getPlaceholderEntries = templateDetail => {
    if (!templateDetail) return []

    const jobType = String(templateDetail.jobType || '').toLowerCase()
    const stringValues = []

    const pushString = value => {
      if (typeof value === 'string' && value) {
        stringValues.push(value)
      }
    }

    const pushArray = values => {
      ;(values || []).forEach(val => pushString(val))
    }

    const pushObjectValues = obj => {
      Object.values(obj || {}).forEach(val => pushString(val))
    }

    pushString(templateDetail.jobType)
    pushString(templateDetail.comment)
    pushString(templateDetail.executable)
    pushArray(templateDetail.arguments)
    pushObjectValues(templateDetail.environments)
    pushObjectValues(templateDetail.customFields)

    if (jobType === 'spark') {
      pushString(templateDetail.className)
      pushArray(templateDetail.jars)
      pushArray(templateDetail.files)
      pushArray(templateDetail.archives)
      pushObjectValues(templateDetail.configs)
    }

    if (jobType === 'shell') {
      pushArray(templateDetail.scripts)
    }

    const placeholders = stringValues.flatMap(template => getJobParamValues(template) || []).filter(Boolean)
    const uniquePlaceholders = Array.from(new Set(placeholders))

    return uniquePlaceholders.map(name => ({
      key: name,
      value: '',
      confKey: name,
      template: `{{${name}}}`
    }))
  }

  const getPlaceholderValueMap = () =>
    (jobConfValues || []).reduce((acc, item) => {
      if (item?.confKey) {
        acc[item.confKey] = item.value || ''
      }

      return acc
    }, {})

  const renderTemplateValue = template => {
    if (!template) return ''
    const valueMap = getPlaceholderValueMap()

    return template.replace(/\{\{([^}]+)\}\}/g, (_, key) => valueMap[key] || `{{${key}}}`)
  }

  useEffect(() => {
    const fetchJobTemplateDetails = async () => {
      setIsLoading(true)
      try {
        const { payload: jobTemplate } = await dispatch(getJobTemplate({ metalake, jobTemplate: currentJobTemplate }))
        setIsLoading(false)
        setJobTemplateDetail(jobTemplate)
        const placeholderEntries = getPlaceholderEntries(jobTemplate)
        form.setFieldsValue({ jobConf: placeholderEntries })
      } catch (error) {
        console.log(error)
        setIsLoading(false)
      }
    }
    if (currentJobTemplate) {
      fetchJobTemplateDetails()
    }
  }, [currentJobTemplate])

  const handleSubmit = e => {
    e.preventDefault()
    form
      .validateFields()
      .then(async () => {
        setConfirmLoading(true)

        const submitData = {
          jobTemplateName: values.jobTemplateName,
          jobConf: values.jobConf?.reduce((acc, item) => {
            if (item.confKey && item.value) {
              acc[item.confKey] = item.value
            }

            return acc
          }, {})
        }
        await dispatch(createJob({ metalake, data: submitData }))
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
        title='Run Job'
        open={open}
        onOk={handleSubmit}
        okText='Submit'
        maskClosable={false}
        keyboard={false}
        width={1000}
        confirmLoading={confirmLoading}
        onCancel={handleCancel}
      >
        <Paragraph type='secondary'>Create a new job by the template.</Paragraph>
        <Spin spinning={isLoading}>
          <Form
            form={form}
            initialValues={defaultValues}
            layout='vertical'
            name='policyForm'
            validateMessages={validateMessages}
          >
            <Form.Item
              name='jobTemplateName'
              label='Template Name'
              rules={[{ required: true }]}
              messageVariables={{ label: 'template name' }}
            >
              <Select
                popupRender={menu => (
                  <>
                    {menu}
                    <Divider style={{ margin: '8px 0' }} />
                    <div className='flex justify-between'>
                      <Button
                        type='text'
                        icon={<PlusOutlined />}
                        onClick={() => {
                          setOpenTemplate(true)
                        }}
                      >
                        Register Job Template
                      </Button>
                      <Button
                        type='text'
                        icon={<ArrowRightOutlined />}
                        onClick={() => {
                          router.push(`/jobTemplates?metalake=${metalake}`)
                        }}
                      >
                        Go to Job Templates
                      </Button>
                    </div>
                  </>
                )}
                options={jobTemplateNames.map(template => ({ label: template, value: template }))}
              />
            </Form.Item>
            <div className='relative rounded border border-gray-200'>
              <div className='pointer-events-none absolute inset-y-0 left-1/2 w-px -translate-x-1/2 bg-gray-200' />
              <div className='pointer-events-none absolute left-1/2 top-1/2 flex size-6 -translate-x-1/2 -translate-y-1/2 items-center justify-center rounded-full border border-gray-200 bg-white text-gray-500'>
                <Icons.iconify icon='mdi:arrow-left' className='size-4' />
              </div>
              <div className='grid grid-cols-2 gap-6 p-4'>
                <div className='space-y-2 max-h-[320px] overflow-y-auto pr-2'>
                  <Paragraph className='text-sm !mb-0'>Template Parameters</Paragraph>
                  <div>
                    <Paragraph className='text-[12px] mb-1'>Basic Fields</Paragraph>
                    <div className='pl-4 space-y-1'>
                      <div className='text-[12px] text-gray-700'>
                        <span className='text-gray-400'>Job Type:</span> {jobTemplateDetail?.jobType || '-'}
                      </div>
                      <div className='text-[12px] text-gray-700'>
                        <span className='text-gray-400'>Comment:</span> {jobTemplateDetail?.comment || '-'}
                      </div>
                      <div className='text-[12px] text-gray-700'>
                        <span className='text-gray-400'>Executable:</span>{' '}
                        {renderTemplateValue(jobTemplateDetail?.executable || '') || '-'}
                      </div>
                      {jobTemplateDetail?.jobType === 'spark' && (
                        <div className='text-[12px] text-gray-700'>
                          <span className='text-gray-400'>Class Name:</span>{' '}
                          {renderTemplateValue(jobTemplateDetail?.className || '') || '-'}
                        </div>
                      )}
                    </div>
                  </div>
                  <div>
                    <Paragraph className='text-[12px] mb-1'>Arguments</Paragraph>
                    <div className='pl-4'>
                      {(jobTemplateDetail?.arguments || []).length > 0 ? (
                        <div className='space-y-1'>
                          {(jobTemplateDetail?.arguments || []).map((arg, index) => (
                            <div key={`arg-${index}`} className='text-sm text-gray-700'>
                              {renderTemplateValue(arg)}
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div className='text-sm text-gray-400'>No arguments</div>
                      )}
                    </div>
                  </div>
                  <div>
                    <Paragraph className='text-[12px] mb-1'>Environment Variables</Paragraph>
                    <div className='pl-4'>
                      {Object.keys(jobTemplateDetail?.environments || {}).length > 0 ? (
                        <div className='space-y-1'>
                          {Object.entries(jobTemplateDetail?.environments || {}).map(([key, value]) => (
                            <div key={`env-${key}`} className='text-sm text-gray-700'>
                              <span className='text-gray-400'>{key}:</span> {renderTemplateValue(value)}
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div className='text-[12px] text-gray-400'>No environment variables</div>
                      )}
                    </div>
                  </div>
                  <div>
                    <Paragraph className='text-[12px] mb-1'>Custom Fields</Paragraph>
                    <div className='pl-4'>
                      {Object.keys(jobTemplateDetail?.customFields || {}).length > 0 ? (
                        <div className='space-y-1'>
                          {Object.entries(jobTemplateDetail?.customFields || {}).map(([key, value]) => (
                            <div key={`custom-${key}`} className='text-sm text-gray-700'>
                              <span className='text-gray-400'>{key}:</span> {renderTemplateValue(value)}
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div className='text-[12px] text-gray-400'>No custom fields</div>
                      )}
                    </div>
                  </div>
                  {jobTemplateDetail?.jobType === 'spark' && (
                    <>
                      <div>
                        <Paragraph className='text-[12px] mb-1'>Jars</Paragraph>
                        <div className='pl-4'>
                          {(jobTemplateDetail?.jars || []).length > 0 ? (
                            <div className='space-y-1'>
                              {(jobTemplateDetail?.jars || []).map((item, index) => (
                                <div key={`jar-${index}`} className='text-sm text-gray-700'>
                                  {renderTemplateValue(item)}
                                </div>
                              ))}
                            </div>
                          ) : (
                            <div className='text-[12px] text-gray-400'>No jars</div>
                          )}
                        </div>
                      </div>
                      <div>
                        <Paragraph className='text-[12px] mb-1'>Files</Paragraph>
                        <div className='pl-4'>
                          {(jobTemplateDetail?.files || []).length > 0 ? (
                            <div className='space-y-1'>
                              {(jobTemplateDetail?.files || []).map((item, index) => (
                                <div key={`file-${index}`} className='text-sm text-gray-700'>
                                  {renderTemplateValue(item)}
                                </div>
                              ))}
                            </div>
                          ) : (
                            <div className='text-[12px] text-gray-400'>No files</div>
                          )}
                        </div>
                      </div>
                      <div>
                        <Paragraph className='text-[12px] mb-1'>Archives</Paragraph>
                        <div className='pl-4'>
                          {(jobTemplateDetail?.archives || []).length > 0 ? (
                            <div className='space-y-1'>
                              {(jobTemplateDetail?.archives || []).map((item, index) => (
                                <div key={`archive-${index}`} className='text-sm text-gray-700'>
                                  {renderTemplateValue(item)}
                                </div>
                              ))}
                            </div>
                          ) : (
                            <div className='text-[12px] text-gray-400'>No archives</div>
                          )}
                        </div>
                      </div>
                      <div>
                        <Paragraph className='text-[12px] mb-1'>Configs</Paragraph>
                        <div className='pl-4'>
                          {Object.keys(jobTemplateDetail?.configs || {}).length > 0 ? (
                            <div className='space-y-1'>
                              {Object.entries(jobTemplateDetail?.configs || {}).map(([key, value]) => (
                                <div key={`config-${key}`} className='text-[12px] text-gray-700'>
                                  <span className='text-gray-400'>{key}:</span> {renderTemplateValue(value)}
                                </div>
                              ))}
                            </div>
                          ) : (
                            <div className='text-[12px] text-gray-400'>No configs</div>
                          )}
                        </div>
                      </div>
                    </>
                  )}
                  {jobTemplateDetail?.jobType === 'shell' && (
                    <div>
                      <Paragraph className='text-[12px] mb-1'>Scripts</Paragraph>
                      <div className='pl-4'>
                        {(jobTemplateDetail?.scripts || []).length > 0 ? (
                          <div className='space-y-1'>
                            {(jobTemplateDetail?.scripts || []).map((item, index) => (
                              <div key={`script-${index}`} className='text-sm text-gray-700'>
                                {renderTemplateValue(item)}
                              </div>
                            ))}
                          </div>
                        ) : (
                          <div className='text-[12px] text-gray-400'>No scripts</div>
                        )}
                      </div>
                    </div>
                  )}
                </div>
                <div className='pl-2 max-h-[320px] overflow-y-auto pr-2'>
                  <Form.Item label='Job Configuration'>
                    <div className='flex flex-col gap-2'>
                      <Form.List name='jobConf'>
                        {fields => (
                          <>
                            {fields.map(({ key, name, ...restField }) => (
                              <Form.Item label={null} className='align-items-center mb-0' key={key}>
                                <Flex gap='small' align='start' key={key}>
                                  <Form.Item
                                    {...restField}
                                    name={[name, 'key']}
                                    rules={[{ required: true, message: 'Please enter the job config key!' }]}
                                    className='mb-0 w-full grow'
                                  >
                                    <Input disabled placeholder='Job Config Key' />
                                  </Form.Item>
                                  <Form.Item
                                    {...restField}
                                    name={[name, 'value']}
                                    rules={[{ required: true, message: 'Please enter the job config value!' }]}
                                    className='mb-0 w-full grow'
                                  >
                                    <Input
                                      placeholder={
                                        form.getFieldValue(['jobConf', name, 'template']) || 'Job Config Value'
                                      }
                                      onChange={e => updateOnChange(name, e.target.value)}
                                    />
                                  </Form.Item>
                                </Flex>
                              </Form.Item>
                            ))}
                          </>
                        )}
                      </Form.List>
                    </div>
                  </Form.Item>
                </div>
              </div>
            </div>
          </Form>
        </Spin>
        {openTemplate && (
          <RegisterJobTemplateDialog open={openTemplate} setOpen={setOpenTemplate} metalake={metalake} />
        )}
      </Modal>
    </>
  )
}
