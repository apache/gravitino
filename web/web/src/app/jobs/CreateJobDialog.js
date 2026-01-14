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
    const changedTemplate = jobConfValues[index].template
    if (!changedTemplate) return

    const updatedJobConf = [...jobConfValues]

    jobConfValues.forEach(index => {
      let hasChange = false
      updatedJobConf.forEach((conf, i) => {
        if (i !== index && conf?.template === changedTemplate) {
          updatedJobConf[i] = { ...conf, value: newValue }
          hasChange = true
        }
      })

      if (hasChange) {
        form.setFieldsValue({ jobConf: updatedJobConf })
      }
    })
  }

  useEffect(() => {
    const fetchJobTemplateDetails = async () => {
      setIsLoading(true)
      try {
        const { payload: jobTemplate } = await dispatch(getJobTemplate({ metalake, jobTemplate: currentJobTemplate }))
        setIsLoading(false)
        form.setFieldValue('jobConf', [])
        const { arguments: args, environments, customFields } = jobTemplate
        args.forEach((conf, index) => {
          form.setFieldValue(['jobConf', index], {
            key: getJobParamValues(conf)?.[0],
            value: '',
            confKey: getJobParamValues(conf)?.[0],
            template: conf
          })
        })
        Object.entries(environments).forEach(([key, value], index) => {
          form.setFieldValue(['jobConf', args.length + index], {
            key,
            value: '',
            confKey: getJobParamValues(value)?.[0],
            template: value
          })
        })
        Object.entries(customFields).forEach(([key, value], index) => {
          form.setFieldValue(['jobConf', args.length + Object.keys(environments).length + index], {
            key,
            value: '',
            confKey: getJobParamValues(value)?.[0],
            template: value
          })
        })
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
        width={800}
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
            <Form.Item name='jobTemplateName' label='Template Name' rules={[{ required: true }]}>
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
            <Form.Item label='Job Configuration'>
              <div className='flex flex-col gap-2'>
                <Form.List name='jobConf'>
                  {(fields, { add, remove }) => (
                    <>
                      {fields.map(({ key, name, ...restField }) => (
                        <Form.Item label={null} className='align-items-center mb-1' key={key}>
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
                                placeholder={form.getFieldValue(['jobConf', name, 'template']) || 'Job Config Value'}
                                onChange={e => updateOnChange(name, e.target.value)}
                              />
                            </Form.Item>
                            <Form.Item className='mb-0 grow-0'>
                              <Icons.Minus
                                className='size-8 cursor-pointer text-gray-400 hover:text-defaultPrimary'
                                onClick={() => remove(name)}
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
          </Form>
        </Spin>
        {openTemplate && (
          <RegisterJobTemplateDialog open={openTemplate} setOpen={setOpenTemplate} metalake={metalake} />
        )}
      </Modal>
    </>
  )
}
