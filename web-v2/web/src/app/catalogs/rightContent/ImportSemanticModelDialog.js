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

import { useEffect, useRef, useState } from 'react'
import { Alert, Button, Descriptions, Modal, Space, Typography, Upload, message } from 'antd'
import { UploadOutlined } from '@ant-design/icons'
import { importSemanticModelApi } from '@/lib/api/semanticModels'
import { useAppDispatch } from '@/lib/hooks/useStore'
import { fetchSemanticModels } from '@/lib/store/metalakes'
import { parseOssiePreview } from '@/lib/utils/ossiePreview'
import { semanticModelErrorMessage } from '@/lib/utils/semanticModel'

export default function ImportSemanticModelDialog({ open, setOpen, metalake, catalog, schema }) {
  const dispatch = useAppDispatch()
  const [file, setFile] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [selection, setSelection] = useState(null)
  const [reading, setReading] = useState(false)
  const selectionId = useRef(0)
  useEffect(
    () => () => {
      selectionId.current++
    },
    []
  )

  const selectFile = async selected => {
    const id = ++selectionId.current
    setError('')
    setSelection(null)
    setFile(selected)
    setReading(true)
    try {
      const document = await selected.text()
      const format = selected.name.toLowerCase().endsWith('.json') ? 'json' : 'yaml'
      const preview = parseOssiePreview(document, format)
      if (id === selectionId.current) setSelection({ document, format, preview })
    } catch (err) {
      if (id === selectionId.current) setError(semanticModelErrorMessage(err, 'Unable to read the OSSIE document.'))
    } finally {
      if (id === selectionId.current) setReading(false)
    }
  }

  const importDocument = async () => {
    if (!selection || reading || loading) return
    setLoading(true)
    setError('')
    try {
      const { document, format } = selection

      const response = await importSemanticModelApi({
        metalake,
        catalog,
        schema,
        document,
        format
      })
      if (!response?.semanticModel) throw new Error('Import returned an empty response.')
      await dispatch(fetchSemanticModels({ metalake, catalog, schema, init: true }))
      message.success('Semantic Model imported')
      setOpen(false)
    } catch (err) {
      setError(semanticModelErrorMessage(err, 'Failed to import the OSSIE document.'))
    } finally {
      setLoading(false)
    }
  }

  return (
    <Modal
      title='Import OSSIE document'
      style={{ top: 24 }}
      styles={{ body: { maxHeight: 'calc(100vh - 160px)', overflowY: 'auto' } }}
      open={open}
      onCancel={() => setOpen(false)}
      onOk={importDocument}
      okText='Import'
      confirmLoading={loading}
      okButtonProps={{
        disabled: !selection || reading,
        'data-refer': 'import-semantic-model-submit',
        'aria-label': 'Import'
      }}
      cancelButtonProps={{ disabled: loading }}
      closable={!loading}
      maskClosable={!loading}
      keyboard={!loading}
    >
      <Space direction='vertical' className='w-full' size='middle'>
        <Typography.Paragraph className='mb-0'>
          Select a standalone OSSIE YAML or JSON document to create a Semantic Model in this schema.
        </Typography.Paragraph>
        <Upload
          accept='.yaml,.yml,.json'
          maxCount={1}
          disabled={loading}
          fileList={file ? [file] : []}
          beforeUpload={selected => {
            setError('')
            if (!/\.(ya?ml|json)$/i.test(selected.name)) {
              selectionId.current++
              setSelection(null)
              setReading(false)
              setFile(null)
              setError('Select a .yaml, .yml, or .json file.')

              return Upload.LIST_IGNORE
            }
            selectFile(selected)

            return false
          }}
          onRemove={() => {
            selectionId.current++
            setSelection(null)
            setReading(false)
            setFile(null)
            setError('')
          }}
        >
          <Button icon={<UploadOutlined />} disabled={loading}>
            Select file
          </Button>
        </Upload>
        {reading && <Typography.Text>Reading document...</Typography.Text>}
        {selection && (
          <>
            <Descriptions bordered size='small' column={2} title='Import preview'>
              <Descriptions.Item label='Model' span={2}>
                {selection.preview.name}
              </Descriptions.Item>
              <Descriptions.Item label='Destination' span={2}>
                {metalake} / {catalog} / {schema}
              </Descriptions.Item>
              {['datasets', 'fields', 'relationships', 'metrics'].map(key => (
                <Descriptions.Item key={key} label={key[0].toUpperCase() + key.slice(1)}>
                  {selection.preview[key]}
                </Descriptions.Item>
              ))}
            </Descriptions>
            <Typography.Text type='secondary'>
              The server will validate the complete document and its sources on import.
            </Typography.Text>
          </>
        )}
        {error && (
          <Alert
            type='error'
            showIcon
            message='Import failed'
            description={<span className='whitespace-pre-wrap'>{error}</span>}
          />
        )}
      </Space>
    </Modal>
  )
}
