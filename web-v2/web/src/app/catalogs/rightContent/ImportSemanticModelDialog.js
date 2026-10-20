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
import { Alert, Button, Modal, Space, Typography, Upload, message } from 'antd'
import Link from 'next/link'
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
  const [errorStage, setErrorStage] = useState('file')
  const [selection, setSelection] = useState(null)
  const [reading, setReading] = useState(false)
  const selectionId = useRef(0)
  const errorRef = useRef(null)
  useEffect(() => {
    if (error) errorRef.current?.scrollIntoView({ block: 'nearest' })
  }, [error])
  useEffect(
    () => () => {
      selectionId.current++
    },
    []
  )

  const selectFile = async selected => {
    const id = ++selectionId.current
    setErrorStage('file')
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
    setErrorStage('import')
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
      if (!response?.semanticModel?.name) throw new Error('Import returned an empty response.')
      await dispatch(fetchSemanticModels({ metalake, catalog, schema, init: true }))

      const params = new URLSearchParams({
        metalake,
        catalog,
        catalogType: 'relational',
        schema,
        semanticModel: response.semanticModel.name
      })
      message.success({
        key: 'semantic-model-import',
        content: (
          <Space wrap>
            <span>
              Semantic Model <strong>{response.semanticModel.name}</strong> imported
            </span>
            <Link href={`/catalogs?${params.toString()}`} onClick={() => message.destroy('semantic-model-import')}>
              View model
            </Link>
          </Space>
        ),
        duration: 10
      })
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
        <div>
          <Typography.Text type='secondary' className='block'>
            Destination schema
          </Typography.Text>
          <Typography.Text strong>
            {metalake} / {catalog} / {schema}
          </Typography.Text>
        </div>
        {selection && (
          <div>
            <Typography.Text type='secondary' className='block'>
              OSSIE version
            </Typography.Text>
            <Typography.Text strong>{selection.preview.version}</Typography.Text>
          </div>
        )}
        <Upload
          accept='.yaml,.yml,.json'
          maxCount={1}
          showUploadList={false}
          disabled={loading}
          fileList={file ? [file] : []}
          beforeUpload={selected => {
            setErrorStage('file')
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
        >
          <Button aria-label={file ? 'Replace file' : 'Select file'} icon={<UploadOutlined />} disabled={loading}>
            {file ? 'Replace file' : 'Select file'}
          </Button>
        </Upload>
        {file && (
          <div className='flex items-center gap-3'>
            <div className='min-w-0 flex-1'>
              <Typography.Text className='block break-all'>{file.name}</Typography.Text>
              <Typography.Text type='secondary'>
                {file.name.toLowerCase().endsWith('.json') ? 'JSON' : 'YAML'} · {file.size.toLocaleString()} bytes
              </Typography.Text>
            </div>
            <Button
              disabled={loading}
              onClick={() => {
                selectionId.current++
                setSelection(null)
                setReading(false)
                setFile(null)
                setError('')
              }}
            >
              Remove file
            </Button>
          </div>
        )}
        {reading && <Typography.Text>Reading document...</Typography.Text>}
        {selection && (
          <>
            <section aria-label='Import preview' data-refer='import-preview'>
              <Typography.Text type='secondary' className='block'>
                Import preview
              </Typography.Text>
              <Typography.Title level={5} className='break-words' style={{ marginTop: 4 }}>
                {selection.preview.name}
              </Typography.Title>
              <dl className='m-0 grid grid-cols-4 gap-3'>
                {['datasets', 'fields', 'relationships', 'metrics'].map(key => (
                  <div key={key}>
                    <dt className='text-xs text-textSecondary'>{key[0].toUpperCase() + key.slice(1)}</dt>
                    <dd className='m-0 text-lg font-medium'>{selection.preview[key]}</dd>
                  </div>
                ))}
              </dl>
            </section>
            <Typography.Text type='secondary'>
              The server will validate the complete document and its sources on import.
            </Typography.Text>
          </>
        )}
        {error && (
          <div ref={errorRef} className='space-y-2'>
            <Alert
              type='error'
              showIcon
              message={errorStage === 'file' ? 'File preview failed' : 'Import failed'}
              description={<span className='whitespace-pre-wrap'>{error}</span>}
            />
            <Typography.Text type='secondary'>
              {errorStage === 'file'
                ? 'No import was attempted. Replace or remove the file to correct it.'
                : 'The selected file is retained. Retry the import or replace the file.'}
            </Typography.Text>
          </div>
        )}
      </Space>
    </Modal>
  )
}
