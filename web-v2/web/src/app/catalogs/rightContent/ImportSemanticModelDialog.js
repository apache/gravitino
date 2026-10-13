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

import { useState } from 'react'
import { Alert, Button, Modal, Space, Typography, Upload, message } from 'antd'
import { UploadOutlined } from '@ant-design/icons'
import { importSemanticModelApi } from '@/lib/api/semanticModels'
import { useAppDispatch } from '@/lib/hooks/useStore'
import { fetchSemanticModels } from '@/lib/store/metalakes'
import { semanticModelErrorMessage } from '@/lib/utils/semanticModel'

export default function ImportSemanticModelDialog({ open, setOpen, metalake, catalog, schema }) {
  const dispatch = useAppDispatch()
  const [file, setFile] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const importDocument = async () => {
    if (!file || loading) return
    setLoading(true)
    setError('')
    try {
      const document = await file.text()
      if (!document.trim()) throw new Error('The selected file is empty.')

      const response = await importSemanticModelApi({
        metalake,
        catalog,
        schema,
        document,
        format: file.name.toLowerCase().endsWith('.json') ? 'json' : 'yaml'
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
      open={open}
      onCancel={() => setOpen(false)}
      onOk={importDocument}
      okText='Import'
      confirmLoading={loading}
      okButtonProps={{ disabled: !file, 'data-refer': 'import-semantic-model-submit' }}
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
              setFile(null)
              setError('Select a .yaml, .yml, or .json file.')

              return Upload.LIST_IGNORE
            }
            setFile(selected)

            return false
          }}
          onRemove={() => {
            setFile(null)
            setError('')
          }}
        >
          <Button icon={<UploadOutlined />} disabled={loading}>
            Select file
          </Button>
        </Upload>
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
