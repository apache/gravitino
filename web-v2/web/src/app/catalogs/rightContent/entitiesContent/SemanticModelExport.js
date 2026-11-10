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
import { Alert, Button, Dropdown, Space } from 'antd'
import { DownloadOutlined } from '@ant-design/icons'
import { exportSemanticModelApi } from '@/lib/api/semanticModels'
import { semanticModelErrorMessage } from '@/lib/utils/semanticModel'

export default function SemanticModelExport({ metalake, catalog, schema, semanticModel, disabled }) {
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const exportDocument = async ({ key: format }) => {
    setLoading(true)
    setError('')
    try {
      const content = await exportSemanticModelApi({ metalake, catalog, schema, semanticModel, format })
      if (typeof content !== 'string' || !content.trim()) throw new Error('Export returned an empty document.')
      const blob = new Blob([content], { type: format === 'json' ? 'application/json' : 'application/yaml' })
      const url = URL.createObjectURL(blob)
      const link = document.createElement('a')
      link.href = url
      link.download = `${semanticModel.replace(/[^A-Za-z0-9._-]/g, '_')}.ossie.${format}`
      document.body.appendChild(link)
      link.click()
      link.remove()
      setTimeout(() => URL.revokeObjectURL(url), 0)
    } catch (err) {
      setError(semanticModelErrorMessage(err, 'Failed to export the OSSIE document.'))
    } finally {
      setLoading(false)
    }
  }

  return (
    <Space direction='vertical' align='end' className='mb-3 flex'>
      <Dropdown
        trigger={['click']}
        disabled={disabled || loading}
        menu={{
          items: [
            { key: 'yaml', label: 'Download YAML' },
            { key: 'json', label: 'Download JSON' }
          ],
          onClick: exportDocument
        }}
      >
        <Button aria-label='Export OSSIE' icon={<DownloadOutlined />} loading={loading} disabled={disabled}>
          Export OSSIE
        </Button>
      </Dropdown>
      {error && <Alert type='error' showIcon message='Export failed' description={error} />}
    </Space>
  )
}
