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

import fs from 'node:fs'
import path from 'node:path'

import { cleanupSVG, importDirectory, runSVGO } from '@iconify/tools'
import { getIconsCSS } from '@iconify/utils'

const SVGs = {
  dir: 'src/lib/icons/svg',
  prefix: 'custom-icons'
}

const target = path.join(__dirname, 'iconify-icons.css')

;(async () => {
  const icons = []

  const iconSet = await importDirectory(SVGs.dir, {
    prefix: SVGs.prefix
  })

  await iconSet.forEach(async name => {
    const svg = iconSet.toSVG(name)
    cleanupSVG(svg)
    runSVGO(svg)
    iconSet.fromSVG(name, svg)
  })

  icons.push(iconSet.export())

  await fs.promises.writeFile(
    target,
    `
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

${icons
  .map(iconSet =>
    getIconsCSS(iconSet, Object.keys(iconSet.icons), {
      iconSelector: '.{prefix}-{name}'
    })
  )
  .join('\n')}
  `,
    'utf8'
  )
})()
