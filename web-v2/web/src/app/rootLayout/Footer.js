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

import Link from 'next/link'

const Footer = props => {
  return (
    <footer className='sticky bottom-0 z-40 bg-slate-800 text-slate-100 py-4 text-center leading-4'>
      <div className='container flex items-center justify-between'>
        <span>
          ©{new Date().getFullYear()}
          <Link
            className={'no-underline hover:text-defaultPrimary ml-2'}
            target='_blank'
            href='https://gravitino.apache.org/'
            data-refer='footer-link-gravitino'
          >
            ASF
          </Link>
        </span>
        <span className='flex gap-2'>
          <Link
            className={'no-underline hover:text-defaultPrimary'}
            target='_blank'
            href='https://github.com/apache/gravitino/blob/main/LICENSE'
            data-refer='footer-link-license'
          >
            License
          </Link>
          <Link
            className={'no-underline hover:text-defaultPrimary'}
            target='_blank'
            href='https://gravitino.apache.org/docs/latest/'
            data-refer='footer-link-docs'
          >
            Documentation
          </Link>
          <Link
            className={'no-underline hover:text-defaultPrimary'}
            target='_blank'
            href='https://github.com/apache/gravitino/issues'
            data-refer='footer-link-support'
          >
            Support
          </Link>
        </span>
      </div>
    </footer>
  )
}

export default Footer
