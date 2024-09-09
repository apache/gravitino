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

const MainContent = ({ children }) => {
  return (
    <main
      className={`layout-main twc-grow twc-w-full twc-px-[1.5rem] sm:twc-px-6 twc-p-8 twc-transition-[padding] twc-duration-300 twc-ease-in-out twc-mx-auto [@media(min-width:1440px)]:twc-max-w-[1440px] [@media(min-width:1200px)]:twc-max-w-full twc-max-h-[100vh]`}
    >
      {children}
    </main>
  )
}

export default MainContent
