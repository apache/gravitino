/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
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
