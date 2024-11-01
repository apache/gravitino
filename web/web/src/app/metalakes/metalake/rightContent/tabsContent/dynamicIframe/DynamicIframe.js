import { useState } from 'react'
import Loading from '@/app/rootLayout/Loading'

const DynamicIframe = () => {
  const [isLoaded, setIsLoaded] = useState(false)

  return (
    <>
      {!isLoaded && <Loading />}
      <iframe
        src="https://huggingface.co/datasets/neuralwork/arxiver/embed/viewer/default/train"
        frameborder="0"
        width="100%"
        height="100%"
        className={isLoaded ? 'twc-mt-[-42px]' : 'twc-hidden twc-mt-[-42px]'}
        onLoad={() => setIsLoaded(true)}
      ></iframe>
    </>
  )
};

export default DynamicIframe;