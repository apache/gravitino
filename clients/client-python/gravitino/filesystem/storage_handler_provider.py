class StorageHandlerProvider(ABC):
    @abstractmethod
    def get_storage_handler(self) -> StorageHandler:
        """返回存储处理器实例"""
        pass

    @abstractmethod
    def scheme(self) -> str:
        """返回支持的URI scheme，如's3a', 'custom'等"""
        pass

    @abstractmethod
    def name(self) -> str:
        """返回provider名称"""
        pass