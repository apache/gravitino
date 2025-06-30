from abc import ABC, abstractmethod
from gravitino.filesystem.gvfs_storage_handler import StorageHandler


class StorageHandlerProvider(ABC):
    """
    Abstract base class for storage handler providers.

    This interface allows users to provide custom storage handlers for new file systems
    without modifying the core GVFS code. Similar to Java's FileSystemProvider pattern.
    """

    @abstractmethod
    def get_storage_handler(self) -> StorageHandler:
        """
        Returns the storage handler instance.

        :return: The storage handler instance
        """
        pass

    @abstractmethod
    def scheme(self) -> str:
        """
        Returns the URI scheme supported by this provider.

        :return: The URI scheme (e.g., 's3a', 'hdfs', 'custom')
        """
        pass

    @abstractmethod
    def name(self) -> str:
        """
        Returns the name of this provider.

        :return: The provider name
        """
        pass