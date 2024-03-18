"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
from typing import Type

class Namespace:
    def __init__(self, *levels):
        assert levels is not None
        for level in levels:
            assert level is not None
        self.__levels = levels
  
    def levels(self) -> tuple:
        """
        Get the levels of the namespace

        Returns:
            levels: the levels of the namespace
        """
        return self.__levels
    
    def level(self, pos) -> str:
        """
        Get the the level at the given position
        Args:
            pos (int): the position of the level

        Returns:
            level(str): the level at the given position
        """
        assert pos >= 0 and pos < len(self.__levels)
        return self.__levels[pos]
    
    def length(self) -> int:
        """
        Get the length of the namespace

        Returns:
            length: the length of the namespace
        """
        print('values:' + str(self.__levels))
        return len(self.__levels)
    
    def is_empty(self) -> int:
        """
        Check if the namespace is empty

        Returns: true if the the namespace is empty, or else false.
        """
        return len(self.__levels) == 0
        
    

    


