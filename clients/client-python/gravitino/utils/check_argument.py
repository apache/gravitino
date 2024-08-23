from gravitino.exceptions.base import IllegalArugmentException

def check_argument(condition, message):
    """
    Checks a condition and raises a ValueError with the provided message if the condition is False.

    Args:
        condition (bool): The condition to check.
        message (str): The error message to raise if the condition is False.
    """
    if not condition:
        raise IllegalArugmentException(message)