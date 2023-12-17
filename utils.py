from typing import Any

def flatten_list(nested_list: list[list]) -> list[Any]:
    """
    Flattens a nested list into a single-level list.

    Parameters:
        nested_list: The nested list to be flattened.

    Returns:
      list[Any]: A single-level list containing all elements from the nested list.
    """
    flattened = []
    for item in nested_list:
        if isinstance(item, list):
            flattened.extend(flatten_list(item))
        else:
            flattened.append(item)
    return flattened


def calculate_delay(elapsed_time: int) -> int:
    """
    Determines the delay duration based on the elapsed time.

    This function computes the delay duration to regulate the request rate, depending on the time taken for the 
    preceding requests. It returns different delay times according to specific ranges of elapsed time.

    Parameters:
        elapsed_time: The time elapsed in seconds for the preceding operation.

    Returns:
        int: The duration of delay in seconds based on the elapsed time ranges.
    """
    if elapsed_time < 35:
        return 300
    elif elapsed_time < 60:
        return 240
    elif elapsed_time < 120:
        return 180
    elif elapsed_time < 180:
        return 120
    else:
        return 60
