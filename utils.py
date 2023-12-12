def flatten_list(nested_list: list[list]):
    flattened = []
    for item in nested_list:
        if isinstance(item, list):
            flattened.extend(flatten_list(item))
        else:
            flattened.append(item)
    return flattened


def calculate_delay(elapsed_time: int):
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
