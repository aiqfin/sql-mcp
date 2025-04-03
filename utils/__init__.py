def trans_none_only(source_dict: dict, new_dict: dict) -> dict:
    result = source_dict.copy()
    for key, value in new_dict.items():
        if key not in result or result[key] is None:
            result[key] = value
    return result


def replace_config_dict(source_dict: dict, new_dict: dict) -> dict:
    result = source_dict.copy()
    for key, value in new_dict.items():
        if value is not None:
            result[key] = value
    return result
