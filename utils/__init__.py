
def flat_dict(pass_dict, keys, values):
    if isinstance(values, dict):
        for sub_key, sub_value in values.items():
            flat_dict(pass_dict, sub_key, sub_value)
    else:
        if keys in pass_dict:
            pass_dict[keys].append(values)
        else:
            pass_dict[keys] = [values]

def flat_json(json_):
    flattened_json = {'uid4':list()}
    for sale in json_.items():
        flattened_json['uid4'].append(sale[0])
        for key, value in sale[1].items():
            flat_dict(flattened_json, key, value)
    return flattened_json
