
def flat_dict_pd(pass_dict, keys, values):
    if isinstance(values, dict):
        for sub_key, sub_value in values.items():
            flat_dict(pass_dict, sub_key, sub_value)
    else:
        if keys in pass_dict:
            pass_dict[keys].append(values)
        else:
            pass_dict[keys] = [values]


def flat_json_pd(json_):
    flattened_json = {'uid4':list()}
    for sale in json_.items():
        flattened_json['uid4'].append(sale[0])
        for key, value in sale[1].items():
            flat_dict_pd(flattened_json, key, value)
    return flattened_json

def flat_dict(pass_dict, keys, values):
    if isinstance(values, dict):
        for sub_key, sub_value in values.items():
            flat_dict(pass_dict, sub_key, sub_value)
    else:
        pass_dict[keys] = values
    
def flat_json(json_):
    end_dict = []
    for row in json_.items():
        pass_dict = {}
        pass_dict['uid4'] = row[0]
        for key, value in row[1].items():
            flat_dict(pass_dict, key, value)
        end_dict.append(pass_dict)
    return end_dict