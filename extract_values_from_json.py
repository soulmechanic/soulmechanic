import pandas as pd

def extract_elements(json_obj, keys):
    result = {}
    
    def extract(json_obj, keys, result):
        if isinstance(json_obj, dict):
            for key, value in json_obj.items():
                if key in keys:
                    result[key] = value
                else:
                    extract(value, keys, result)
        elif isinstance(json_obj, list):
            for item in json_obj:
                extract(item, keys, result)
    
    extract(json_obj, keys, result)
    return result
