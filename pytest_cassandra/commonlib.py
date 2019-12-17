#!/usr/bin/env python
# coding=utf-8

import codecs
import json
import copy


def show_json(json_obj):
    obj = copy.deepcopy(json_obj)
    _convert_obj(obj)

    json_dump_str = json.dumps(obj, indent=2, ensure_ascii=False)
    return json_dump_str


def _convert_obj(obj):
    # Convert obj to make it can be json dumped. Now we just handle the bytes value
    if isinstance(obj, dict):

        for k,v in obj.items():
            if isinstance(v, bytes):
                obj[k] = codecs.encode(obj[k], 'hex').decode('ascii').upper()
            _convert_obj(v)

    elif isinstance(obj, list):
        for item in obj:
            _convert_obj(item)

    else:
        return obj
