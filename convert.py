#!/usr/bin/env python3

import io, libconf, sys, json

def rebuild_tuples(obj):
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k.endswith("_tuple"):
                out[k[:-6]] = tuple(rebuild_tuples(v))
            else:
                out[k] = rebuild_tuples(v)
        return out
    elif isinstance(obj, list):
        out = []
        for entry in obj:
            out.append(rebuild_tuples(entry))
        return out
    return obj

with io.open(sys.argv[1]) as f:
    obj = json.load(f)
    obj = rebuild_tuples(obj)
    print(libconf.dumps(obj))
