#!/usr/bin/env python3

import io, libconf, sys, json

with io.open(sys.argv[1]) as f:
    obj = json.load(f)
    print(libconf.dumps(obj))
