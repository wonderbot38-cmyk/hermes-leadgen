#!/bin/bash
# Start Hermes Lead-Gen dashboard
cd "$(dirname "$0")"
PY="/Library/Frameworks/Python.framework/Versions/3.13/bin/python3"
if [ ! -x "$PY" ]; then PY="python3"; fi
exec "$PY" server.py
