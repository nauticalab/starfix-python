"""Frozen legacy hash formats of starfix-python.

Each submodule reproduces one released version byte for byte, so that data hashed by that
version stays findable. Import the version explicitly, for example
``from starfix.legacy.v0_4 import ArrowDigester``; nothing is re-exported here.

Never hash new data with a legacy module: use ``starfix.ArrowDigester`` (hash format 2).
"""
