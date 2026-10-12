"""Atomic publish generations for public maps.

A generation freezes the content of one save (ordered layers with the dataset
versions they were saved against, plus view parameters), builds the style,
sprite and optional images into a generation-scoped storage namespace, and
moves the map's public pointer only after every required asset is confirmed.
"""
