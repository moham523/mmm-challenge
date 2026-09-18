"""Dagster entry point (dg convention) -- loads every definition found
under defs/ automatically. You shouldn't need to edit this file.

Candidate: add your assets/jobs as modules under defs/ (e.g. defs/assets.py).
`dg dev` / `dg check defs` / `dg list defs` will pick them up automatically.
"""

from pathlib import Path

from dagster import definitions, load_from_defs_folder


@definitions
def defs():
    return load_from_defs_folder(path_within_project=Path(__file__).parent)
