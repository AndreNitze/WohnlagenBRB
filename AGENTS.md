# Repository Guidelines

## Project Structure & Module Organization
This repository is centered on notebook-driven analysis for housing-location scoring in Brandenburg an der Havel.

- Main notebooks: `wohnlagen.ipynb` (primary workflow), `wohnlagen_2026.ipynb`, `public-transport.ipynb`, `laerm.ipynb`
- Python helpers/scripts: `helper.py`, `geocoder.py`, `routing.py`, `routing2.py`, `medizinische-zentren.py`, `einzelhandel-adressen.py`
- Map export utility: `maps/html_to_png.py`
- Input data: `data/` (CSV, SHP, GPKG, GTFS)
- Generated outputs: `out/` (geocoded/routing CSVs), `cache/` (API cache artifacts)

Keep new processing scripts at repository root or in `maps/` if map-render related.

## Build, Test, and Development Commands
- `uv sync` installs dependencies from `pyproject.toml` and `uv.lock`.
- `pip install -r requirements.txt` is an alternative setup path.
- `jupyter lab` starts the notebook environment.
- `python geocoder.py` geocodes configured source CSVs (edit constants at file top first).
- `python routing.py` computes route distances and writes enriched CSV outputs.
- `python maps/html_to_png.py --start 4 --end 12` renders cluster HTML maps to PNG screenshots.

Run commands from the repository root so relative paths like `data/...` and `out/...` resolve correctly.

## Coding Style & Naming Conventions
Use Python with 4-space indentation and `snake_case` for functions/variables/files. Follow existing script style: module-level constants in `UPPER_CASE`, concise comments, and clear German-domain column naming when matching source data (for example `Straßenname`, `HsnrZus`).

No enforced formatter/linter config is currently committed. Keep changes minimal, readable, and consistent with surrounding code.

## Testing Guidelines
There is currently no automated `pytest` suite in this repository. Validate changes with:

- Script smoke runs on a small input slice
- Notebook cell re-execution for modified sections
- Output checks in `out/` (expected columns, non-empty coordinates/distances)

If you add non-trivial logic, include a focused `pytest` test module under `tests/` and document how to run it.

## Commit & Pull Request Guidelines
Recent history favors short, imperative commit subjects (for example: `Refactor routing.py...`, `Add public transport analysis...`, `Fix undefined ...`).

- Keep subject lines action-oriented and specific to changed files/workflow.
- Group related notebook/script/data-schema edits in one commit; separate unrelated refactors.
- PRs should include: purpose, changed files, data dependencies, and a brief validation summary (commands run, notebooks rerun, output files produced).
