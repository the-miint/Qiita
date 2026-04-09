"""DuckLake file registration — moves staged Parquet to permanent storage and registers."""

import shutil
from pathlib import Path

import duckdb


def register_staged_parquet(
    *,
    staging_dir: Path,
    ducklake_connstr: str,
    ducklake_data_path: str,
    table_file_map: dict[str, str],
) -> list[Path]:
    """Move Parquet files from staging to DuckLake data path and register.

    Compute backends write Parquet to an ephemeral staging directory. This
    function moves each file to permanent storage under ``ducklake_data_path``
    and registers it via ``ducklake_add_data_files`` (metadata-only — no data
    IO). File ownership transfers to DuckLake after registration.

    The move-then-register pattern is split into two phases so that a
    registration failure doesn't leave files in a half-moved state:

    1. Move all files from staging to permanent storage
    2. Register all moved files in DuckLake

    Uses ``shutil.move`` (not ``rename``) to handle cross-filesystem moves
    (e.g., SLURM local scratch → shared NFS).

    Parameters
    ----------
    staging_dir
        Directory containing Parquet files written by a compute backend.
    ducklake_connstr
        libpq connection string for the DuckLake Postgres catalog.
    ducklake_data_path
        Root directory where DuckLake stores data files.
    table_file_map
        Mapping of ``{filename: ducklake_table_name}`` for files to register.
        Files that don't exist in ``staging_dir`` are silently skipped.

    Returns
    -------
    list[Path]
        Paths of the registered files in their permanent locations.
    """
    perm_root = Path(ducklake_data_path)

    # Phase 1: move all files to permanent storage.
    moved: list[tuple[str, Path]] = []
    for filename, table in table_file_map.items():
        src = staging_dir / filename
        if not src.exists():
            continue
        dest_dir = perm_root / table
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / src.name
        shutil.move(src, dest)
        moved.append((table, dest))

    # Phase 2: register all moved files in DuckLake.
    with duckdb.connect(":memory:") as conn:
        conn.execute("LOAD ducklake; LOAD postgres;")
        conn.execute(
            f"ATTACH 'ducklake:postgres:{ducklake_connstr}' AS qiita_lake"
            f" (DATA_PATH '{ducklake_data_path}');"
        )
        for table, dest in moved:
            conn.execute(
                "CALL ducklake_add_data_files('qiita_lake', ?, ?)",
                [table, str(dest)],
            )

    return [dest for _, dest in moved]
