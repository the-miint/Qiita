"""Stable native-step module-path test constants.

Parallels `qiita_common.testing.containers` for container values: tests
across packages reference these by name so a rename of the canonical
native-step module path only needs to update this file.

Production code never reads these — actual module paths flow in from
the workflow YAML at sync time and live on `qiita.action.steps`. These
constants exist only so test fixtures and assertions can declare a
known native runtime without inlining a magic string at every site.
"""

from __future__ import annotations

FASTQ_TO_PARQUET_MODULE: str = "qiita_compute_orchestrator.jobs.fastq_to_parquet"
