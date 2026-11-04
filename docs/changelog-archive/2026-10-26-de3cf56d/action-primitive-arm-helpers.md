### Changed
- **Workflow action steps share one scope check and one "and N more" list helper (#769).** The wrong-scope refusal of each action step that checks its scope, and the capped problem lists of `persist-qc-checks` and the barcode-roster check, were written inline at each site. No behavior change: every message is unchanged.
