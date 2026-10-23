### Changed

- **The ENA import runbook says how ENA's control-type attributes are treated (#768).** `neg_cont_type` and `pos_cont_type` stay study-local text and never mark a biosample as a control; a real control is marked by setting its `qiita sample type` to a control term.
