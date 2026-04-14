# `scripts/checkpoints/`

Use this directory for generated file-based JSON checkpoints created by long-running scripts.

Rules:
- checkpoint files are generated, not hand-authored
- writes should be atomic so interrupted runs do not leave partial JSON behind
- checkpoint data should include resumable progress fields such as cursor, batch number, and row counters
- completed checkpoints should make reruns safe by default unless the caller explicitly forces a restart

Naming convention:
- `<script_stem>__<run_label>.json` for named jobs
- keep names lowercase and machine-friendly

Generated checkpoint files in this directory are ignored by git. The directory and this README stay tracked so the structure remains visible.
