# Standalone reproductions

Each script starts a fresh StarRocks 4.1.7 cluster with plain Docker, from the images in
`../loads/config`. They need no Antithesis and no fault injection. Build the images first:

```bash
cd loads/config && docker compose build
```

| Script | Finding | What it shows |
| --- | --- | --- |
| `label_dedup.sh` | 1 | A retried Stream Load is rejected until the FE restarts, then accepted, and the rows are stored twice |
| `run.sh` + `multi_table_txn.sh` | 2 | `MODE=multi ALTER_AT=during run.sh 20`: a column add during a multi-table load crashes every BE. `MODE=single` / `MODE=plain` are the controls |
| `torn_read.sh` | 3 | `FILTER=1 REPL=1 torn_read.sh 90 32 4`: one SELECT sees a multi-table transaction in one table and not the other. `SHARED=1` runs it against `loads/config-shared` |
| `torn_read_var.sh` | 3 | The same, with a configurable query (`QUERY`) and session settings (`PRE`), used to rule causes in and out |
| `torn_persist.sh` | 3 | The torn read inside `INSERT ... SELECT`: the half-applied state is stored in a table |
| `torn_partitions.sh` | 3 (weak) | The same race across two partitions of one table. Seen once in about 8,500 reads |

Debugging aids for Finding 2, in `debug/`:

- `run_gdb.sh` attaches gdb to a BE (`debug/Dockerfile.be-gdb`, `debug/catch.py`) and dumps the
  memtable's schema, slots and chunks at the crash.
- `bp_race.sh` pauses the FE's load planner with jdb (`debug/fe-jdwp.override.yaml`) and adds the
  column while it is paused. That crashes the BEs on the first try.
