# balance.py test suite

Stdlib-only (`unittest`) — no dependencies to install. Run everything with:

```bash
python tests/run.py            # all tests
python tests/run.py -v         # verbose
python tests/run.py test_rest  # a single module
```

(or `python -m unittest discover -s tests -t tests`).

## Layers

| File | What it covers |
|------|----------------|
| `fixtures/_gen.py` | Generates the `*.status` fixtures from compact Python scenario specs. Re-run after editing scenarios: `python tests/fixtures/_gen.py`. |
| `test_invariants.py` | Policy invariants on the planned layout for **every** fixture in both modes: anti-affinity, rack-awareness, shard limits, no replicated-master migration, endpoint↔master alignment, out-of-scope untouched, **no reversing/pass-through churn**, and greedy **convergence**. Plus the `oscillation` regression guard. |
| `test_cli.py` | Black-box CLI: JSON structure, plan classification, `--verbose`/`--format html`, determinism, `execute` refusal on a status file, and `--config` exclusion. |
| `test_planner.py` | White-box units: compaction correctness (drops reversing failovers + net-zero moves), scoring, and `valid_move` hard constraints. |
| `test_rest.py` | REST health gate: `rest_health` classification and `wait_cluster_ok` settle/hold loop (fake client + fake clock, never sleeps). |
| `test_golden.py` | Snapshot of `--format json` for representative fixtures. |

## Fixtures

Synthetic `rladmin status extra all` captures, one per scenario (balanced, memory/
CPU-imbalanced, endpoint-misaligned, dense, rack-aware, RAM-blocked, CPU-short,
force-overcommit, flex/memcached out-of-scope, non-replicated, tiny, excluded-db,
all-nodes policy). `oscillation.status` is a real capture that provoked the greedy
limit-cycle bug and guards against its return.

Add a scenario by writing a builder in `fixtures/_gen.py`, adding it to `ALL`, and
re-running the generator. Classification expectations live in `helpers.py`
(`NOOP`, `NEEDS_FORCE`, `SCORE_MAY_DROP`).

## Golden snapshots

Deterministic JSON captured under `golden/`. When you change planning behaviour on
purpose, refresh and review the diff:

```bash
BALANCE_UPDATE_GOLDEN=1 python tests/run.py test_golden
```
