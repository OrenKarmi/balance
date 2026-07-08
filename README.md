# balance.py — Redis Enterprise Software database rebalancing tool

A single-file, standard-library-only (Python 3.8+) tool that analyzes a Redis
Enterprise **Software** cluster and proposes — and optionally executes — a low-risk
plan to even out per-node **memory** and **CPU** utilization, while keeping every
database's endpoint co-located with its master shard(s).

> Scope (hard constraints): Redis Enterprise **Software** only (not Cloud / OSS),
> **Redis-on-RAM** databases only (Flex / Auto Tiering DBs are detected and reported
> out-of-scope, never rebalanced).

## What it does

- Evaluates balance cluster-wide as the coefficient of variation of per-node
  utilization (memory + CPU), across every in-scope database's shards and endpoints.
- Produces a plan using only **single, low-risk operations**:
  - migrate one **replica** shard,
  - **failover** a shard's role (never migrates a replicated master's process),
  - relocate/re-bind an **endpoint** (`rladmin migrate db … endpoint_to_shards`).
  No shard swaps.
- Enforces, always: master/replica **anti-affinity** and **rack-awareness** (when the
  cluster has it). Honors **dense/sparse** shard placement and proxy policy.
- **Endpoint↔master alignment** as a first-class goal: keeps each DB's endpoint with
  its master(s) (preferring load-neutral failovers to relocate masters onto the
  endpoint's node), independent of resource balance.

## Input sources (interchangeable — same scoring/planning)

- **live `rladmin`** (default; run on a cluster node),
- **`--status-file`** — a captured `rladmin status extra all` (analysis only), or
- **`--rest`** — the cluster REST API (can also execute).

## Usage

```bash
python balance.py                       # plan (read-only), default
python balance.py plan --format html > report.html
python balance.py plan --format json
python balance.py execute                # plan, then migrate after approval

# Off-node analysis from a captured file (plan only):
#   on a cluster node:  rladmin status extra all > status.txt
python balance.py plan --status-file ./status.txt

# REST API:
python balance.py plan --rest --rest-fqdn <host> --rest-user <user> --rest-password <pw>

# With a rules config:
python balance.py plan --config ./balance.config.json   # copy balance.config.json.sample first
```

`plan` is read-only (current layout → desired layout → rebalancing plan).
`execute` performs the migrations **after explicit approval**, verifying cluster
health between operations.

## Configuration

Optional JSON config (`--config`) with cluster-wide defaults and per-database
overrides — CPU weights, placement/endpoint policy, exclusions, and the deploy
health-check timeouts. See [`balance.config.json.sample`](balance.config.json.sample).

## Files

- `balance.py` — the tool (single file, standard library only).
- `balance.config.json.sample` — annotated example config; copy it to your own file and pass with `--config`.

## Safety

`execute` is the only mutating mode; it always asks for confirmation, migrations run
online (but may cause brief client disconnections / transient load), and it aborts if
the cluster does not return to a healthy status within the configured timeout.
