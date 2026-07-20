"""Shared test helpers: paths, balance import, CLI runner, and the invariant
checker used across the balance.py test suite.

Everything is stdlib-only (unittest). Import `balance` directly for white-box
checks; drive the real CLI as a subprocess for black-box checks.
"""
import json
import os
import subprocess
import sys
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIX_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")
BALANCE_PY = os.path.join(REPO_ROOT, "balance.py")

if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import balance  # noqa: E402


# --------------------------------------------------------------------------- #
# fixtures
# --------------------------------------------------------------------------- #
def fixtures() -> List[str]:
    """Sorted list of fixture names (without the .status suffix)."""
    return sorted(f[:-7] for f in os.listdir(FIX_DIR) if f.endswith(".status"))


def fixture_path(name: str) -> str:
    return os.path.join(FIX_DIR, name + ".status")


# Expected rendered classification under a plain `plan` (no --force), derived from
# observed tool behaviour. Tests assert the tool keeps behaving this way.
NOOP = {"balanced", "ram_blocked"}                  # emits no commands
# NO PLAN without --force (resource-short). `oscillation` is a real capture that
# provoked the greedy limit-cycle; it is the guard's self-contained reproducer.
NEEDS_FORCE = {"cpu_short", "force_overcommit", "oscillation"}
# everything else is expected to emit a deployable plan under plain `plan`.

# Endpoint alignment and dense-consolidation are first-class goals pursued
# INDEPENDENT of resource balance, so these two may lower the resource score.
SCORE_MAY_DROP = {"endpoint_misaligned", "dense_placement"}


# --------------------------------------------------------------------------- #
# CLI runner (black-box)
# --------------------------------------------------------------------------- #
def run_cli(*args: str, timeout: int = 120) -> Tuple[int, str, str]:
    """Invoke `python balance.py <args>` from the repo root. Returns
    (returncode, stdout, stderr). Color is off (not a TTY)."""
    proc = subprocess.run(
        [sys.executable, BALANCE_PY, *args],
        cwd=REPO_ROOT, capture_output=True, text=True, timeout=timeout,
    )
    return proc.returncode, proc.stdout, proc.stderr


def plan_json(name: str, *extra: str) -> Dict[str, Any]:
    """Run `plan --status-file <fixture> --format json [extra]` and parse it."""
    rc, out, err = run_cli("plan", "--status-file", fixture_path(name),
                           "--format", "json", *extra)
    assert rc == 0, f"plan --format json failed for {name} (rc={rc}): {err}"
    return json.loads(out)


def rladmin_commands(text: str) -> List[str]:
    """Extract the emitted rladmin command lines from a plan's text output."""
    return [ln.strip() for ln in text.splitlines()
            if ln.strip().startswith("rladmin ")]


# --------------------------------------------------------------------------- #
# white-box planning (build the analysis context directly)
# --------------------------------------------------------------------------- #
def build_ctx(name: str, force: bool = False):
    """Return (cluster, ctx) where ctx = balance._Ctx run over the fixture."""
    cluster = balance.discover_from_status_file(fixture_path(name))
    cluster.config = balance.Config()
    ctx = balance._Ctx(cluster, force=force)
    return cluster, ctx


def _maps(cluster) -> Tuple[Dict[int, Any], Dict[int, Any]]:
    db_by_uid = {d.uid: d for d in cluster.databases}
    shard_by_uid = {s.uid: s for s in cluster.shards}
    return db_by_uid, shard_by_uid


# --------------------------------------------------------------------------- #
# invariant checker  ->  list of "invariant: detail" violation strings
# --------------------------------------------------------------------------- #
def check_invariants(name: str, cluster, ctx) -> List[str]:
    v: List[str] = []
    planned = ctx.planned_state
    caps = ctx.caps
    db_by_uid, shard_by_uid = _maps(cluster)
    shards_by_db: Dict[int, List[Any]] = {}
    for s in cluster.shards:
        shards_by_db.setdefault(s.bdb_uid, []).append(s)

    def groups(db_uid):
        g: Dict[Any, List[Any]] = {}
        for s in shards_by_db.get(db_uid, []):
            g.setdefault(s.ha_group, []).append(s)
        return g

    # 1. Master/replica anti-affinity: HA-group members on distinct nodes.
    for d in cluster.databases:
        for key, members in groups(d.uid).items():
            nodes = [planned.place[s.uid] for s in members]
            if len(set(nodes)) != len(nodes):
                v.append(f"anti_affinity: db{d.uid} group {key} co-located on {nodes}")

    # 2. Rack-awareness: HA-group members on distinct racks (when rack-aware).
    if cluster.rack_aware:
        for d in cluster.databases:
            for key, members in groups(d.uid).items():
                racks = [caps[planned.place[s.uid]].rack_id for s in members]
                racks = [r for r in racks if r is not None]
                if len(set(racks)) != len(racks):
                    v.append(f"rack_awareness: db{d.uid} group {key} shares a rack {racks}")

    # 3. Shard-count limit per node (incl. out-of-scope shards).
    per_node: Dict[int, int] = {}
    for s in cluster.shards:
        per_node[planned.place[s.uid]] = per_node.get(planned.place[s.uid], 0) + 1
    for n, cnt in per_node.items():
        mx = caps[n].max_shards if n in caps else None
        if mx is not None and cnt > mx:
            v.append(f"shard_limit: node{n} has {cnt} shards > max {mx}")

    # 4. Every planned step must validate (holds even under --force; RAM is the only
    #    thing --force relaxes, and valid_move reports the non-RAM constraints).
    for st in ctx.steps:
        if not st.get("valid", True):
            v.append(f"invalid_step: step {st.get('step')} {st.get('kind')} "
                     f"db{st.get('db')} not valid")

    # 5. A replicated master's process is never migrated (mastership moves via failover).
    for st in ctx.steps:
        if st["kind"] == "shard":
            d = db_by_uid.get(st["db"])
            if d is not None and d.replication and st.get("role") == "master":
                v.append(f"master_migrated: db{d.uid} shard {st.get('shard')} "
                         "migrated as a replicated master")

    # 6. Endpoints sit on master nodes for policy-following DBs.
    for d in cluster.databases:
        if d.uid not in planned.scope:
            continue
        if planned.indep_ep.get(d.uid) or d.proxy_policy == "all-nodes":
            continue
        if not cluster.config.respect_endpoint(d):
            continue
        masters = {planned.place[s.uid] for s in shards_by_db.get(d.uid, [])
                   if planned.role[s.uid] == "master"}
        ep = planned.ep.get(d.uid, set())
        if ep and not ep.issubset(masters):
            v.append(f"endpoint_alignment: db{d.uid} endpoints {ep} not on masters {masters}")

    # 7. Out-of-scope DBs (flex / non-redis) are never touched.
    for d in cluster.databases:
        if not (d.is_flex or d.db_type != "redis"):
            continue
        for s in shards_by_db.get(d.uid, []):
            if planned.place[s.uid] != s.node_uid or planned.role[s.uid] != s.role:
                v.append(f"oos_touched: db{d.uid} shard {s.uid} moved/failed over")

    # 8. No shard is migrated more than once (compaction + no-oscillation guard).
    seen = set()
    for st in ctx.steps:
        if st["kind"] == "shard":
            if st["shard"] in seen:
                v.append(f"shard_moved_twice: shard {st['shard']} appears in >1 move "
                         "(reversing/pass-through churn)")
            seen.add(st["shard"])

    return v


def raw_greedy_move_count(cluster, force: bool) -> int:
    """Number of moves the greedy search emits BEFORE compaction - a proxy for
    convergence (a thrashing search would approach optimize()'s max_iter)."""
    caps = balance.node_capacities(
        cluster, balance.compute_loads(cluster, balance.Placement.current(cluster),
                                       cluster.endpoints_by_db))
    _state, moves, _db_caps = balance.optimize(cluster, caps, force=force)
    return len(moves)
