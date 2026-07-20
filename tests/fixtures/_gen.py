#!/usr/bin/env python3
"""Fixture generator for balance.py tests.

Emits VALID `rladmin status extra all` capture files from compact Python scenario
specs, so tests exercise the real parser + planner + renderer end-to-end without a
live cluster. Run `python tests/fixtures/_gen.py` to (re)generate every *.status
file in this directory.

Format notes (must match balance.discover_from_status_file):
  * Sections are `HEADER:` lines (uppercase); tables are fixed-width, columns keyed
    off header-token START positions (balance.parse_status_table).
  * CLUSTER NODES: FREE_RAM='free/total' (total used), PROVISIONAL_RAM='avail/cap'
    (avail = RAM for new shards), SHARDS='used/max' (max = shard-count limit).
  * DATABASES: MEMORY_SIZE, REPLICATION(enabled/disabled), PLACEMENT(dense/sparse),
    PROXY_POLICY, TYPE, SHARDS, and an AUTO_TIERING column when any DB is flex.
  * ENDPOINTS: ROLE column carries the proxy policy (e.g. 'all-master-shards').
  * SHARDS: DB:ID, ID, NODE, ROLE, SLOTS, USED_MEMORY.
"""
import os
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

SLOTS_TOTAL = 16384


@dataclass
class NodeSpec:
    uid: int
    cores: int = 12
    ram_total_gb: float = 32.0      # FREE_RAM total
    prov_avail_gb: float = 24.0     # PROVISIONAL_RAM available (room for new shards)
    max_shards: int = 100
    rack: Optional[str] = None
    status: str = "OK"

    @property
    def addr(self) -> str:
        return f"172.16.22.{10 + self.uid}"


@dataclass
class DbSpec:
    uid: int
    name: str
    per_shard_mb: int
    replication: bool
    placement: str            # 'dense' | 'sparse'
    policy: str               # 'single' | 'all-master-shards' | 'all-master-proxies' | 'all-nodes'
    shards: List[Tuple[int, str]]     # ordered [(node_uid, 'master'|'slave'), ...]
    endpoints: List[int]              # actual endpoint node uids
    db_type: str = "redis"
    flex: bool = False

    @property
    def n_master(self) -> int:
        return sum(1 for _n, r in self.shards if r == "master")

    @property
    def memory_size_mb(self) -> int:
        return self.per_shard_mb * self.n_master


@dataclass
class Scenario:
    name: str
    nodes: List[NodeSpec]
    dbs: List[DbSpec]
    note: str = ""
    rack_aware: bool = False


# --------------------------------------------------------------------------- #
# table emitter (fixed width, left-justified; header tokens are single words)
# --------------------------------------------------------------------------- #
def _emit_table(headers: List[str], rows: List[List[str]]) -> str:
    widths = [len(h) for h in headers]
    for r in rows:
        for i, cell in enumerate(r):
            widths[i] = max(widths[i], len(str(cell)))
    gap = 2

    def fmt(cells: List[str]) -> str:
        return "".join(str(c).ljust(widths[i] + gap) for i, c in enumerate(cells)).rstrip()

    out = [fmt(headers)]
    out.extend(fmt(r) for r in rows)
    return "\n".join(out)


def _gb(x: float) -> str:
    return f"{x:g}GB"


def _mb(x: int) -> str:
    if x and x % 1024 == 0:
        return f"{x // 1024:g}GB"
    return f"{x:g}MB"


def _slots(idx: int, total: int) -> str:
    """Even slot-range split across `total` master groups."""
    if total <= 0:
        return "0-0"
    step = SLOTS_TOTAL // total
    lo = idx * step
    hi = (SLOTS_TOTAL - 1) if idx == total - 1 else (lo + step - 1)
    return f"{lo}-{hi}"


def render(scn: Scenario) -> str:
    any_flex = any(d.flex for d in scn.dbs)

    # --- CLUSTER (informational) ---
    parts: List[str] = []
    parts.append("CLUSTER:")
    parts.append(f"OK. Cluster master: {scn.nodes[0].uid} ({scn.nodes[0].addr})")
    parts.append("Cluster health: OK")
    parts.append("")

    # --- CLUSTER NODES ---
    node_headers = ["NODE:ID", "ROLE", "ADDRESS", "HOSTNAME", "SHARDS", "CORES",
                    "FREE_RAM", "PROVISIONAL_RAM", "RACK_ID", "STATUS"]
    node_rows = []
    used_shards = {n.uid: 0 for n in scn.nodes}
    for d in scn.dbs:
        for node_uid, _role in d.shards:
            used_shards[node_uid] = used_shards.get(node_uid, 0) + 1
    for i, n in enumerate(scn.nodes):
        role = "master" if i == 0 else "slave"
        free = max(0.0, n.ram_total_gb * 0.4)
        node_rows.append([
            f"{'*' if i == 0 else ''}node:{n.uid}", role, n.addr, f"host{n.uid}",
            f"{used_shards.get(n.uid, 0)}/{n.max_shards}", str(n.cores),
            f"{_gb(free)}/{_gb(n.ram_total_gb)}",
            f"{_gb(n.prov_avail_gb)}/{_gb(n.ram_total_gb * 0.8)}",
            (n.rack or "-"), n.status,
        ])
    parts.append("CLUSTER NODES:")
    parts.append(_emit_table(node_headers, node_rows))
    parts.append("")

    # --- DATABASES ---
    db_headers = ["DB:ID", "NAME", "TYPE", "STATUS", "SHARDS", "MEMORY_SIZE",
                  "PLACEMENT", "REPLICATION", "PROXY_POLICY"]
    if any_flex:
        db_headers.append("AUTO_TIERING")
    db_rows = []
    for d in scn.dbs:
        row = [
            f"db:{d.uid}", d.name, d.db_type, "active", str(d.n_master),
            _mb(d.memory_size_mb), d.placement,
            "enabled" if d.replication else "disabled", d.policy,
        ]
        if any_flex:
            row.append("enabled" if d.flex else "disabled")
        db_rows.append(row)
    parts.append("DATABASES:")
    parts.append(_emit_table(db_headers, db_rows))
    parts.append("")

    # --- ENDPOINTS (ROLE carries the policy) ---
    ep_headers = ["DB:ID", "NAME", "ID", "NODE", "ROLE", "SSL", "WATCHDOG_STATUS"]
    ep_rows = []
    for d in scn.dbs:
        for node_uid in d.endpoints:
            ep_rows.append([f"db:{d.uid}", d.name, f"endpoint:{d.uid}:1",
                            f"node:{node_uid}", d.policy, "No", "OK"])
    parts.append("ENDPOINTS:")
    parts.append(_emit_table(ep_headers, ep_rows))
    parts.append("")

    # --- SHARDS ---
    sh_headers = ["DB:ID", "NAME", "ID", "NODE", "ROLE", "SLOTS", "USED_MEMORY",
                  "RAM_FRAG", "WATCHDOG_STATUS", "STATUS"]
    sh_rows = []
    shard_uid = 1
    for d in scn.dbs:
        # group masters/slaves so each HA group shares a slot range
        n = d.n_master
        # index masters in order; a slave shares its preceding master's group idx.
        m_idx = 0
        group_of = []
        for _node, role in d.shards:
            if role == "master":
                group_of.append(m_idx)
                m_idx += 1
            else:
                group_of.append(max(0, m_idx - 1) if m_idx else 0)
        for k, (node_uid, role) in enumerate(d.shards):
            used = d.per_shard_mb if role == "master" else max(1, int(d.per_shard_mb * 0.6))
            sh_rows.append([
                f"db:{d.uid}", d.name, f"redis:{shard_uid}", f"node:{node_uid}",
                role, _slots(group_of[k], n), _mb(used), "19.4MB", "OK", "OK",
            ])
            shard_uid += 1
    parts.append("SHARDS:")
    parts.append(_emit_table(sh_headers, sh_rows))
    parts.append("")
    return "\n".join(parts) + "\n"


# --------------------------------------------------------------------------- #
# scenario builders
# --------------------------------------------------------------------------- #
def _mirror(pairs: List[Tuple[int, int]]) -> List[Tuple[int, str]]:
    """[(master_node, slave_node), ...] -> ordered master/slave shard list."""
    out: List[Tuple[int, str]] = []
    for mn, sn in pairs:
        out.append((mn, "master"))
        out.append((sn, "slave"))
    return out


def scn_balanced() -> Scenario:
    nodes = [NodeSpec(i) for i in (1, 2, 3, 4)]
    # 4 master groups, masters+slaves evenly spread over 4 nodes.
    db = DbSpec(1, "db1", 1024, True, "sparse", "all-master-shards",
                _mirror([(1, 2), (2, 3), (3, 4), (4, 1)]),
                endpoints=[1, 2, 3, 4])
    return Scenario("balanced", nodes, [db], "already balanced -> NO PLAN NEEDED")


def scn_mem_imbalanced() -> Scenario:
    nodes = [NodeSpec(i) for i in (1, 2, 3, 4)]
    # node1 hosts 3 masters, node4 hosts none -> memory skew (movable slaves exist).
    db = DbSpec(1, "db1", 2048, True, "sparse", "all-master-shards",
                _mirror([(1, 2), (1, 3), (1, 4), (2, 3)]),
                endpoints=[1, 2])
    return Scenario("mem_imbalanced", nodes, [db], "node1 overloaded on memory")


def scn_cpu_imbalanced() -> Scenario:
    nodes = [NodeSpec(i, cores=8) for i in (1, 2, 3, 4)]
    # pile shards + endpoints on node1 to spike its required vCPU.
    db1 = DbSpec(1, "db1", 512, True, "sparse", "all-master-shards",
                 _mirror([(1, 2), (1, 3), (1, 4), (2, 4)]), endpoints=[1, 2, 4])
    db2 = DbSpec(2, "db2", 512, True, "sparse", "all-master-shards",
                 _mirror([(1, 3), (3, 1)]), endpoints=[1, 3])
    return Scenario("cpu_imbalanced", nodes, [db1, db2], "node1 hot on vCPU")


def scn_endpoint_misaligned() -> Scenario:
    nodes = [NodeSpec(i) for i in (1, 2, 3, 4)]
    # single-policy DB: masters mostly on node3, endpoint parked on node1.
    db = DbSpec(1, "db1", 1024, True, "dense", "single",
                _mirror([(3, 4), (3, 4), (3, 4), (3, 4)]), endpoints=[1])
    return Scenario("endpoint_misaligned", nodes, [db],
                    "single endpoint not co-located with masters")


def scn_dense_placement() -> Scenario:
    nodes = [NodeSpec(i) for i in (1, 2, 3, 4)]
    # non-replicated dense DB spread one-master-per-node -> masters ARE movable, so
    # the dense priority pass should consolidate them onto fewer nodes.
    db = DbSpec(1, "db1", 256, False, "dense", "all-master-shards",
                [(1, "master"), (2, "master"), (3, "master"), (4, "master")],
                endpoints=[1, 2, 3, 4])
    return Scenario("dense_placement", nodes, [db], "dense DB should consolidate")


def scn_rack_aware() -> Scenario:
    nodes = [NodeSpec(1, rack="rack-a"), NodeSpec(2, rack="rack-a"),
             NodeSpec(3, rack="rack-b"), NodeSpec(4, rack="rack-b")]
    db = DbSpec(1, "db1", 1024, True, "sparse", "all-master-shards",
                _mirror([(1, 3), (1, 4), (2, 3), (3, 2)]), endpoints=[1, 2, 3])
    return Scenario("rack_aware", nodes, [db], "rack-aware anti-affinity", rack_aware=True)


def scn_ram_blocked() -> Scenario:
    # non-replicated (no failover escape hatch) + every node full -> memory
    # imbalance that CANNOT be fixed by any single move -> NO PLAN (too full).
    nodes = [NodeSpec(i, ram_total_gb=16, prov_avail_gb=0.2) for i in (1, 2, 3, 4)]
    db = DbSpec(1, "db1", 4096, False, "sparse", "all-master-shards",
                [(1, "master"), (1, "master"), (1, "master"), (2, "master")],
                endpoints=[1, 2])
    return Scenario("ram_blocked", nodes, [db], "imbalanced but too full to move")


def scn_cpu_short() -> Scenario:
    # total required vCPU exceeds total cores -> no feasible balance w/o --force.
    nodes = [NodeSpec(i, cores=4) for i in (1, 2, 3, 4)]
    db1 = DbSpec(1, "db1", 512, True, "sparse", "all-master-shards",
                 _mirror([(1, 2), (2, 3), (3, 4), (4, 1)]), endpoints=[1, 2, 3, 4])
    db2 = DbSpec(2, "db2", 512, True, "sparse", "all-master-shards",
                 _mirror([(1, 2), (2, 3), (3, 4), (4, 1)]), endpoints=[1, 2, 3, 4])
    return Scenario("cpu_short", nodes, [db1, db2], "cluster short on vCPU (force needed)")


def scn_force_overcommit() -> Scenario:
    # like _bug.txt: imbalanced AND resource-short -> only --force yields a plan.
    nodes = [NodeSpec(i, cores=6, ram_total_gb=16, prov_avail_gb=1.0) for i in (1, 2, 3, 4)]
    db1 = DbSpec(1, "db1", 2048, True, "sparse", "all-master-shards",
                 _mirror([(1, 2), (1, 3), (1, 4), (2, 3), (2, 4)]), endpoints=[1, 2])
    db2 = DbSpec(2, "db2", 1024, True, "sparse", "all-master-shards",
                 _mirror([(1, 2), (1, 3), (3, 4), (4, 1)]), endpoints=[1, 3])
    return Scenario("force_overcommit", nodes, [db1, db2], "force over-commit; must converge")


def scn_flex_oos() -> Scenario:
    nodes = [NodeSpec(i) for i in (1, 2, 3, 4)]
    db1 = DbSpec(1, "db1", 1024, True, "sparse", "all-master-shards",
                 _mirror([(1, 2), (1, 3), (1, 4), (2, 3)]), endpoints=[1, 2])
    flex = DbSpec(2, "db2flex", 2048, True, "sparse", "all-master-shards",
                  _mirror([(1, 2), (1, 3)]), endpoints=[1], flex=True)
    return Scenario("flex_oos", nodes, [db1, flex], "flex DB must never be rebalanced")


def scn_memcached_oos() -> Scenario:
    nodes = [NodeSpec(i) for i in (1, 2, 3, 4)]
    db1 = DbSpec(1, "db1", 1024, True, "sparse", "all-master-shards",
                 _mirror([(1, 2), (1, 3), (1, 4), (2, 3)]), endpoints=[1, 2])
    mc = DbSpec(2, "db2mc", 1024, True, "sparse", "all-master-shards",
                _mirror([(1, 2), (1, 3)]), endpoints=[1], db_type="memcached")
    return Scenario("memcached_oos", nodes, [db1, mc], "non-redis DB must be out of scope")


def scn_non_replicated() -> Scenario:
    nodes = [NodeSpec(i) for i in (1, 2, 3, 4)]
    # no replication: all shards are masters (movable, they carry endpoints).
    db = DbSpec(1, "db1", 1024, False, "sparse", "all-master-shards",
                [(1, "master"), (1, "master"), (1, "master"), (2, "master")],
                endpoints=[1, 2])
    return Scenario("non_replicated", nodes, [db], "non-replicated DB imbalance")


def scn_tiny_two_node() -> Scenario:
    nodes = [NodeSpec(1), NodeSpec(2)]
    db = DbSpec(1, "db1", 1024, True, "sparse", "all-master-shards",
                _mirror([(1, 2), (1, 2)]), endpoints=[1])
    return Scenario("tiny_two_node", nodes, [db], "2-node edge case")


def scn_excluded_db() -> Scenario:
    # db2 is imbalanced but will be excluded via config -> must stay put.
    nodes = [NodeSpec(i) for i in (1, 2, 3, 4)]
    db1 = DbSpec(1, "db1", 1024, True, "sparse", "all-master-shards",
                 _mirror([(1, 2), (2, 3), (3, 4), (4, 1)]), endpoints=[1, 2, 3, 4])
    db2 = DbSpec(2, "db2", 2048, True, "sparse", "all-master-shards",
                 _mirror([(1, 2), (1, 3), (1, 4), (2, 3)]), endpoints=[1, 2])
    return Scenario("excluded_db", nodes, [db1, db2], "db2 excluded via config")


def scn_all_nodes_policy() -> Scenario:
    nodes = [NodeSpec(i) for i in (1, 2, 3, 4)]
    db = DbSpec(1, "db1", 1024, True, "sparse", "all-nodes",
                _mirror([(1, 2), (1, 3), (1, 4), (2, 3)]),
                endpoints=[1, 2, 3, 4])
    return Scenario("all_nodes_policy", nodes, [db], "all-nodes proxy policy")


ALL = [
    scn_balanced, scn_mem_imbalanced, scn_cpu_imbalanced, scn_endpoint_misaligned,
    scn_dense_placement, scn_rack_aware, scn_ram_blocked, scn_cpu_short,
    scn_force_overcommit, scn_flex_oos, scn_memcached_oos, scn_non_replicated,
    scn_tiny_two_node, scn_excluded_db, scn_all_nodes_policy,
]

# scenarios expected to yield a deployable plan under normal `plan` (no --force)
PLANNABLE = {"mem_imbalanced", "cpu_imbalanced", "endpoint_misaligned",
             "dense_placement", "rack_aware", "flex_oos", "memcached_oos",
             "non_replicated", "excluded_db", "all_nodes_policy"}
BALANCED = {"balanced"}
NEEDS_FORCE = {"cpu_short", "force_overcommit"}
BLOCKED = {"ram_blocked"}


def main() -> None:
    here = os.path.dirname(os.path.abspath(__file__))
    for builder in ALL:
        scn = builder()
        path = os.path.join(here, scn.name + ".status")
        with open(path, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(render(scn))
        print("wrote", os.path.relpath(path, os.path.dirname(here)))
    # config used by the excluded_db scenario
    cfg = os.path.join(here, "excluded_db.config.json")
    with open(cfg, "w", encoding="utf-8", newline="\n") as fh:
        fh.write('{\n  "databases": {\n    "db2": { "exclude_from_balancing": true }\n  }\n}\n')
    print("wrote", os.path.relpath(cfg, os.path.dirname(here)))


if __name__ == "__main__":
    main()
