#!/usr/bin/env python3
"""Redis Enterprise Software - Database Rebalancing Tool.

Scope (hard constraints):
  * Redis Enterprise SOFTWARE only - not Cloud, not OSS/CE.
  * Redis-on-RAM databases only. Flex / Auto Tiering (bigstore) DBs are
    detected and reported as out-of-scope, never rebalanced.
  * Multi-database clusters: balance is evaluated cluster-wide, accounting
    for every in-scope database's shards/endpoints on each node together.
  * Endpoint<->master alignment: a DB's endpoint must sit with its master
    shard(s) per proxy policy (single -> the majority-master node; all-master-
    shards/-proxies -> every master node). This is enforced as its own goal -
    a misaligned endpoint is corrected (load-neutral failover preferred, else
    endpoint re-bind) even when the cluster is already resource-balanced.
  * Cluster state comes from ONE of three interchangeable inputs, all feeding the
    SAME scoring/planning: live `rladmin` (default), a captured
    `rladmin status extra all` file (--status-file, plan-only), or the REST API
    (--rest). The live rladmin path reads (all READ-ONLY in Step 1; mutating
    verbs are used only in 'execute'):
        rladmin status extra all       -> nodes, databases, shards, endpoints (one call)
        rladmin info db [<id>]         -> per-DB config (memory limit, proxy policy)
        rladmin info cluster           -> name + rack-awareness (best-effort)
        rladmin info node <id>         -> per-node rack id (only if rack-aware)

Architecture (built to carry Steps 2-4):
  * Inventory (Node/Database/Shard) is STATIC, discovered once.
  * A Placement maps shard_uid -> node_uid. The current placement is one
    instance; Step 2 will generate alternative candidate placements.
  * Per-node load (NodeLoad) and scoring are PURE functions of
    (inventory, placement), so any candidate placement can be scored and
    compared without re-reading the cluster or mutating inventory.

Resource model
  RAM (available per node): PROVISIONAL_RAM from `rladmin status nodes` is the
    RAM a node can still devote to NEW shards (FREE_RAM overstates it because
    the OS and Redis Enterprise reserve memory):
        ram available = PROVISIONAL_RAM ; ram capacity = provisioned + PROVISIONAL_RAM
  CPU (required per node), modelled as virtual cores:
        each shard (master or replica) = 1.0 vCPU ; each endpoint = 1.5 vCPU
        required vCPU = shards * 1.0 + endpoints * 1.5 ; available vCPU = cores

LIMITATIONS - discovery & resource model (L1-L8). Planner-specific limitations
(L9-L13) are documented at the "STEP 2" section further down.
  L1  Per-shard memory is the even-split limit memory_size/shards_count, not the
      live per-shard footprint. Correct for provisioning-based balancing of RAM
      databases; not a measure of actual key distribution skew.
  L2  For --status-file input, endpoint placement is DERIVED from each DB's proxy
      policy + master locations (single -> majority-masters node; all-master-shards
      -> every master node; unrecognised -> one endpoint). Live rladmin and --rest
      read the real ENDPOINTS instead of deriving them.
  L3  Only proxy policies 'single', 'all-master-shards'/'all-master-proxies',
      and 'all-nodes' are modelled; others fall back to 'single'.
  L4  CPU demand is the static 1.0/1.5 vCPU model, NOT measured live utilisation.
  L5  Bulk `rladmin info db` parsing depends on the RE version; if it yields no
      per-DB blocks the tool falls back to one `info db <id>` call per database
      (slower on clusters with hundreds of DBs).
  L6  rack_id is read via one `rladmin info node <id>` call per node, and only
      when the cluster is rack-aware. If unavailable, rack-aware validity cannot
      be checked by later steps.
  L7  Non-hostable node detection (quorum-only / maintenance) is heuristic:
      a node is treated as non-hostable only if it is DOWN or reports max
      shards == 0. A dedicated quorum-only flag is not parsed.
  L8  shards_placement (dense/sparse) is a HARD constraint enforced by the planner
      (see L11); Step 1 only captures it.

Modes:
    plan      Steps 1-3 (current layout, desired layout, rebalancing plan).
              READ-ONLY. This is the default when no mode word is given.
    execute   Run steps 1-3, then perform the migrations + endpoint re-binds
              after operator approval. The ONLY mutating mode.

Usage (run directly on a cluster node; rladmin on PATH):
    python balance.py                       # plan (steps 1-3); default
    python balance.py plan --format html > report.html
    python balance.py plan --format json
    python balance.py execute               # plan, then migrate (asks approval)
    # Off-node analysis against a captured status file (execute cannot run migrations):
    python balance.py plan --status-file ./status.txt --format html > report.html
    #   capture it on a cluster node with:  rladmin status extra all > status.txt
    # With a rules config (cluster + per-DB CPU weights, placement/EP policy, excludes):
    python balance.py plan --config ./rules.json   (see balance.config.json.sample)
"""
from __future__ import annotations

import argparse
import base64
import html as _htmlmod
import json
import math
import os
import re
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


# --------------------------------------------------------------------------- #
# Resource model constants (vCPU weights per the cluster sizing policy)
# --------------------------------------------------------------------------- #
SHARD_VCPU = 1.0       # default vCPU per shard (master or replica); configurable
ENDPOINT_VCPU = 1.5    # default vCPU per endpoint (proxy); configurable


# --------------------------------------------------------------------------- #
# Configuration (two levels: cluster-wide defaults + per-database overrides)
#
# JSON file, e.g.:
#   {
#     "cluster":   { "shard_cpu": 1.0, "endpoint_cpu": 1.5,
#                    "respect_shard_placement": true, "respect_endpoint_policy": true },
#     "databases": { "4":   { "shard_cpu": 0.5, "respect_shard_placement": false },
#                    "clm": { "exclude_from_balancing": true } }
#   }
# A database entry (keyed by uid or name) OVERRIDES the cluster defaults.
#
# Configurable rules:
#   shard_cpu / endpoint_cpu      - vCPU weight per shard / endpoint.
#   respect_shard_placement       - true: keep dense/sparse. false: tool may
#                                   relax it (e.g. co-locate a small DB's shards,
#                                   suggesting a sparse->dense change).
#   respect_endpoint_policy       - true: endpoints follow the proxy policy.
#                                   false: endpoints may be relocated freely
#                                   (suggesting a proxy-policy change).
#   consolidate_dense             - true (default): PRIORITISE dense placement -
#                                   pack a dense DB's shards onto as few nodes as
#                                   possible (endpoint follows), even if that lowers
#                                   the raw balance score. false: only prevent
#                                   spreading; do not actively pack. Needs
#                                   respect_shard_placement=true to apply.
#   exclude_from_balancing        - true: NEVER migrate this DB's shards/endpoints;
#                                   they still consume resources (counted), just pinned.
# CLUSTER-ONLY (ignored under 'databases'):
#   exclude_nodes                      - list of node uids the rebalancer must not use:
#                                        nothing migrates ONTO them and their shards are
#                                        left in place (like a DOWN/quorum node). They drop
#                                        out of the balance pool (score/feasibility/targets).
#                                        Also settable on the CLI: --exclude-nodes 3,5.
#   shard_migration_check_timeout      - seconds to wait, after each shard migration /
#                                        failover during 'execute', for the cluster
#                                        status to return to OK before aborting (default 30).
#   endpoint_migration_check_timeout   - same, after each endpoint re-bind (default 30).
#   rest_post_op_settle_seconds        - REST execute only: minimum quiet period held
#                                        after each op before the next one, even once the
#                                        status looks OK. The REST API cannot observe the
#                                        proxy/DMC routing reconciliation (rladmin's
#                                        WATCHDOG 'multiple nodes') that lingers after a
#                                        migration completes, so this bounded hold gives
#                                        it time to clear (default 15; capped by the
#                                        matching *_migration_check_timeout).
# ALWAYS-ON (not configurable): master/replica anti-affinity; rack-awareness
# (when the cluster has it enabled). Setting these false in config is ignored.
# --------------------------------------------------------------------------- #
DEFAULT_CHECK_TIMEOUT = 30          # seconds; deploy health-gate budget per op (see below)
DEFAULT_REST_POST_OP_SETTLE = 15    # seconds; REST-only minimum settle hold after each op

CONFIG_DEFAULTS = {
    # resources
    "shard_cpu": SHARD_VCPU,
    "endpoint_cpu": ENDPOINT_VCPU,
    # placement policy
    "respect_shard_placement": True,
    "consolidate_dense": True,
    "respect_endpoint_policy": True,
    # scope
    "exclude_from_balancing": False,
    "exclude_nodes": [],                       # CLUSTER-ONLY: node uids to leave untouched
    # deploy health-gate budgets (CLUSTER-ONLY; no per-DB override)
    "shard_migration_check_timeout": DEFAULT_CHECK_TIMEOUT,
    "endpoint_migration_check_timeout": DEFAULT_CHECK_TIMEOUT,
    "rest_post_op_settle_seconds": DEFAULT_REST_POST_OP_SETTLE,
}
_LOCKED_RULES = ("anti_affinity", "rack_awareness")
# Keys that only make sense cluster-wide; a per-DB entry cannot override them.
_CLUSTER_ONLY = ("shard_migration_check_timeout", "endpoint_migration_check_timeout",
                 "rest_post_op_settle_seconds", "exclude_nodes")


class Config:
    """Two-level rule config: cluster defaults overlaid by per-DB overrides."""

    def __init__(self, raw: Optional[Dict[str, Any]] = None) -> None:
        raw = raw or {}
        self.cluster = {**CONFIG_DEFAULTS, **(raw.get("cluster") or {})}
        self.databases = raw.get("databases") or {}
        self._cache: Dict[int, Dict[str, Any]] = {}
        for scope in (self.cluster, *(v for v in self.databases.values() if isinstance(v, dict))):
            for r in _LOCKED_RULES:
                if r in scope and not scope[r]:
                    sys.stderr.write(f"WARNING: '{r}' is always enforced and cannot be disabled; "
                                     "ignoring the config value.\n")
        for name, entry in self.databases.items():
            if isinstance(entry, dict):
                for k in _CLUSTER_ONLY:
                    if k in entry:
                        sys.stderr.write(f"WARNING: '{k}' is a cluster-only setting; ignoring "
                                         f"it under databases['{name}'] (set it in 'cluster').\n")

    def _for(self, db: "Database") -> Dict[str, Any]:
        if db.uid not in self._cache:
            eff = dict(self.cluster)
            override = self.databases.get(str(db.uid)) or self.databases.get(db.name) or {}
            if isinstance(override, dict):
                eff.update({k: v for k, v in override.items()
                            if k not in _LOCKED_RULES and k not in _CLUSTER_ONLY})
            self._cache[db.uid] = eff
        return self._cache[db.uid]

    def shard_cpu(self, db: "Database") -> float:
        return float(self._for(db).get("shard_cpu", SHARD_VCPU))

    def endpoint_cpu(self, db: "Database") -> float:
        return float(self._for(db).get("endpoint_cpu", ENDPOINT_VCPU))

    def respect_placement(self, db: "Database") -> bool:
        return bool(self._for(db).get("respect_shard_placement", True))

    def respect_endpoint(self, db: "Database") -> bool:
        return bool(self._for(db).get("respect_endpoint_policy", True))

    def consolidate_dense(self, db: "Database") -> bool:
        return bool(self._for(db).get("consolidate_dense", True))

    def excluded(self, db: "Database") -> bool:
        return bool(self._for(db).get("exclude_from_balancing", False))

    # Cluster-only (never per-DB): deploy health-gate budgets, in seconds.
    def shard_check_timeout(self) -> float:
        return float(self.cluster.get("shard_migration_check_timeout", DEFAULT_CHECK_TIMEOUT))

    def endpoint_check_timeout(self) -> float:
        return float(self.cluster.get("endpoint_migration_check_timeout", DEFAULT_CHECK_TIMEOUT))

    def rest_post_op_settle(self) -> float:
        return float(self.cluster.get("rest_post_op_settle_seconds", DEFAULT_REST_POST_OP_SETTLE))

    def excluded_nodes(self) -> set:
        """Node uids the rebalancer must not use (config 'exclude_nodes' and/or the
        --exclude-nodes CLI flag, merged into the cluster scope). Non-int entries are
        ignored."""
        out: set = set()
        for x in (self.cluster.get("exclude_nodes") or []):
            try:
                out.add(int(x))
            except (TypeError, ValueError):
                continue
        return out


def load_config(path: Optional[str]) -> Config:
    if not path:
        return Config()
    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except OSError as exc:
        raise SystemExit(f"Cannot read config file {path}: {exc}")
    except ValueError as exc:
        raise SystemExit(f"Config file {path} is not valid JSON: {exc}")
    if not isinstance(raw, dict):
        raise SystemExit(f"Config file {path} must be a JSON object.")
    return Config(raw)


# --------------------------------------------------------------------------- #
# rladmin client (read-only): live subprocess or pre-captured files
# --------------------------------------------------------------------------- #
class RladminClient:
    """Runs read-only `rladmin` commands live on a cluster node. For off-node
    analysis use --status-file (a captured `rladmin status extra all`)."""

    def __init__(self, rladmin_path: str = "rladmin") -> None:
        self.rladmin_path = rladmin_path
        # rladmin accepts the DB id as "<n>" on some versions and "db:<n>" on
        # others; the working form is probed once (info_db) then cached here.
        self._db_id_form: Optional[str] = None
        # Each rladmin invocation re-connects to the cluster (~1-2s), so the
        # results that discovery needs are fetched once and memoised here.
        self._cache: Dict[str, str] = {}

    def _command(self, args: List[str], required: bool = True, quiet: bool = False,
                 timeout: float = 60) -> str:
        try:
            proc = subprocess.run(
                [self.rladmin_path, *args],
                capture_output=True, text=True, timeout=timeout,
            )
        except FileNotFoundError:
            raise SystemExit(
                f"'{self.rladmin_path}' not found. Run this tool on a Redis Enterprise "
                "cluster node, pass --rladmin-path, or use --status-file with captured output."
            )
        except subprocess.TimeoutExpired:
            raise SystemExit(f"`rladmin {' '.join(args)}` did not respond within {timeout:.0f}s")
        except (subprocess.SubprocessError, OSError) as exc:
            raise SystemExit(f"Failed to run rladmin {' '.join(args)}: {exc}")
        if proc.returncode != 0:
            msg = (proc.stderr or proc.stdout).strip()[:400]
            if required:
                raise SystemExit(f"`rladmin {' '.join(args)}` exited {proc.returncode}: {msg}")
            if not quiet:
                sys.stderr.write(f"WARNING: `rladmin {' '.join(args)}` failed: {msg}\n")
            return ""
        return proc.stdout or ""

    def status_all(self, refresh: bool = False, timeout: float = 60) -> str:
        """ONE `rladmin status extra all` (CLUSTER NODES + DATABASES + ENDPOINTS
        + SHARDS) - replaces three separate status calls. Cached for the run;
        pass refresh=True to force a live re-read (used by the deploy health gate).
        timeout bounds the subprocess so a stuck check can't block indefinitely."""
        if refresh or "status_all" not in self._cache:
            self._cache["status_all"] = self._command(
                ["status", "extra", "all"], required=True, timeout=timeout)
        return self._cache["status_all"]

    def info_db_all(self) -> str:
        # Bulk config for all DBs in one call (see LIMITATION L5). Cached.
        if "info_db_all" not in self._cache:
            self._cache["info_db_all"] = self._command(["info", "db"], required=False)
        return self._cache["info_db_all"]

    def info_db(self, db_id: int) -> str:
        """`rladmin info db <id>`, tolerating both id forms ("<n>" and "db:<n>").
        Probes the bare-number form first, falls back to "db:<n>", and caches
        whichever works so later DBs skip the probe (and its warning)."""
        def valid(txt: str) -> bool:
            return bool(txt) and re.search(rf"\bdb:{db_id}\b", txt) is not None

        # Templates applied per id: "{}" -> "1", "db:{}" -> "db:1".
        templates = [self._db_id_form] if self._db_id_form else ["{}", "db:{}"]
        for tmpl in templates:
            txt = self._command(["info", "db", tmpl.format(db_id)], required=False, quiet=True)
            if valid(txt):
                self._db_id_form = tmpl  # cache the working form for later DBs
                return txt
        # Nothing parseable from either form: surface a real error on the last try.
        last = self._db_id_form or "db:{}"
        return self._command(["info", "db", last.format(db_id)], required=True)

    def info_cluster(self) -> str:
        if "info_cluster" not in self._cache:
            self._cache["info_cluster"] = self._command(["info", "cluster"], required=False)
        return self._cache["info_cluster"]

    def info_node(self, node_id: int) -> str:
        return self._command(["info", "node", str(node_id)], required=False)

    def execute(self, args: List[str]):
        """Run a MUTATING rladmin command (Step 4 deploy only). Returns
        (returncode, combined_output). The ONLY method that changes the cluster."""
        try:
            proc = subprocess.run(
                [self.rladmin_path, *args],
                capture_output=True, text=True, timeout=600,
            )
        except FileNotFoundError:
            raise SystemExit(f"'{self.rladmin_path}' not found; cannot execute deploy.")
        except (subprocess.SubprocessError, OSError) as exc:
            return 1, f"failed to run rladmin: {exc}"
        return proc.returncode, (proc.stdout or "") + (proc.stderr or "")


# --------------------------------------------------------------------------- #
# Inventory (static topology, discovered once)
# --------------------------------------------------------------------------- #
@dataclass
class Node:
    uid: int
    addr: str
    total_memory: int          # bytes, physical RAM of the node
    cores: int                 # available vCPU
    status: str = "unknown"
    rack_id: Optional[str] = None
    provisional_ram: Optional[int] = None   # RAM available for NEW shards
    max_shards: Optional[int] = None         # per-node shard ceiling (status nodes)
    hostable: bool = True                     # False for DOWN / quorum-only (see L7)

    @property
    def available_ram(self) -> int:
        if self.provisional_ram is not None:
            return self.provisional_ram
        return self.total_memory


@dataclass
class Database:
    uid: int
    name: str
    memory_size: int           # total dataset memory limit, bytes
    shards_count: int          # number of MASTER shards
    replication: bool
    sharding: bool
    shard_placement: str       # "dense" | "sparse"  (captured; see L8)
    proxy_policy: str          # endpoint placement policy
    db_type: str               # "redis" | "memcached"
    is_flex: bool              # bigstore / Auto Tiering -> out of scope

    @property
    def per_shard_memory(self) -> int:
        # LIMITATION L1: even-split provisioning limit, not live footprint.
        if self.shards_count <= 0:
            return 0
        return self.memory_size // self.shards_count


@dataclass
class Shard:
    uid: int
    role: str                  # "master" | "slave"
    bdb_uid: int
    node_uid: int              # current node (the current placement)
    used_memory: int = 0
    slots: str = ""            # hash-slot range; identifies the HA group

    @property
    def ha_group(self) -> Tuple[int, str]:
        """Master and its replica(s) share (bdb_uid, slots) and must not
        co-locate. Used by the Step 3 planner for anti-affinity."""
        return (self.bdb_uid, self.slots)


@dataclass
class Cluster:
    name: str
    nodes: List[Node] = field(default_factory=list)
    databases: List[Database] = field(default_factory=list)
    shards: List[Shard] = field(default_factory=list)
    rack_aware: bool = False
    ram_source: str = "rladmin status nodes (PROVISIONAL_RAM)"
    config: Any = None   # Config; set during discovery (defaults if no --config)
    # ACTUAL endpoint node uids per DB, read from the ENDPOINTS section / REST
    # endpoints[] during discovery. Used to detect endpoint<->master misalignment
    # (empty for a DB -> derive from policy, backward compatible).
    endpoints_by_db: Dict[int, set] = field(default_factory=dict)
    # Indexes (built by index()) - avoid O(n) scans in hot loops at scale.
    node_by_uid: Dict[int, Node] = field(default_factory=dict)
    db_by_uid: Dict[int, Database] = field(default_factory=dict)
    shards_by_bdb: Dict[int, List[Shard]] = field(default_factory=dict)

    def index(self) -> None:
        self.node_by_uid = {n.uid: n for n in self.nodes}
        self.db_by_uid = {d.uid: d for d in self.databases}
        by_bdb: Dict[int, List[Shard]] = defaultdict(list)
        for s in self.shards:
            by_bdb[s.bdb_uid].append(s)
        self.shards_by_bdb = dict(by_bdb)

    def node(self, uid: int) -> Optional[Node]:
        return self.node_by_uid.get(uid)

    def db(self, uid: int) -> Optional[Database]:
        return self.db_by_uid.get(uid)


# --------------------------------------------------------------------------- #
# Placement + derived per-node load (pure functions of inventory + placement)
# --------------------------------------------------------------------------- #
@dataclass
class Placement:
    """Maps shard_uid -> node_uid. The current layout is one instance; Step 2
    produces alternative candidate placements scored the same way."""
    shard_node: Dict[int, int]

    @classmethod
    def current(cls, cluster: Cluster) -> "Placement":
        return cls({s.uid: s.node_uid for s in cluster.shards})


@dataclass
class NodeLoad:
    provisioned_memory: int = 0
    used_memory: int = 0
    master_shards: int = 0
    replica_shards: int = 0
    endpoints: int = 0
    vcpu: float = 0.0   # weighted required vCPU (shards*shard_cpu + endpoints*endpoint_cpu)

    @property
    def total_shards(self) -> int:
        return self.master_shards + self.replica_shards


@dataclass
class NodeView:
    """A node's static inventory combined with its load under one placement.
    Carries the per-node accessors used by scoring and rendering."""
    node: Node
    load: NodeLoad

    # static passthrough
    @property
    def uid(self) -> int: return self.node.uid
    @property
    def addr(self) -> str: return self.node.addr
    @property
    def cores(self) -> int: return self.node.cores
    @property
    def status(self) -> str: return self.node.status
    @property
    def rack_id(self) -> Optional[str]: return self.node.rack_id
    @property
    def total_memory(self) -> int: return self.node.total_memory
    @property
    def provisional_ram(self) -> Optional[int]: return self.node.provisional_ram
    @property
    def max_shards(self) -> Optional[int]: return self.node.max_shards

    # dynamic passthrough
    @property
    def provisioned_memory(self) -> int: return self.load.provisioned_memory
    @property
    def used_memory(self) -> int: return self.load.used_memory
    @property
    def master_shards(self) -> int: return self.load.master_shards
    @property
    def replica_shards(self) -> int: return self.load.replica_shards
    @property
    def endpoints(self) -> int: return self.load.endpoints
    @property
    def total_shards(self) -> int: return self.load.total_shards

    # computed
    @property
    def available_ram(self) -> int: return self.node.available_ram
    @property
    def ram_capacity(self) -> int:
        if self.node.provisional_ram is not None:
            return self.load.provisioned_memory + self.node.provisional_ram
        return self.node.total_memory
    @property
    def required_vcpu(self) -> float:
        return self.load.vcpu   # weighted (per-DB shard_cpu / endpoint_cpu)
    @property
    def free_vcpu(self) -> float:
        return self.node.cores - self.required_vcpu


# --------------------------------------------------------------------------- #
# rladmin output parsing helpers
# --------------------------------------------------------------------------- #
def parse_mem_token(tok: str) -> Optional[int]:
    """Parse an rladmin memory token ('8.2GB', '512MB', '0', or 'X/Y' -> X)."""
    tok = (tok or "").strip()
    if not tok or tok in ("-", "N/A", "n/a"):
        return None
    if "/" in tok:
        tok = tok.split("/", 1)[0].strip()
    mult = {"B": 1, "KB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3,
            "TB": 1024 ** 4, "PB": 1024 ** 5}
    # Prefer a number+unit token (e.g. '1.00GB'); tolerates surrounding text such
    # as the '1073741824 (1.00GB)' form some rladmin versions print.
    m = re.search(r"(-?[0-9]*\.?[0-9]+)\s*([KMGTP]?B)\b", tok, re.IGNORECASE)
    if m:
        return int(float(m.group(1)) * mult[m.group(2).upper()])
    # No unit anywhere -> treat a bare leading integer as bytes ('1073741824').
    m = re.search(r"-?[0-9]+", tok)
    return int(m.group(0)) if m else None


def _ratio_parts(cell: str) -> Tuple[str, str]:
    if "/" in (cell or ""):
        a, b = cell.split("/", 1)
        return a.strip(), b.strip()
    return (cell or "").strip(), ""


def _id_from(cell: str) -> Optional[int]:
    m = re.search(r"(\d+)", cell or "")
    return int(m.group(1)) if m else None


def _coerce_bool(val: Optional[str]) -> bool:
    if val is None:
        return False
    v = val.strip().lower()
    if v in ("enabled", "true", "yes", "on"):
        return True
    if v in ("disabled", "false", "no", "off", "", "0"):
        return False
    m = re.match(r"^(\d+)", v)
    return bool(m and int(m.group(1)) > 0)


def parse_status_table(text: str, sentinel: str) -> List[Dict[str, str]]:
    """Parse a single fixed-width `rladmin status ...` table into row dicts.

    Columns are keyed off header-token START positions, robust to empty cells
    (e.g. a blank EXTERNAL_ADDRESS) that break naive whitespace splits.
    """
    lines = text.splitlines()
    header_idx = next((i for i, l in enumerate(lines) if sentinel in l), None)
    if header_idx is None:
        return []
    header = lines[header_idx]
    cols = [(m.group(), m.start()) for m in re.finditer(r"\S+", header)]
    spans = [
        (name, start, cols[j + 1][1] if j + 1 < len(cols) else None)
        for j, (name, start) in enumerate(cols)
    ]
    first_col = spans[0][0]
    rows: List[Dict[str, str]] = []
    for line in lines[header_idx + 1:]:
        if not line.strip():
            continue
        row = {
            name: (line[start:end] if end is not None else line[start:]).strip()
            for name, start, end in spans
        }
        if row.get(first_col):
            rows.append(row)
    return rows


def parse_info_block(text: str) -> Dict[str, str]:
    """Parse an `rladmin info ...` block into a flat {key: value} dict.

    Only indented top-level 'key: value' lines with identifier keys are kept
    (nested list items and the 'db:<id> [name]:' header are skipped).
    """
    kv: Dict[str, str] = {}
    for line in text.splitlines():
        if not re.match(r"^\s+\S", line):
            continue
        if ":" not in line:
            continue
        key, _, val = line.strip().partition(":")
        key = key.strip().lower()
        if re.fullmatch(r"[a-z0-9_]+", key):
            kv[key] = val.strip()
    return kv


def parse_info_db_blocks(text: str) -> Dict[int, str]:
    """Split bulk `rladmin info db` output into {db_id: block_text} (see L5)."""
    blocks: Dict[int, str] = {}
    current: Optional[int] = None
    buf: List[str] = []
    for line in text.splitlines():
        m = re.match(r"^\*?db:(\d+)\b", line)
        if m:
            if current is not None:
                blocks[current] = "\n".join(buf)
            current = int(m.group(1))
            buf = [line]
        elif current is not None:
            buf.append(line)
    if current is not None:
        blocks[current] = "\n".join(buf)
    return blocks


def _info_db_name(text: str) -> Optional[str]:
    m = re.search(r"db:\d+\s*\[([^\]]*)\]", text)
    return m.group(1) if m else None


# --------------------------------------------------------------------------- #
# Discovery (rladmin-only)
# --------------------------------------------------------------------------- #
def _build_node(row: Dict[str, str]) -> Optional[Node]:
    uid = _id_from(row.get("NODE:ID", ""))
    if uid is None:
        return None
    _, total_tok = _ratio_parts(row.get("FREE_RAM", ""))
    total_memory = parse_mem_token(total_tok) or 0
    provisional = parse_mem_token(row.get("PROVISIONAL_RAM", ""))
    _, max_tok = _ratio_parts(row.get("SHARDS", ""))
    max_shards = int(max_tok) if max_tok.isdigit() else None
    try:
        cores = int(re.search(r"\d+", row.get("CORES", "0")).group())  # type: ignore[union-attr]
    except (AttributeError, ValueError):
        cores = 0
    status = (row.get("STATUS", "unknown") or "unknown").lower()
    # LIMITATION L7: heuristic non-hostable detection.
    hostable = status != "down" and max_shards != 0
    return Node(
        uid=uid,
        addr=row.get("ADDRESS", "?") or "?",
        total_memory=total_memory,
        cores=cores,
        status=status,
        provisional_ram=provisional,
        max_shards=max_shards,
        hostable=hostable,
    )


def _build_database(db_id: int, status_row: Dict[str, str], info_text: str,
                    masters_for_db: int) -> Database:
    """Merge the two rladmin sources for a DB:

      * `rladmin status databases` row  -> memory_size, replication, type, shards,
        placement (these columns are NOT in `rladmin info db` on 8.0.x).
      * `rladmin info db` block (kv)     -> proxy_policy, bigstore (and fallbacks).
    """
    kv = parse_info_block(info_text)
    row = status_row or {}
    name = (row.get("NAME") or _info_db_name(info_text) or kv.get("name")
            or f"bdb:{db_id}")

    # shards_count: status SHARDS column, else info-db, else observed master count.
    shards_count = _id_from(row.get("SHARDS", "")) or 0
    if shards_count <= 0 and kv.get("shards_count", "").strip().isdigit():
        shards_count = int(kv["shards_count"].strip())
    if shards_count <= 0:
        shards_count = masters_for_db

    # memory_size + replication + type come from status (info db lacks them on 8.0.x).
    memory_size = parse_mem_token(row.get("MEMORY_SIZE") or row.get("MEMORY")
                                  or row.get("MEMORY_LIMIT") or "")
    if not memory_size:
        memory_size = parse_mem_token(kv.get("memory_size", "")) or 0
    replication = (_coerce_bool(row.get("REPLICATION")) if row.get("REPLICATION") is not None
                   else _coerce_bool(kv.get("replication")))
    db_type = (row.get("TYPE") or kv.get("type") or "redis").lower()
    placement = (row.get("PLACEMENT") or kv.get("shards_placement") or "dense").lower()
    proxy_policy = (kv.get("proxy_policy") or row.get("PROXY_POLICY") or "single").lower()

    bigstore = _coerce_bool(kv.get("bigstore")) or (parse_mem_token(kv.get("bigstore_ram_size", "")) or 0) > 0
    flex_col = row.get("BIGSTORE") or row.get("FLASH") or row.get("AUTO_TIERING") or ""
    return Database(
        uid=db_id,
        name=name,
        memory_size=memory_size,
        shards_count=shards_count,
        replication=replication,
        sharding=shards_count > 1,
        shard_placement=placement,
        proxy_policy=proxy_policy,
        db_type=db_type,
        is_flex=bigstore or _coerce_bool(flex_col),
    )


def discover(client: RladminClient) -> Cluster:
    """Read the full cluster topology via rladmin (read-only)."""
    cluster = Cluster(name="redis-enterprise-cluster")
    cluster.config = Config()  # default; overridden by --config in run()

    # These three reads are independent and each `rladmin` invocation reconnects to
    # the cluster (~1-2s), so fetch them CONCURRENTLY: wall-clock drops from the sum
    # of the calls to the slowest single one. Results are cached on the client, so
    # this only warms the cache - later accessors just read it back.
    #   info_cluster    -> name + rack-awareness (best-effort)
    #   status_all      -> ONE `status extra all`: NODES + DATABASES + SHARDS + ENDPOINTS
    #   info_db_all     -> bulk per-DB config (proxy_policy); see L5 fallback below
    with ThreadPoolExecutor(max_workers=3) as ex:
        f_cluster = ex.submit(client.info_cluster)
        f_status = ex.submit(client.status_all)
        f_bulk = ex.submit(client.info_db_all)
        info_cluster = f_cluster.result()   # best-effort ("" on failure)
        status_text = f_status.result()     # required: raises SystemExit on failure
        bulk = f_bulk.result()              # best-effort ("" on failure)

    # Cluster-level (best-effort): name + rack-awareness.
    if info_cluster:
        ckv = parse_info_block(info_cluster)
        cluster.rack_aware = _coerce_bool(ckv.get("rack_aware"))
        if ckv.get("name"):
            cluster.name = ckv["name"]

    secs = split_status_sections(status_text)

    # Nodes (+ rack id straight from the RACK-ID column when present).
    for row in parse_status_table(_find_section(secs, "NODES"), "NODE:ID"):
        node = _build_node(row)
        if node is None:
            continue
        rk = row.get("RACK_ID") or row.get("RACK-ID") or row.get("RACK") or ""
        if rk and rk not in ("-", "N/A", "n/a"):
            node.rack_id = rk
            cluster.rack_aware = True
        cluster.nodes.append(node)

    # Shards (also a fallback shard-count source for databases).
    masters_per_db: Dict[int, int] = defaultdict(int)
    for row in parse_status_table(_find_section(secs, "SHARDS"), "DB:ID"):
        bdb_uid = _id_from(row.get("DB:ID", ""))
        node_uid = _id_from(row.get("NODE", ""))
        shard_uid = _id_from(row.get("ID", ""))
        if bdb_uid is None or node_uid is None or shard_uid is None:
            continue
        role = (row.get("ROLE", "master") or "master").lower()
        cluster.shards.append(Shard(
            uid=shard_uid, role=role, bdb_uid=bdb_uid, node_uid=node_uid,
            used_memory=parse_mem_token(row.get("USED_MEMORY", "")) or 0,
            slots=row.get("SLOTS", "") or "",
        ))
        if role == "master":
            masters_per_db[bdb_uid] += 1

    # Actual endpoint placement (for endpoint<->master alignment detection).
    ep_nodes_per_db, _ = parse_endpoint_nodes(secs)
    cluster.endpoints_by_db = {uid: set(v) for uid, v in ep_nodes_per_db.items()}

    # Databases: status columns (memory/replication/type), enriched with the bulk
    # `info db` block (proxy_policy) fetched above. One `info db` covers all DBs.
    bulk_blocks = parse_info_db_blocks(bulk) if bulk else {}
    for row in parse_status_table(_find_section(secs, "DATABASES"), "DB:ID"):
        db_id = _id_from(row.get("DB:ID", ""))
        if db_id is None:
            continue
        info_text = bulk_blocks.get(db_id) or client.info_db(db_id)  # L5 fallback
        cluster.databases.append(
            _build_database(db_id, row, info_text, masters_per_db.get(db_id, 0))
        )

    # LIMITATION L6: rack id only needs a per-node `info node` if rack-aware AND
    # the status table didn't already provide it (older rladmin without RACK-ID).
    if cluster.rack_aware:
        for node in cluster.nodes:
            if node.rack_id:
                continue
            txt = client.info_node(node.uid)
            if txt:
                node.rack_id = parse_info_block(txt).get("rack_id") or node.rack_id

    cluster.index()
    return cluster


# --------------------------------------------------------------------------- #
# Discovery from a single `rladmin status [extra all]` capture file
#
# `rladmin status extra all > file` emits ALL sections in one file (CLUSTER
# NODES / DATABASES / ENDPOINTS / SHARDS). It does NOT include the per-DB
# configured memory LIMIT or proxy_policy (those come from `rladmin info db`),
# so this mode derives:
#   * shard memory  <- USED_MEMORY column (ACTUAL usage, not the limit)
#   * endpoint nodes <- the real ENDPOINTS section (no proxy-policy guess)
#   * proxy_policy   <- inferred from the endpoint vs master node sets
#   * rack_id        <- node RACK_ID column if present
# Configured memory limit / flex / proxy_policy are read from columns if the
# capture happens to include them, else fall back as above. For exact limits,
# proxy_policy and rack ids, use the live rladmin or --rest input instead.
# --------------------------------------------------------------------------- #
def split_status_sections(text: str) -> Dict[str, str]:
    """Split combined `rladmin status` output into {SECTION HEADER: body}."""
    header_re = re.compile(r"^[A-Z][A-Z0-9 /_-]*:$")
    sections: Dict[str, str] = {}
    cur: Optional[str] = None
    buf: List[str] = []
    for line in text.splitlines():
        s = line.strip()
        if header_re.match(s):  # e.g. "CLUSTER NODES:", "DATABASES:", "SHARDS:"
            if cur is not None:
                sections[cur] = "\n".join(buf)
            cur = s[:-1].strip()
            buf = []
        elif cur is not None:
            buf.append(line)
    if cur is not None:
        sections[cur] = "\n".join(buf)
    return sections


def _find_section(sections: Dict[str, str], *keywords: str) -> str:
    for name, body in sections.items():
        if all(k in name for k in keywords):
            return body
    return ""


_KNOWN_POLICIES = ("single", "all-master-shards", "all-master-proxies", "all-nodes")


def parse_endpoint_nodes(secs: Dict[str, str]) -> Tuple[Dict[int, set], Dict[int, str]]:
    """From a split `rladmin status` ENDPOINTS section return the ACTUAL endpoint
    placement: ({db_uid: {node_uids}}, {db_uid: proxy_policy_if_present})."""
    ep_nodes: Dict[int, set] = defaultdict(set)
    ep_policy: Dict[int, str] = {}
    for row in parse_status_table(_find_section(secs, "ENDPOINTS"), "DB:ID"):
        bdb_uid = _id_from(row.get("DB:ID", ""))
        node_uid = _id_from(row.get("NODE", ""))
        if bdb_uid is None or node_uid is None:
            continue
        ep_nodes[bdb_uid].add(node_uid)
        pol = (row.get("ROLE") or row.get("POLICY") or "").strip().lower()
        if pol in _KNOWN_POLICIES:
            ep_policy[bdb_uid] = pol
    return dict(ep_nodes), ep_policy


def _infer_proxy_policy(ep_nodes: set, master_nodes: set, n_nodes: int) -> str:
    if len(ep_nodes) <= 1:
        return "single"
    if ep_nodes == master_nodes:
        return "all-master-shards"
    if len(ep_nodes) >= n_nodes:
        return "all-nodes"
    return "all-master-shards"


def discover_from_status_file(path: str) -> Cluster:
    """Build the cluster inventory from a single `rladmin status [extra all]` file."""
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            text = fh.read()
    except OSError as exc:
        raise SystemExit(f"Cannot read status file {path}: {exc}")

    secs = split_status_sections(text)
    cluster = Cluster(
        name="redis-enterprise-cluster",
        ram_source="rladmin status (PROVISIONAL_RAM); shard memory = USED_MEMORY "
                   "(no configured limit in status output)",
    )
    cluster.config = Config()  # default; overridden by --config in run()

    nodes_body = _find_section(secs, "NODES")
    if not nodes_body:
        raise SystemExit(
            f"{path} does not look like `rladmin status` output (no CLUSTER NODES section). "
            "Generate it with: rladmin status extra all > " + os.path.basename(path))
    for row in parse_status_table(nodes_body, "NODE:ID"):
        node = _build_node(row)
        if node is None:
            continue
        rk = row.get("RACK_ID") or row.get("RACK-ID") or row.get("RACK") or ""
        if rk and rk not in ("-", "N/A", "n/a"):
            node.rack_id = rk
            cluster.rack_aware = True
        cluster.nodes.append(node)

    # Shards: also gather per-DB master counts, master nodes, and used memory.
    masters_per_db: Dict[int, int] = defaultdict(int)
    master_nodes_per_db: Dict[int, set] = defaultdict(set)
    master_used_per_db: Dict[int, int] = defaultdict(int)
    for row in parse_status_table(_find_section(secs, "SHARDS"), "DB:ID"):
        bdb_uid = _id_from(row.get("DB:ID", ""))
        node_uid = _id_from(row.get("NODE", ""))
        shard_uid = _id_from(row.get("ID", ""))
        if bdb_uid is None or node_uid is None or shard_uid is None:
            continue
        role = (row.get("ROLE", "master") or "master").lower()
        used = parse_mem_token(row.get("USED_MEMORY", "")) or 0
        cluster.shards.append(Shard(
            uid=shard_uid, role=role, bdb_uid=bdb_uid, node_uid=node_uid,
            used_memory=used, slots=row.get("SLOTS", "") or "",
        ))
        if role == "master":
            masters_per_db[bdb_uid] += 1
            master_nodes_per_db[bdb_uid].add(node_uid)
            master_used_per_db[bdb_uid] += used

    # Endpoints: real per-DB endpoint -> node placement (and policy if present).
    ep_nodes_per_db, ep_policy_per_db = parse_endpoint_nodes(secs)
    cluster.endpoints_by_db = {uid: set(v) for uid, v in ep_nodes_per_db.items()}

    n_nodes = len(cluster.nodes)
    for row in parse_status_table(_find_section(secs, "DATABASES"), "DB:ID"):
        db_id = _id_from(row.get("DB:ID", ""))
        if db_id is None:
            continue
        shards_count = masters_per_db.get(db_id, 0)
        if shards_count <= 0:  # fall back to the SHARDS column "used/total"
            first, _ = _ratio_parts(row.get("SHARDS", ""))
            shards_count = int(first) if first.isdigit() else 0
        # Configured memory limit if present, else ACTUAL used memory of masters.
        mem_col = parse_mem_token(row.get("MEMORY") or row.get("MEMORY_SIZE")
                                  or row.get("MEMORY_LIMIT") or "")
        memory_size = mem_col if mem_col else master_used_per_db.get(db_id, 0)
        proxy_policy = ((row.get("PROXY_POLICY") or "").strip().lower()
                        or ep_policy_per_db.get(db_id)
                        or _infer_proxy_policy(ep_nodes_per_db.get(db_id, set()),
                                               master_nodes_per_db.get(db_id, set()), n_nodes))
        flex_col = row.get("BIGSTORE") or row.get("FLASH") or row.get("AUTO_TIERING") or ""
        cluster.databases.append(Database(
            uid=db_id,
            name=row.get("NAME", f"bdb:{db_id}") or f"bdb:{db_id}",
            memory_size=memory_size,
            shards_count=shards_count,
            replication=_coerce_bool(row.get("REPLICATION")),
            sharding=shards_count > 1,
            shard_placement=(row.get("PLACEMENT", "dense") or "dense").lower(),
            proxy_policy=proxy_policy,
            db_type=(row.get("TYPE", "redis") or "redis").lower(),
            is_flex=_coerce_bool(flex_col),
        ))

    cluster.index()
    return cluster


# --------------------------------------------------------------------------- #
# Scope, requirements, and load (pure over inventory + placement)
# --------------------------------------------------------------------------- #
def in_scope_databases(cluster: Cluster) -> List[Database]:
    """RAM redis DBs the tool accounts for (counted in loads). Includes excluded
    DBs - they consume resources; they are just pinned (see movable_databases)."""
    return [d for d in cluster.databases if not d.is_flex and d.db_type == "redis"]


def out_of_scope_databases(cluster: Cluster) -> List[Database]:
    return [d for d in cluster.databases if d.is_flex or d.db_type != "redis"]


def movable_databases(cluster: Cluster) -> List[Database]:
    """In-scope DBs whose shards/endpoints the plan may migrate (excludes any DB
    marked exclude_from_balancing)."""
    return [d for d in in_scope_databases(cluster) if not cluster.config.excluded(d)]


def placement_endpoint_nodes(cluster: Cluster, placement: Placement, db: Database) -> set:
    """Node uids hosting an endpoint for this DB UNDER THE GIVEN PLACEMENT.

    Placement-dependent because 'all-master-shards' endpoints follow the master
    shards' nodes. See LIMITATIONS L2/L3.
    """
    master_nodes = {
        placement.shard_node.get(s.uid)
        for s in cluster.shards_by_bdb.get(db.uid, [])
        if s.role == "master"
    }
    master_nodes.discard(None)
    if db.proxy_policy in ("all-master-shards", "all-master-proxies"):
        return set(master_nodes)
    if db.proxy_policy == "all-nodes":
        return {n.uid for n in cluster.nodes}
    # 'single' (or unrecognised): one endpoint, co-located with a master node.
    return set(sorted(master_nodes)[:1])


def database_requirements(cluster: Cluster, placement: Placement, db: Database) -> Dict[str, Any]:
    """Total RAM and vCPU a database requires (shards + endpoints).

    RAM: memory_size * (2 if replication else 1). Endpoints carry no per-database
         RAM reservation in this model. CPU: shards*1 + endpoints*1.5.
    """
    replicas = 2 if db.replication else 1
    total_shards = db.shards_count * replicas
    n_endpoints = len(placement_endpoint_nodes(cluster, placement, db))
    return {
        "memory": db.memory_size * replicas,
        "vcpu": total_shards * cluster.config.shard_cpu(db)
                + n_endpoints * cluster.config.endpoint_cpu(db),
        "total_shards": total_shards,
        "endpoints": n_endpoints,
    }


def compute_loads(cluster: Cluster, placement: Placement,
                  endpoint_nodes: Optional[Dict[int, set]] = None) -> Dict[int, NodeLoad]:
    """Fold in-scope shards + endpoints onto nodes for the given placement.
    vCPU is weighted by the per-DB shard_cpu / endpoint_cpu config.

    endpoint_nodes: optional {db_uid: {node_uids}} override for endpoint placement -
    pass cluster.endpoints_by_db to score the ACTUAL (as-discovered) endpoint
    locations (revealing endpoint<->master misalignment). When omitted, or for a DB
    not present in it, endpoint nodes are DERIVED from the proxy policy + masters."""
    loads: Dict[int, NodeLoad] = {n.uid: NodeLoad() for n in cluster.nodes}
    scope_uids = {d.uid for d in in_scope_databases(cluster)}

    for shard in cluster.shards:
        if shard.bdb_uid not in scope_uids:
            continue
        node_uid = placement.shard_node.get(shard.uid)
        load = loads.get(node_uid) if node_uid is not None else None
        db = cluster.db_by_uid.get(shard.bdb_uid)
        if load is None or db is None:
            continue
        load.provisioned_memory += db.per_shard_memory
        load.used_memory += shard.used_memory
        load.vcpu += cluster.config.shard_cpu(db)
        if shard.role == "master":
            load.master_shards += 1
        else:
            load.replica_shards += 1

    for db in in_scope_databases(cluster):
        ep_cpu = cluster.config.endpoint_cpu(db)
        if endpoint_nodes is not None and db.uid in endpoint_nodes:
            ep_uids = endpoint_nodes[db.uid]
        else:
            ep_uids = placement_endpoint_nodes(cluster, placement, db)
        for uid in ep_uids:
            if uid in loads:
                loads[uid].endpoints += 1
                loads[uid].vcpu += ep_cpu
    return loads


def build_node_views(cluster: Cluster, placement: Placement,
                     endpoint_nodes: Optional[Dict[int, set]] = None) -> List[NodeView]:
    loads = compute_loads(cluster, placement, endpoint_nodes)
    return [NodeView(n, loads[n.uid]) for n in cluster.nodes]


# --------------------------------------------------------------------------- #
# Scoring
# --------------------------------------------------------------------------- #
def _coeff_of_variation(values: List[float]) -> float:
    vals = [float(v) for v in values]
    if not vals:
        return 0.0
    mean = sum(vals) / len(vals)
    if mean == 0:
        return 0.0
    var = sum((v - mean) ** 2 for v in vals) / len(vals)
    return math.sqrt(var) / mean


@dataclass
class BalanceScore:
    """Cluster-wide balance score, 0..100 (higher == more balanced).

    Balance == evenness of resource UTILISATION across nodes (CV of per-node
    utilisation; CV==0 is perfectly even). Each component maps to 0..100 via
    100*(1 - min(CV, 1)). Scored resources (equally weighted):
      * memory -> provisioned_memory / ram_capacity   (capacity uses PROVISIONAL_RAM)
      * cpu    -> required_vcpu / cores                (shards*1 + endpoints*1.5)
    """
    memory_cv: float
    cpu_cv: float
    memory_score: float
    cpu_score: float
    overall: float

    @property
    def is_balanced(self) -> bool:
        return self.memory_cv <= 0.05 and self.cpu_cv <= 0.05


def score_layout(views: List[NodeView]) -> BalanceScore:
    active = [v for v in views if v.status != "down"]
    mem_util = [v.provisioned_memory / v.ram_capacity for v in active if v.ram_capacity > 0]
    cpu_util = [v.required_vcpu / v.cores for v in active if v.cores > 0]

    memory_cv = _coeff_of_variation(mem_util)
    cpu_cv = _coeff_of_variation(cpu_util)
    memory_score = 100.0 * (1.0 - min(memory_cv, 1.0))
    cpu_score = 100.0 * (1.0 - min(cpu_cv, 1.0))
    return BalanceScore(
        memory_cv=memory_cv, cpu_cv=cpu_cv,
        memory_score=memory_score, cpu_score=cpu_score,
        overall=(memory_score + cpu_score) / 2.0,
    )


def capacity_warnings(views: List[NodeView]) -> List[str]:
    warns: List[str] = []
    for v in sorted(views, key=lambda x: x.uid):
        if v.status == "down":
            continue
        if v.provisional_ram is not None and v.provisional_ram < 0:
            warns.append(
                f"node {v.uid}: RAM over-committed (PROVISIONAL_RAM negative: "
                f"{fmt_bytes(v.provisional_ram)})"
            )
        if v.cores > 0 and v.required_vcpu > v.cores:
            warns.append(
                f"node {v.uid}: CPU over-subscribed (requires {v.required_vcpu:g} vCPU, "
                f"has {v.cores})"
            )
        if v.max_shards is not None and v.total_shards > v.max_shards:
            warns.append(
                f"node {v.uid}: shard limit exceeded ({v.total_shards} > max {v.max_shards})"
            )
    return warns


# --------------------------------------------------------------------------- #
# STEP 2: desired layout via constraint-aware local search
#
# Hard constraints (NEVER violated): HA anti-affinity, rack-awareness,
# shard-placement (dense/sparse), endpoint/proxy policy, per-node RAM ceiling,
# per-node CPU cores (hard limit), per-node shard limit, hostable-only targets.
# Objective (within the feasible region): minimise memory-CV + CPU-CV equally.
# Tiebreaker: fewest moves.
#
# Allowed operations (operational policy): only SINGLE, low-risk steps -
#   * move ONE shard, or
#   * relocate ONE endpoint (single-proxy DBs only; no data movement).
# Shard SWAPS are intentionally NOT used (too risky / less preferred). When the
# cluster is so full that no helpful shard can be migrated, the tool ALERTS and
# recommends added capacity instead of swapping.
#
# LIMITATIONS (Step 2 specific):
#   L9   Heuristic greedy descent seeded from current; finds a good, not provably
#        optimal, layout. Candidate pruning (hot->cold nodes) may miss some moves.
#   L10  Only single shard moves and single endpoint relocations are generated
#        (no swaps, by design). On RAM-saturated clusters that would need a swap
#        to improve, the tool reports an ALERT rather than rebalancing.
#   L11  dense/sparse are HARD constraints, never broken:
#        - dense: a shard may only move to a node that ALREADY hosts that DB, so
#          the DB's node footprint can shrink (consolidate) but never grow. A
#          dense DB is therefore never spread across new nodes to chase balance.
#        - sparse: even spread, capped at ceil(total_shards / hostable_nodes)
#          shards of the DB per node.
#        Both are enforced even under --force (they are policy, not resources).
#   L12  Endpoint relocation assumes single-proxy endpoints are freely placeable
#        on any hostable node; the current endpoint node is seeded from the
#        Step-1 estimate (L2), not a dedicated `rladmin status endpoints` read.
# --------------------------------------------------------------------------- #
@dataclass
class NodeCapacity:
    """Placement-invariant capacity walls for a node."""
    uid: int
    ram_ceiling: int           # max RAM for shards = current provisioned + PROVISIONAL_RAM
    cores: int
    max_shards: Optional[int]
    rack_id: Optional[str]
    hostable: bool


def apply_node_exclusions(cluster: Cluster) -> None:
    """Mark configured exclude_nodes as non-hostable (idempotent). A non-hostable
    node is not a migration source or target and drops out of the balance pool, so
    its shards stay put and nothing new lands on it - the same treatment DOWN /
    quorum-only nodes already get."""
    excl = cluster.config.excluded_nodes()
    if not excl:
        return
    for n in cluster.nodes:
        if n.uid in excl:
            n.hostable = False


def node_capacities(cluster: Cluster, current_loads: Dict[int, NodeLoad]) -> Dict[int, NodeCapacity]:
    caps: Dict[int, NodeCapacity] = {}
    for n in cluster.nodes:
        if n.provisional_ram is not None:
            ceiling = current_loads[n.uid].provisioned_memory + n.provisional_ram
        else:
            ceiling = n.total_memory
        caps[n.uid] = NodeCapacity(n.uid, ceiling, n.cores, n.max_shards, n.rack_id, n.hostable)
    return caps


def _req_vcpu_load(load: NodeLoad) -> float:
    return load.vcpu   # weighted required vCPU, accumulated with per-DB CPU weights


def score_from_loads(loads: Dict[int, NodeLoad], caps: Dict[int, NodeCapacity]) -> BalanceScore:
    """Score using placement-invariant ceilings (valid for any placement).

    For the current placement this equals Step 1's score_layout(), because
    ram_ceiling == provisioned + PROVISIONAL_RAM == ram_capacity there.
    """
    mem, cpu = [], []
    for u, c in caps.items():
        if not c.hostable:
            continue
        if c.ram_ceiling > 0:
            mem.append(loads[u].provisioned_memory / c.ram_ceiling)
        if c.cores > 0:
            cpu.append(_req_vcpu_load(loads[u]) / c.cores)
    memory_cv = _coeff_of_variation(mem)
    cpu_cv = _coeff_of_variation(cpu)
    memory_score = 100.0 * (1.0 - min(memory_cv, 1.0))
    cpu_score = 100.0 * (1.0 - min(cpu_cv, 1.0))
    return BalanceScore(memory_cv, cpu_cv, memory_score, cpu_score,
                        (memory_score + cpu_score) / 2.0)


@dataclass(frozen=True)
class SpreadCap:
    """Per-node ceiling on how many shards of ONE sparse DB a node may hold.

    Two tiers (LIMITATION L11): the even-spread share applies on most nodes, but
    'roomy' nodes (provisioned memory below the cluster average - i.e. NOT
    saturated by dense/pinned DBs) get a higher 'relaxed' cap. That lets a sparse
    DB vacate the dense-loaded nodes and fill the empty ones for better cluster
    balance, while still spanning a minimum number of nodes (the spread intent)."""
    even: int
    relaxed: int
    roomy: frozenset

    def cap_for(self, node_uid: int) -> int:
        return self.relaxed if node_uid in self.roomy else self.even


# A sparse DB never collapses below this many nodes, even under full relaxation.
_MIN_SPARSE_SPREAD = 3


def build_spread_caps(state: "_Live", eligible: List[int]) -> Dict[int, Optional["SpreadCap"]]:
    """Per-DB sparse spread caps. None for dense (footprint-bound). 'roomy' nodes
    are those whose provisioned memory is below the cluster average; a sparse DB
    may exceed its even share there to relieve saturated nodes (balance-aware,
    Option A). Computed once from the initial layout - the saturating load is
    dense/pinned, which does not move, so the roomy set is stable."""
    cluster = state.cluster
    n_elig = max(1, len(eligible))
    provs = [state.loads[u].provisioned_memory for u in eligible]
    avg = (sum(provs) / len(provs)) if provs else 0
    roomy = frozenset(u for u in eligible if state.loads[u].provisioned_memory < avg)
    n_roomy = len(roomy)
    out: Dict[int, Optional[SpreadCap]] = {}
    for uid, db in state.scope.items():
        if db.shard_placement != "sparse":
            out[uid] = None  # dense: footprint-bound, no even-spread cap
            continue
        total = len(cluster.shards_by_bdb.get(uid, []))
        even = max(1, math.ceil(total / n_elig))
        # Relax onto roomy nodes, but never span fewer than _MIN_SPARSE_SPREAD.
        spread_target = max(n_roomy, min(n_elig, total, _MIN_SPARSE_SPREAD))
        relaxed = max(even, math.ceil(total / max(1, spread_target)))
        out[uid] = SpreadCap(even, relaxed, roomy)
    return out


class _Live:
    """Mutable layout state with O(1) op/undo for the local search.

    Only single, low-risk operations are modelled, per operational policy:
      * move ONE shard, or
      * relocate ONE endpoint (single-proxy DBs only; no data movement).
    Shard SWAPS are intentionally NOT supported - they are riskier and less
    preferred. When the cluster is too full to migrate, the tool ALERTS instead
    of swapping (see rebalance_blocked_by_memory / rendering).
    """

    def __init__(self, cluster: Cluster, caps: Dict[int, NodeCapacity]) -> None:
        self.cluster = cluster
        self.caps = caps
        self.cfg = cluster.config
        self.place: Dict[int, int] = {s.uid: s.node_uid for s in cluster.shards}
        # Roles are MUTABLE here (a failover swaps a group's master/slave) - the
        # Shard.role attribute is the original; self.role is the current role.
        self.role: Dict[int, str] = {s.uid: s.role for s in cluster.shards}
        # Seed the initial layout (= current) from the ACTUAL discovered endpoint
        # locations when available, so misalignment is visible; fall back to the
        # policy-derived nodes for any DB without discovered endpoints.
        self.loads: Dict[int, NodeLoad] = compute_loads(
            cluster, Placement(self.place), cluster.endpoints_by_db)
        # counted (in-scope) DBs - their resources count; some may be pinned.
        self.scope: Dict[int, Database] = {d.uid: d for d in in_scope_databases(cluster)}
        self.ep: Dict[int, set] = {
            uid: set(cluster.endpoints_by_db.get(uid)
                     or placement_endpoint_nodes(cluster, Placement(self.place), db))
            for uid, db in self.scope.items()
        }
        # An endpoint set is "independent" (freely relocatable) ONLY when
        # respect_endpoint_policy=False (suggesting a proxy-policy change).
        # Otherwise endpoints follow the policy: 'single' stays co-located with a
        # master node, 'all-master-shards'/'amp' on every master node, 'all-nodes'
        # on all nodes - recomputed when masters move, never moved off-policy.
        self.indep_ep: Dict[int, bool] = {
            uid: (not self.cfg.respect_endpoint(db))
            for uid, db in self.scope.items()
        }
        # DBs whose single endpoint has been ALIGNED to its master node(s): the
        # resource phase must not move their masters (that would relocate the
        # endpoint). Populated by optimize() after the alignment phase; empty
        # elsewhere. Slaves of these DBs remain movable.
        self.pinned_single: set = set()
        # Out-of-scope (e.g. flex) shards: not moved, but they occupy shard slots
        # (count against max_shards). RAM is already netted out of PROVISIONAL_RAM.
        self.other: Dict[int, int] = {n.uid: 0 for n in cluster.nodes}
        for s in cluster.shards:
            if s.bdb_uid not in self.scope and s.node_uid in self.other:
                self.other[s.node_uid] += 1

    def _set_ep(self, db: Database, new: set) -> None:
        cur = self.ep[db.uid]
        ep_cpu = self.cfg.endpoint_cpu(db)
        for n in cur - new:
            if n in self.loads:
                self.loads[n].endpoints -= 1
                self.loads[n].vcpu -= ep_cpu
        for n in new - cur:
            if n in self.loads:
                self.loads[n].endpoints += 1
                self.loads[n].vcpu += ep_cpu
        self.ep[db.uid] = set(new)

    def _master_nodes(self, db: Database) -> set:
        return {self.place[s.uid] for s in self.cluster.shards_by_bdb[db.uid]
                if self.role[s.uid] == "master"} - {None}

    def _recompute_ep(self, db: Database) -> None:
        if db.proxy_policy == "single":
            # `rladmin migrate db .. endpoint_to_shards` binds the single proxy to
            # the node hosting the MOST master shards (fewest client hops). Model
            # that: pick the majority-masters node. Tie-break: keep the current
            # endpoint node if it is one of the tied leaders (matches the command's
            # idempotency), else the lowest node-uid. Never a non-master node.
            cnt: Dict[int, int] = {}
            for s in self.cluster.shards_by_bdb[db.uid]:
                if self.role[s.uid] == "master":
                    n = self.place[s.uid]
                    if n is not None:
                        cnt[n] = cnt.get(n, 0) + 1
            if not cnt:
                self._set_ep(db, set())
                return
            maxc = max(cnt.values())
            leaders = {n for n, c in cnt.items() if c == maxc}
            cur = next(iter(self.ep[db.uid]), None)
            chosen = cur if cur in leaders else min(leaders)
            self._set_ep(db, {chosen})
        elif db.proxy_policy in ("all-master-shards", "all-master-proxies"):
            self._set_ep(db, self._master_nodes(db))
        else:
            self._set_ep(db, set(placement_endpoint_nodes(self.cluster, Placement(self.place), db)))

    # --- operation 1: MIGRATE ONE shard (slave, or a non-replicated master) ---
    # A replicated master's PROCESS is never migrated (rladmin would fail it over,
    # relocating the master + its endpoint unpredictably); see valid_move.
    def do_move(self, shard: Shard, dst: int, db: Database):
        src = self.place[shard.uid]
        role = self.role[shard.uid]
        m = db.per_shard_memory
        w = self.cfg.shard_cpu(db)
        ls, ld = self.loads[src], self.loads[dst]
        ls.provisioned_memory -= m; ld.provisioned_memory += m
        ls.used_memory -= shard.used_memory; ld.used_memory += shard.used_memory
        ls.vcpu -= w; ld.vcpu += w
        if role == "master":
            ls.master_shards -= 1; ld.master_shards += 1
        else:
            ls.replica_shards -= 1; ld.replica_shards += 1
        self.place[shard.uid] = dst
        old_ep = None
        # A migrating master (non-replicated only) carries its endpoint with it.
        if (role == "master"
                and db.proxy_policy in ("single", "all-master-shards", "all-master-proxies")
                and not self.indep_ep.get(db.uid, False)):
            old_ep = set(self.ep[db.uid])
            self._recompute_ep(db)
        return (shard, src, dst, db, old_ep, role)

    def undo_move(self, token) -> None:
        shard, src, dst, db, old_ep, role = token
        m = db.per_shard_memory
        w = self.cfg.shard_cpu(db)
        ls, ld = self.loads[src], self.loads[dst]
        ld.provisioned_memory -= m; ls.provisioned_memory += m
        ld.used_memory -= shard.used_memory; ls.used_memory += shard.used_memory
        ld.vcpu -= w; ls.vcpu += w
        if role == "master":
            ld.master_shards -= 1; ls.master_shards += 1
        else:
            ld.replica_shards -= 1; ls.replica_shards += 1
        self.place[shard.uid] = src
        if old_ep is not None:
            self._set_ep(db, old_ep)

    # --- operation 2: FAILOVER a group (swap master<->slave role in place) ---
    # No data moves; the master (and its endpoint for single/ams/amp) shifts from
    # the old master node to the promoted replica's node. Load (mem/CPU of the two
    # shard processes) is unchanged; only the endpoint's CPU moves.
    def do_failover(self, db: Database, master_uid: int, slave_uid: int):
        self.role[master_uid], self.role[slave_uid] = (
            self.role[slave_uid], self.role[master_uid])
        old_ep = None
        if (db.proxy_policy in ("single", "all-master-shards", "all-master-proxies")
                and not self.indep_ep.get(db.uid, False)):
            old_ep = set(self.ep[db.uid])
            self._recompute_ep(db)
        return (db, master_uid, slave_uid, old_ep)

    def undo_failover(self, token) -> None:
        db, master_uid, slave_uid, old_ep = token
        self.role[master_uid], self.role[slave_uid] = (
            self.role[slave_uid], self.role[master_uid])
        if old_ep is not None:
            self._set_ep(db, old_ep)

    def group_pair(self, db: Database, shard: Shard):
        """Return (master_uid, slave_uid) for shard's HA group under current roles,
        or None if not a replicated master+slave pair."""
        members = [s for s in self.cluster.shards_by_bdb[db.uid]
                   if s.ha_group == shard.ha_group]
        if len(members) != 2:
            return None
        m = [s.uid for s in members if self.role[s.uid] == "master"]
        sl = [s.uid for s in members if self.role[s.uid] == "slave"]
        return (m[0], sl[0]) if m and sl else None

    # --- operation 3: relocate ONE endpoint (independent endpoint sets only) ---
    def do_ep_move(self, db: Database, src: int, dst: int):
        new = set(self.ep[db.uid])
        new.discard(src); new.add(dst)
        self._set_ep(db, new)
        return (db, src, dst)

    def undo_ep_move(self, token) -> None:
        db, src, dst = token
        new = set(self.ep[db.uid])
        new.discard(dst); new.add(src)
        self._set_ep(db, new)

    def valid_move(self, shard: Shard, dst: int, db: Database,
                   db_caps: Dict[int, Optional["SpreadCap"]], ignore_ram: bool = False) -> bool:
        """All HARD constraints except CPU (CPU is checked post-move on loads).
        ignore_ram lets the alert logic detect moves blocked solely by RAM."""
        c = self.caps[dst]
        if not c.hostable:
            return False
        # A replicated master's process is never migrated - its node can only change
        # via failover (do_failover). Only slaves (and non-replicated masters) migrate.
        if db.replication and self.role[shard.uid] == "master":
            return False
        # An aligned single-policy DB's master is pinned: moving it would relocate the
        # (co-located) endpoint, undoing the alignment. Its slaves stay movable.
        if db.uid in self.pinned_single and self.role[shard.uid] == "master":
            return False
        # ALWAYS enforced: HA anti-affinity + rack-awareness.
        for other in self.cluster.shards_by_bdb[db.uid]:
            if other.uid == shard.uid or other.ha_group != shard.ha_group:
                continue
            on = self.place.get(other.uid)
            if on == dst:
                return False
            if self.cluster.rack_aware and on in self.caps:
                orc = self.caps[on].rack_id
                if orc is not None and c.rack_id is not None and orc == c.rack_id:
                    return False
        if not ignore_ram and \
                self.loads[dst].provisioned_memory + db.per_shard_memory > c.ram_ceiling:
            return False  # RAM ceiling
        if c.max_shards is not None and \
                self.loads[dst].total_shards + self.other[dst] + 1 > c.max_shards:
            return False  # shard-count limit (incl. flex shards already there)
        # dense/sparse - enforced unless respect_shard_placement=False for this DB.
        if self.cfg.respect_placement(db):
            if db.shard_placement == "dense":
                on_dst = any(self.place.get(o.uid) == dst
                             for o in self.cluster.shards_by_bdb[db.uid] if o.uid != shard.uid)
                if not on_dst:
                    return False  # dense: never expand the DB's node footprint
            sc = db_caps.get(db.uid)
            if sc is not None:
                cnt = sum(1 for sh in self.cluster.shards_by_bdb[db.uid]
                          if self.place[sh.uid] == dst)
                if cnt + 1 > sc.cap_for(dst):
                    return False  # sparse: even-spread cap (relaxed on roomy nodes)
        return True

    def valid_ep_move(self, db: Database, src: int, dst: int) -> bool:
        # Endpoints consume no modelled RAM/shard slots; only CPU (checked via
        # cpu_violations) and the target must be hostable and not already bound.
        return (self.caps[dst].hostable and src in self.ep[db.uid]
                and dst not in self.ep[db.uid])

    def cpu_violations(self) -> int:
        return sum(
            1 for u, c in self.caps.items()
            if c.hostable and c.cores > 0 and _req_vcpu_load(self.loads[u]) > c.cores
        )


def consolidate_dense(state: _Live, caps: Dict[int, NodeCapacity],
                      db_caps: Dict[int, Optional["SpreadCap"]], force: bool = False) -> List[Dict[str, Any]]:
    """PRIORITY pass: realise 'dense' placement by packing each dense DB's shards
    (per role) onto a SINGLE node when feasible. Dense means 'as few nodes as
    possible', so a dense DB spread across nodes is consolidated; the single /
    all-master endpoint then follows the masters automatically (do_move recompute).

    Constraint-safe: each move is validated (HA, rack, RAM ceiling, shard limit,
    dense footprint) and a DB/role is consolidated only if ALL its shards fit one
    node without a new CPU violation; otherwise it is left as-is. Returns moves."""
    cluster = state.cluster
    moves: List[Dict[str, Any]] = []
    for db in movable_databases(cluster):
        if (db.shard_placement != "dense" or not cluster.config.respect_placement(db)
                or not cluster.config.consolidate_dense(db)):
            continue
        for role in ("master", "slave"):
            shards = [s for s in cluster.shards_by_bdb[db.uid] if s.role == role]
            if len(shards) <= 1 or len({state.place[s.uid] for s in shards}) <= 1:
                continue  # nothing to consolidate
            # Prefer the node already hosting the most of these shards (fewest moves).
            for target, _n in Counter(state.place[s.uid] for s in shards).most_common():
                if not caps[target].hostable:
                    continue  # never consolidate onto an excluded/non-hostable node
                tokens, ok = [], True
                for s in shards:
                    if state.place[s.uid] == target:
                        continue
                    src = state.place[s.uid]
                    # never migrate a shard OFF an excluded/non-hostable node either
                    if not caps[src].hostable:
                        ok = False
                        break
                    if state.valid_move(s, target, db, db_caps, ignore_ram=force):
                        tokens.append((s, src, state.do_move(s, target, db)))
                    else:
                        ok = False
                        break
                if ok and not force:  # don't pack to the point of over-subscribing CPU
                    tc = state.caps[target]
                    if tc.cores > 0 and _req_vcpu_load(state.loads[target]) > tc.cores:
                        ok = False
                if ok:
                    for s, src, _tok in tokens:
                        moves.append({"kind": "shard", "shard": s.uid, "db": db.uid,
                                      "db_name": db.name, "role": role, "src": src,
                                      "dst": target, "bytes": db.per_shard_memory,
                                      "consolidate": True})
                    break  # this role consolidated onto `target`
                for _s, _src, tok in reversed(tokens):
                    state.undo_move(tok)
    return moves


def align_endpoints(state: "_Live") -> None:
    """Re-derive every policy-following DB's endpoints to sit with its (current)
    master shards (single -> majority-master node; all-master-shards/-proxies ->
    every master node). Skips 'independent' endpoints (respect_endpoint_policy=false,
    freely placed by the planner) and 'all-nodes' (already everywhere). Idempotent
    for already-aligned DBs. Mismatches it corrects surface as endpoint re-binds via
    endpoint_rebind_commands (which diffs the actual vs this policy-correct state)."""
    for uid, db in state.scope.items():
        if state.indep_ep.get(uid) or db.proxy_policy == "all-nodes":
            continue
        state._recompute_ep(db)


def align_by_failover(state: "_Live", moves: List[Dict[str, Any]], force: bool = False) -> None:
    """Endpoint<->master alignment, PREFERRED branch: keep a single-policy endpoint on
    its current node X by bringing masters TO X via failover, rather than moving the
    endpoint to the masters. For each single-policy replicated DB, fail over EVERY shard
    whose replica is on X (master elsewhere) so as MANY masters as possible co-locate
    with the endpoint (fewest client hops) - bounded only by X's CPU cores. Then pin the
    endpoint on X.

    Failover is CPU load-neutral in this model (master and replica both cost shard_cpu),
    so consolidating adds NO load to X (its replicas were already there) and never
    changes the resource score. The CPU guard therefore bites only when X - endpoint
    included - is already over its cores: then we DON'T pile masters on an overloaded
    node; instead we undo and let align_endpoints() move the endpoint off X to the
    natural majority (relieving X). Under --force the guard is ignored.

    If X ends up NOT a (tied) master-leader (too few replicas on X), we also undo and
    fall back to an endpoint re-bind."""
    def master_counts(uid: int) -> Dict[int, int]:
        c: Dict[int, int] = {}
        for s in state.cluster.shards_by_bdb.get(uid, []):
            if state.role[s.uid] == "master":
                n = state.place[s.uid]
                c[n] = c.get(n, 0) + 1
        return c

    for uid, db in list(state.scope.items()):
        if state.indep_ep.get(uid) or not db.replication or db.proxy_policy != "single":
            continue
        ep = state.ep.get(uid) or set()
        if len(ep) != 1:
            continue
        X = next(iter(ep))
        if X not in state.caps or not state.caps[X].hostable:
            continue  # don't consolidate masters onto an excluded/non-hostable endpoint node
        # Fail over ALL shards whose replica is on X (master elsewhere) -> master on X.
        applied: List[Tuple[Any, Dict[str, Any]]] = []  # (undo token, move dict)
        while True:
            cand = None
            for s in state.cluster.shards_by_bdb.get(uid, []):
                if state.role[s.uid] == "master" and state.place[s.uid] != X:
                    pair = state.group_pair(db, s)
                    if pair and state.place.get(pair[1]) == X:
                        cand = pair + (state.place[pair[0]],)  # (m_uid, s_uid, m_node)
                        break
            if cand is None:
                break  # no more replicas of this DB on X to promote
            m_uid, s_uid, m_node = cand
            tok = state.do_failover(db, m_uid, s_uid)  # replica on X -> master on X
            applied.append((tok, {
                "kind": "failover", "db": db.uid, "db_name": db.name,
                "master_shard": m_uid, "slave_shard": s_uid, "role": "failover",
                "shard": m_uid, "src": m_node, "dst": X, "bytes": 0, "align": True}))
        if not applied:
            continue  # nothing to consolidate here (align_endpoints settles the EP)
        state._set_ep(db, {X})   # tentatively co-locate the endpoint with the masters
        c = master_counts(uid)
        x_leader = c.get(X, 0) >= max(c.values())              # endpoint may stay on X
        cpu_ok = force or state.loads[X].vcpu <= state.caps[X].cores   # X can host it
        if x_leader and cpu_ok:
            moves.extend(mv for _tok, mv in applied)
        else:  # X can't/shouldn't hold the masters -> undo; align_endpoints re-binds EP
            for tok, _mv in reversed(applied):
                state.undo_failover(tok)


def optimize(cluster: Cluster, caps: Dict[int, NodeCapacity], max_iter: int = 5000,
             force: bool = False):
    """Greedy descent using only single, low-risk operations: move ONE shard or
    relocate ONE endpoint per step. No swaps. On ties, the lower-risk endpoint
    relocation (no data moved) is preferred. Returns (state, moves, db_caps).

    force=True is the RESOURCE OVERRIDE: RAM ceilings and CPU-core limits are
    relaxed so a balancing plan is still produced even when the cluster lacks
    resources (the result may OVER-COMMIT nodes and is not deployable). Policy
    constraints (HA, rack, dense/sparse, shard-count) are STILL enforced."""
    state = _Live(cluster, caps)
    eligible = [u for u, c in caps.items() if c.hostable]
    db_caps = build_spread_caps(state, eligible)
    movable = movable_databases(cluster)   # excludes pinned (exclude_from_balancing) DBs
    shard_by_uid = {s.uid: s for s in cluster.shards}
    # PRIORITY pre-pass: pack dense DBs onto single nodes (endpoint follows).
    moves: List[Dict[str, Any]] = consolidate_dense(state, caps, db_caps, force)

    # ALIGNMENT FIRST: co-locate every policy-following DB's endpoint with its
    # master(s) BEFORE resource balancing, so the optimiser works from the aligned
    # baseline (and reports the honest best-achievable-with-alignment score).
    # Single-policy endpoints are kept in place by failing masters over to them
    # (load-neutral); only when that can't reach a majority is the endpoint moved.
    align_by_failover(state, moves, force=force)
    align_endpoints(state)
    # Pin aligned single-policy DBs' masters: the resource phase must not relocate
    # them (that would move the endpoint). Their slaves stay movable.
    state.pinned_single = {
        uid for uid, db in state.scope.items()
        if db.proxy_policy == "single" and state.cfg.respect_endpoint(db)
        and not state.indep_ep.get(uid)
    }
    eps = 1e-9

    def objective() -> float:
        s = score_from_loads(state.loads, caps)
        return s.memory_cv + s.cpu_cv

    base = objective()   # baseline AFTER alignment
    base_viol = state.cpu_violations()

    for _ in range(max_iter):
        # Prune candidates to hotter-than-average -> cooler nodes (L9).
        pr = {}
        for u in eligible:
            c = caps[u]
            mu = state.loads[u].provisioned_memory / c.ram_ceiling if c.ram_ceiling > 0 else 0.0
            cu = _req_vcpu_load(state.loads[u]) / c.cores if c.cores > 0 else 0.0
            pr[u] = max(mu, cu)
        if not pr:
            break
        mean_pr = sum(pr.values()) / len(pr)
        sources = [u for u in eligible if pr[u] > mean_pr + eps] or eligible
        targets = [u for u in eligible if pr[u] < mean_pr - eps] or eligible

        # best candidate across both single-operation neighbourhoods.
        # key = (cpu_violations, objective, risk); risk 0 = endpoint, 1 = shard.
        best = None  # (key, kind, payload)

        def consider(viol: int, obj: float, risk: int, kind: str, payload) -> None:
            nonlocal best
            key = (viol, round(obj, 12), risk)
            if best is None or key < best[0]:
                best = (key, kind, payload)

        # neighbourhood A: endpoint relocations (independent endpoint sets) - no data
        for db in movable:
            if not state.indep_ep.get(db.uid, False):
                continue  # policy-following endpoints move only with their masters
            for src in list(state.ep[db.uid]):
                if src not in sources:
                    continue
                for dst in targets:
                    if not state.valid_ep_move(db, src, dst):
                        continue
                    token = state.do_ep_move(db, src, dst)
                    viol, obj = state.cpu_violations(), objective()
                    state.undo_ep_move(token)
                    if not force and viol > base_viol:
                        continue
                    consider(viol, obj, 0, "ep", (db, src, dst))

        # neighbourhood B: single shard moves
        for db in movable:
            for shard in cluster.shards_by_bdb[db.uid]:
                src = state.place[shard.uid]
                if src not in sources:
                    continue
                for dst in targets:
                    if dst == src or not state.valid_move(shard, dst, db, db_caps, ignore_ram=force):
                        continue
                    token = state.do_move(shard, dst, db)
                    viol, obj = state.cpu_violations(), objective()
                    state.undo_move(token)
                    if not force and viol > base_viol:
                        continue
                    consider(viol, obj, 1, "move", (shard, dst, db, src))

        # Apply the best migrate / endpoint move if it improves. Slave migrations
        # (and non-replicated master migrations) are ALWAYS preferred: failovers are
        # only considered below, when no such move improves the balance further.
        if best is not None:
            (viol, obj, _risk), kind, payload = best
            # Accept only on a STRICT lexicographic improvement in (violations, objective)
            # - the same ordering consider() ranks by. Accepting when EITHER axis improved
            # (obj OR viol) let the search ping-pong between two states that each dominate
            # the other on a different axis (fewer violations vs. better balance), churning
            # out reversing moves until max_iter. Lexicographic acceptance makes the
            # potential monotonically decreasing, so it converges and never reverses.
            # (Non-force behaviour is unchanged: there, viol > base_viol candidates are
            # already filtered, so this reduces to the previous obj-improvement test.)
            if viol < base_viol or (viol == base_viol and obj < base - eps):
                if kind == "ep":
                    db, src, dst = payload
                    state.do_ep_move(db, src, dst)
                    moves.append({"kind": "endpoint", "db": db.uid, "db_name": db.name,
                                  "role": "endpoint", "src": src, "dst": dst, "bytes": 0})
                else:
                    shard, dst, db, src = payload
                    state.do_move(shard, dst, db)
                    moves.append({"kind": "shard", "shard": shard.uid, "db": db.uid,
                                  "db_name": db.name, "role": state.role[shard.uid],
                                  "src": src, "dst": dst, "bytes": db.per_shard_memory})
                base, base_viol = obj, viol
                continue

        # LAST RESORT: relocate a replicated master via FAILOVER (+ migrate the
        # demoted replica). This moves an endpoint (service impact), so it is tried
        # only when no slave/endpoint migration improved the balance.
        best2 = None  # (key, ops)

        def consider2(v: int, o: float, ops) -> None:
            nonlocal best2
            key = (v, round(o, 12), len(ops))  # fewer ops preferred on ties
            if best2 is None or key < best2[0]:
                best2 = (key, ops)

        seen: set = set()
        for db in movable:
            if not db.replication or db.uid in state.pinned_single:
                continue  # pinned single-policy masters stay put (endpoint alignment)
            for shard in cluster.shards_by_bdb[db.uid]:
                pair = state.group_pair(db, shard)
                if pair is None or pair in seen:
                    continue
                seen.add(pair)
                m_uid, s_uid = pair
                m_node, s_node = state.place[m_uid], state.place[s_uid]
                if m_node not in sources:
                    continue
                if not caps[s_node].hostable:
                    continue  # never promote mastership onto an excluded/non-hostable node
                ftok = state.do_failover(db, m_uid, s_uid)  # master now on s_node
                v, o = state.cpu_violations(), objective()
                if force or v <= base_viol:                 # standalone failover
                    consider2(v, o, [("failover", db, m_uid, s_uid, m_node, s_node)])
                demoted = shard_by_uid[m_uid]               # old master, now a replica
                for dst in targets:
                    if dst == m_node or not state.valid_move(demoted, dst, db, db_caps,
                                                             ignore_ram=force):
                        continue
                    mtok = state.do_move(demoted, dst, db)
                    v2, o2 = state.cpu_violations(), objective()
                    state.undo_move(mtok)
                    if force or v2 <= base_viol:
                        consider2(v2, o2, [("failover", db, m_uid, s_uid, m_node, s_node),
                                           ("move", demoted, dst, db, m_node)])
                state.undo_failover(ftok)

        if best2 is None:
            break
        (v, o, _), ops = best2
        if not (o < base - eps or v < base_viol):
            break
        for op in ops:
            if op[0] == "failover":
                _, db, m_uid, s_uid, fn, tn = op
                state.do_failover(db, m_uid, s_uid)
                moves.append({"kind": "failover", "db": db.uid, "db_name": db.name,
                              "master_shard": m_uid, "slave_shard": s_uid, "role": "failover",
                              "shard": m_uid, "src": fn, "dst": tn, "bytes": 0})
            else:
                _, shard, dst, db, src = op
                state.do_move(shard, dst, db)
                moves.append({"kind": "shard", "shard": shard.uid, "db": db.uid,
                              "db_name": db.name, "role": state.role[shard.uid],
                              "src": src, "dst": dst, "bytes": db.per_shard_memory})
        base, base_viol = o, v

    return state, moves, db_caps


def compact_plan(cluster: Cluster, caps: Dict[int, NodeCapacity], greedy_state: _Live,
                 greedy_moves: List[Dict[str, Any]], db_caps: Dict[int, Optional["SpreadCap"]],
                 force: bool = False):
    """Reduce churn: the greedy search optimises score, not move count, so it can
    route interchangeable shards through a node (land one, later move another off
    it) and even fail a group over and back. Since shards of the same (DB, role) are
    interchangeable, only the per-node COUNT matters for balance. This re-derives the
    MINIMAL set of ops from the net change, eliminating both pass-through shard moves
    and reversing/repeated failovers.

    Failovers are handled (not skipped): a failover swaps a group's master/replica
    roles in place (no data moves) but shifts the master-carried endpoint, so it can
    affect load. We therefore rebuild the plan from the greedy END state -
    emit ONE failover per HA group whose master/replica identity net-changed (dropping
    any fail-over-then-back), then derive shard moves grouped by the group's FINAL
    role, ordered failovers-first so a demoted-then-migrated shard is a replica (hence
    movable) by the time its move replays.

    Safety: returns the compacted move list ONLY if the reconstructed end state matches
    the greedy roles exactly, every move validates, the balance score is identical, and
    the result is strictly fewer operations; otherwise None (the caller keeps the
    original, known-good plan). So it never produces a worse or invalid plan - at worst
    it changes nothing.
    """
    shard_moves = [m for m in greedy_moves if m["kind"] == "shard"]
    ep_moves = [m for m in greedy_moves if m["kind"] == "endpoint"]
    fo_moves = [m for m in greedy_moves if m["kind"] == "failover"]
    if not shard_moves and not fo_moves:
        return None  # only endpoint re-binds (already minimal) -> nothing to compact

    target = greedy_state.place        # final node per physical shard
    target_role = greedy_state.role    # final role per physical shard
    live = _Live(cluster, caps)

    # --- 1. Net failovers: one per HA group whose master/replica identity net-swapped.
    # A group failed over an even number of times nets to no change (dropped here); an
    # odd number nets to a single role swap. do_failover keeps processes in place and
    # recomputes the master-carried endpoint, so replaying only the net swaps reproduces
    # the greedy end roles AND endpoint load.
    new_fo: List[Dict[str, Any]] = []
    for db in movable_databases(cluster):
        seen: set = set()
        for s in cluster.shards_by_bdb[db.uid]:
            if s.ha_group in seen:
                continue
            seen.add(s.ha_group)
            members = [m for m in cluster.shards_by_bdb[db.uid] if m.ha_group == s.ha_group]
            if len(members) != 2:
                continue  # only plain master+replica pairs fail over
            om = next((m for m in members if m.role == "master"), None)
            osl = next((m for m in members if m.role == "slave"), None)
            if om is None or osl is None:
                continue
            if target_role[om.uid] == "master":
                continue  # role unchanged for this group -> no failover needed
            live.do_failover(db, om.uid, osl.uid)  # promote original replica -> master
            new_fo.append({
                "kind": "failover", "db": db.uid, "db_name": db.name,
                "master_shard": om.uid, "slave_shard": osl.uid, "role": "failover",
                "shard": om.uid, "src": om.node_uid, "dst": osl.node_uid, "bytes": 0,
                "align": any(m.get("align") for m in fo_moves
                             if m.get("master_shard") == om.uid),
            })
    # The net failovers must reproduce the greedy end roles exactly; if not (e.g. a
    # group with an unexpected shape), bail rather than emit a divergent plan.
    for db in movable_databases(cluster):
        for s in cluster.shards_by_bdb[db.uid]:
            if live.role[s.uid] != target_role[s.uid]:
                return None

    # --- 2. Net shard moves, grouped by the group's FINAL role (post-failover). Only
    # per-node COUNT matters, so match surplus nodes to deficit nodes directly.
    new_shard: List[Dict[str, Any]] = []
    for db in movable_databases(cluster):
        for role in ("master", "slave"):
            group = [s for s in cluster.shards_by_bdb[db.uid] if target_role[s.uid] == role]
            if not group:
                continue
            init_c = Counter(s.node_uid for s in group)   # processes don't move on failover
            fin_c = Counter(target[s.uid] for s in group)
            if init_c == fin_c:
                continue  # group's distribution unchanged -> no moves needed
            surplus: List[int] = []
            deficit: List[int] = []
            for n in set(init_c) | set(fin_c):
                d = fin_c.get(n, 0) - init_c.get(n, 0)
                if d < 0:
                    surplus.extend([n] * (-d))
                elif d > 0:
                    deficit.extend([n] * d)
            surplus.sort(); deficit.sort()
            for src_n, dst_n in zip(surplus, deficit):
                moved = False
                for s in group:
                    if live.place[s.uid] == src_n and live.valid_move(
                            s, dst_n, db, db_caps, ignore_ram=force):
                        live.do_move(s, dst_n, db)
                        new_shard.append({
                            "kind": "shard", "shard": s.uid, "db": db.uid,
                            "db_name": db.name, "role": role, "src": src_n,
                            "dst": dst_n, "bytes": db.per_shard_memory,
                        })
                        moved = True
                        break
                if not moved:
                    return None  # couldn't find a valid direct move -> keep greedy

    for m in ep_moves:  # endpoint relocations are already minimal; replay to match state
        db = live.scope.get(m["db"])
        if db is None or not live.valid_ep_move(db, m["src"], m["dst"]):
            return None
        live.do_ep_move(db, m["src"], m["dst"])

    # Must be exactly equivalent in balance, and actually fewer operations. Emit
    # failovers first so each dependent (demoted-then-migrated) shard move is valid
    # when build_plan replays the list in order.
    if abs(score_from_loads(live.loads, caps).overall
           - score_from_loads(greedy_state.loads, caps).overall) > 1e-6:
        return None
    compacted = new_fo + new_shard + ep_moves
    if len(compacted) >= len(greedy_moves):
        return None
    return compacted


def rebalance_blocked_by_memory(cluster: Cluster, caps: Dict[int, NodeCapacity],
                                state: _Live, db_caps: Dict[int, Optional["SpreadCap"]]):
    """Detect the 'too full to migrate' case: an over-utilised node has a shard
    that cannot move to ANY other node solely because of RAM ceilings (the move
    would be valid if RAM were ignored). This is the situation where a swap would
    otherwise be needed - we ALERT and recommend capacity instead. Returns
    (blocked, smallest_stuck_shard_bytes, largest_free_headroom_bytes)."""
    eligible = [u for u, c in caps.items() if c.hostable]
    util = {u: (state.loads[u].provisioned_memory / caps[u].ram_ceiling)
            for u in eligible if caps[u].ram_ceiling > 0}
    if not util:
        return False, 0, 0
    mean = sum(util.values()) / len(util)
    hot = [u for u in util if util[u] > mean + 1e-9]

    movable_uids = {d.uid for d in movable_databases(cluster)}
    blocked = False
    min_stuck: Optional[int] = None
    for u in hot:
        for shard in cluster.shards:
            if state.place[shard.uid] != u or shard.bdb_uid not in movable_uids:
                continue
            db = state.scope[shard.bdb_uid]
            others = [d for d in eligible if d != u]
            can_move = any(state.valid_move(shard, d, db, db_caps) for d in others)
            if can_move:
                continue
            ram_would_allow = any(
                state.valid_move(shard, d, db, db_caps, ignore_ram=True) for d in others)
            if ram_would_allow:  # only RAM is stopping this shard from migrating
                blocked = True
                sz = db.per_shard_memory
                min_stuck = sz if min_stuck is None else min(min_stuck, sz)
    free = max((caps[u].ram_ceiling - state.loads[u].provisioned_memory for u in eligible),
               default=0)
    return blocked, (min_stuck or 0), free


def feasibility(cluster: Cluster, caps: Dict[int, NodeCapacity], state: _Live) -> Dict[str, Any]:
    """Assess whether the desired (final) placement fits current resources, and
    if not, quantify the shortfall and recommend additional resources."""
    loads = state.loads
    hostable = [c for c in caps.values() if c.hostable]

    ram_over = {u: loads[u].provisioned_memory - c.ram_ceiling
                for u, c in caps.items()
                if c.hostable and loads[u].provisioned_memory > c.ram_ceiling}
    cpu_over = {u: _req_vcpu_load(loads[u]) - c.cores
                for u, c in caps.items()
                if c.hostable and c.cores > 0 and _req_vcpu_load(loads[u]) > c.cores}
    shard_over = {u: loads[u].total_shards - (c.max_shards or 0)
                  for u, c in caps.items()
                  if c.hostable and c.max_shards is not None and loads[u].total_shards > c.max_shards}

    total_ram_req = sum(loads[u].provisioned_memory for u in loads)
    total_ram_cap = sum(c.ram_ceiling for c in hostable)
    total_cpu_req = sum(_req_vcpu_load(loads[u]) for u, c in caps.items() if c.hostable)
    total_cores = sum(c.cores for c in hostable)

    ram_short = max(0, total_ram_req - total_ram_cap)
    cpu_short = max(0, total_cpu_req - total_cores)
    feasible = not ram_over and not cpu_over and not shard_over

    lines: List[str] = []
    if not feasible:
        if ram_short > 0:
            worst = max(ram_over.items(), key=lambda kv: kv[1]) if ram_over else None
            extra = f"  (binding node {worst[0]}: +{fmt_bytes(worst[1])})" if worst else ""
            lines.append(f"RAM : cluster short {fmt_bytes(ram_short)} to place all shards{extra}")
        elif ram_over:
            worst = max(ram_over.items(), key=lambda kv: kv[1])
            lines.append(f"RAM : node {worst[0]} over by {fmt_bytes(worst[1])} "
                         "(aggregate fits; placement is constraint-bound - likely rack/HA)")
        else:
            lines.append("RAM : OK")
        if cpu_short > 0:
            worst = max(cpu_over.items(), key=lambda kv: kv[1]) if cpu_over else None
            extra = f"  (binding node {worst[0]}: +{worst[1]:g} vCPU)" if worst else ""
            lines.append(f"CPU : cluster short {cpu_short:g} vCPU{extra}")
        elif cpu_over:
            worst = max(cpu_over.items(), key=lambda kv: kv[1])
            lines.append(f"CPU : node {worst[0]} over by {worst[1]:g} vCPU "
                         "(aggregate fits; placement is constraint-bound)")
        else:
            lines.append("CPU : OK")
        if shard_over:
            worst = max(shard_over.items(), key=lambda kv: kv[1])
            lines.append(f"SHARDS: node {worst[0]} over shard limit by {worst[1]}")

    recommendation = None
    if not feasible:
        need_ram = ram_short or (max(ram_over.values()) if ram_over else 0)
        need_cpu = cpu_short or (max(cpu_over.values()) if cpu_over else 0)
        parts = []
        if need_ram:
            parts.append(f">= {fmt_bytes(int(need_ram))} RAM")
        if need_cpu:
            parts.append(f">= {need_cpu:g} vCPU")
        if parts:
            recommendation = "add capacity: " + " and ".join(parts) + \
                " (e.g. one additional node of that size, or grow the binding node), then re-run."

    return {
        "feasible": feasible,
        "ram_short": ram_short, "cpu_short": cpu_short,
        "ram_over": ram_over, "cpu_over": cpu_over, "shard_over": shard_over,
        "total_ram_req": total_ram_req, "total_ram_cap": total_ram_cap,
        "total_cpu_req": total_cpu_req, "total_cores": total_cores,
        "lines": lines, "recommendation": recommendation,
    }


def _can_host_on(cluster: Cluster, caps: Dict[int, NodeCapacity], survivors: set) -> bool:
    """Can ALL shards + endpoints be (re)placed on `survivors` within the HARD
    constraints - RAM ceiling, CPU cores, per-node shard limit, master/replica
    anti-affinity, and rack-awareness? Greedy largest-first bin-pack (a heuristic:
    it may miss a fit that exists, so the removable count it yields is a safe lower
    bound). Pinned (exclude_from_balancing) and out-of-scope shards cannot be moved,
    so if any sits on a removed node the survivor set is infeasible."""
    survivors = set(survivors)
    if not survivors:
        return False
    rack_aware = cluster.rack_aware
    prov = {u: 0 for u in survivors}          # provisioned memory
    vcpu = {u: 0.0 for u in survivors}
    cnt = {u: 0 for u in survivors}           # shard slots used (incl. pinned/oos)
    groups: Dict[int, set] = {u: set() for u in survivors}   # ha_group keys on node
    rack = {u: caps[u].rack_id for u in survivors}
    masters: Dict[int, set] = defaultdict(set)   # db uid -> nodes hosting a master

    movable_uids = {d.uid for d in movable_databases(cluster)}

    # Pinned / out-of-scope shards stay put; they must be on a survivor and they
    # consume capacity there.
    for s in cluster.shards:
        if s.bdb_uid in movable_uids:
            continue
        if s.node_uid not in survivors:
            return False
        db = cluster.db_by_uid.get(s.bdb_uid)
        prov[s.node_uid] += db.per_shard_memory if db else 0
        vcpu[s.node_uid] += cluster.config.shard_cpu(db) if db else 0
        cnt[s.node_uid] += 1
        groups[s.node_uid].add(s.ha_group)
        if db and s.role == "master":
            masters[s.bdb_uid].add(s.node_uid)

    def rack_conflict(grp, u) -> bool:
        return rack_aware and rack[u] is not None and any(
            grp in groups[v] and rack[v] == rack[u] for v in survivors if v != u)

    # Place movable shards, largest first; best-fit onto the roomiest legal node.
    movable = [s for s in cluster.shards if s.bdb_uid in movable_uids]
    movable.sort(key=lambda s: cluster.db_by_uid[s.bdb_uid].per_shard_memory, reverse=True)
    for s in movable:
        db = cluster.db_by_uid[s.bdb_uid]
        psm, grp = db.per_shard_memory, s.ha_group
        best, best_free = None, None
        for u in survivors:
            if grp in groups[u] or rack_conflict(grp, u):
                continue                      # anti-affinity / rack
            c = caps[u]
            if c.ram_ceiling > 0 and prov[u] + psm > c.ram_ceiling:
                continue
            if c.max_shards is not None and cnt[u] + 1 > c.max_shards:
                continue
            free = c.ram_ceiling - prov[u]
            if best is None or free > best_free:
                best, best_free = u, free
        if best is None:
            return False
        prov[best] += psm
        vcpu[best] += cluster.config.shard_cpu(db)
        cnt[best] += 1
        groups[best].add(grp)
        if s.role == "master":
            masters[s.bdb_uid].add(best)

    # Endpoints follow the policy on the resulting master set; add their vCPU.
    for db in cluster.databases:
        if db.uid not in movable_uids and db.uid not in masters:
            continue
        mnodes = sorted(masters.get(db.uid, set()))
        if not mnodes and db.uid in movable_uids:
            return False                      # a movable DB with no placed master
        pol = db.proxy_policy
        if pol in ("all-master-shards", "all-master-proxies"):
            ep_nodes = mnodes
        elif pol == "all-nodes":
            ep_nodes = list(survivors)
        else:                                 # single / other
            ep_nodes = mnodes[:1]
        ec = cluster.config.endpoint_cpu(db)
        for u in ep_nodes:
            if u in vcpu:
                vcpu[u] += ec

    return all(caps[u].cores <= 0 or vcpu[u] <= caps[u].cores for u in survivors)


def node_removal_capacity(cluster: Cluster, caps: Dict[int, NodeCapacity]) -> Dict[str, Any]:
    """How many nodes can fail / be removed while the remaining nodes can still
    host all shards + endpoints (see _can_host_on). Greedy: shed the smallest-
    capacity node that keeps the cluster feasible, repeat. Returns the count, an
    example removable set (with per-node resources), and whether nodes differ in
    resources."""
    hostable = [u for u, c in caps.items() if c.hostable]
    survivors = set(hostable)
    removed: List[int] = []
    # Try to remove nodes cheapest-first (keep the biggest -> maximises the count).
    while len(survivors) > 1:
        progress = False
        for u in sorted(survivors, key=lambda x: (caps[x].ram_ceiling, caps[x].cores)):
            if _can_host_on(cluster, caps, survivors - {u}):
                survivors.remove(u)
                removed.append(u)
                progress = True
                break
        if not progress:
            break

    def phys(u):  # physical resources, not load-dependent ram_ceiling
        nd = cluster.node(u)
        return (nd.total_memory if nd else caps[u].ram_ceiling,
                nd.cores if nd else caps[u].cores)

    def node_res(u):
        mem, cores = phys(u)
        return {"uid": u, "total_memory": mem, "cores": cores}

    return {
        "total": len(hostable),
        "removable": len(removed),
        "removed_nodes": [node_res(u) for u in removed],
        "heterogeneous": len({phys(u) for u in hostable}) > 1,
    }


def render_node_removal(cluster: Cluster, caps: Dict[int, NodeCapacity]) -> str:
    """Failure-tolerance note: how many nodes can be removed and still host all."""
    info = node_removal_capacity(cluster, caps)
    total, n = info["total"], info["removable"]
    out = ["Node removal / failure tolerance:"]
    if n <= 0:
        out.append(f"  0 of {total} nodes can be removed - the cluster needs all current nodes to")
        out.append("  host all shards + endpoints (respecting HA anti-affinity, rack, RAM, CPU,")
        out.append("  shard limits). Add capacity for headroom.")
        return "\n".join(out)
    out.append(f"  Up to {n} of {total} node(s) can fail / be removed and the cluster can still")
    out.append("  host all shards + endpoints on the remaining nodes (HA anti-affinity, rack,")
    out.append("  RAM, CPU and shard limits all respected).")
    if info["heterogeneous"]:
        out.append("  Nodes differ in resources; an example removable set:")
        for nd in info["removed_nodes"]:
            out.append(f"    - node:{nd['uid']}  (RAM {fmt_bytes(nd['total_memory'])}, "
                       f"{nd['cores']:g} cores)")
        out.append("  (other equivalent sets may exist; this is one feasible choice.)")
    return "\n".join(out)


# --------------------------------------------------------------------------- #
# Rendering
# --------------------------------------------------------------------------- #
def _db_rules_tag(cluster: Cluster, d: Database) -> str:
    """Compact per-DB config flags that differ from the CLUSTER baseline (so a
    per-database override stands out; a DB with no override shows '-')."""
    cfg = cluster.config
    base = cfg.cluster  # effective cluster-level defaults (built-ins overlaid by 'cluster')
    parts = []
    if cfg.excluded(d):
        parts.append("EXCLUDED")
    if not cfg.respect_placement(d):
        parts.append("place:free")
    if not cfg.respect_endpoint(d):
        parts.append("ep:free")
    if d.shard_placement == "dense" and cfg.respect_placement(d) and not cfg.consolidate_dense(d):
        parts.append("dense:nopack")
    sc, ec = cfg.shard_cpu(d), cfg.endpoint_cpu(d)
    base_sc = float(base.get("shard_cpu", SHARD_VCPU))
    base_ec = float(base.get("endpoint_cpu", ENDPOINT_VCPU))
    if sc != base_sc or ec != base_ec:
        parts.append(f"cpu S{sc:g}/E{ec:g}")
    return ", ".join(parts) if parts else "-"


# --------------------------------------------------------------------------- #
# Optional ANSI colour (auto on a TTY; --color / --no-color override). Table
# widths use VISIBLE length so colour codes never break column alignment.
# --------------------------------------------------------------------------- #
_USE_COLOR = False
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def set_color(enabled: bool) -> None:
    global _USE_COLOR
    _USE_COLOR = enabled
    if enabled and os.name == "nt":  # enable ANSI on Windows consoles (best-effort)
        try:
            import ctypes
            k = ctypes.windll.kernel32
            k.SetConsoleMode(k.GetStdHandle(-11), 7)
        except Exception:
            pass


def _c(s: Any, code: str) -> str:
    return f"\x1b[{code}m{s}\x1b[0m" if _USE_COLOR else str(s)


def _red(s: Any) -> str:
    return _c(s, "31")


def _green(s: Any) -> str:
    return _c(s, "32")


def _blue(s: Any) -> str:
    return _c(s, "34")


def _purple(s: Any) -> str:
    return _c(s, "35")


def _vlen(s: Any) -> int:
    return len(_ANSI_RE.sub("", str(s)))


def _pad(s: Any, w: int) -> str:
    s = str(s)
    return s + " " * max(0, w - _vlen(s))


def _pct(frac: float) -> str:
    """Format a utilisation fraction as 'NN%', red when over 100% (over capacity)."""
    s = f"{frac * 100:.0f}%"
    return _red(s) if frac > 1.0 + 1e-9 else s


def _free_vcpu(v: float) -> str:
    """Spare vCPU as ':g'; red when negative (node over-subscribed - same red as >100%)."""
    s = f"{v:g}"
    return _red(s) if v < -1e-9 else s


def _pct3(frac: float) -> str:
    """Right-justified 'NNN%' (visible width 4), red when over 100%."""
    s = f"{frac * 100:3.0f}%"
    return _red(s) if frac > 1.0 + 1e-9 else s


def _color_action(token: str) -> str:
    """Rebalancing-action token: '-...' green (leaving), '+...' blue (arriving)."""
    t = str(token)
    if t.startswith("-"):
        return _green(t)
    if t.startswith("+"):
        return _blue(t)
    return t


def _chg(pp: float) -> str:
    """Utilisation change in percentage points: down green, up blue, ~0 plain."""
    r = round(pp)
    if r < 0:
        return _green(f"{r:+d}")
    if r > 0:
        return _blue(f"{r:+d}")
    return "+0"


def _ansi_to_html(text: str) -> str:
    """Convert a colour-coded text report into HTML: escape text, map our ANSI
    colours (31 red, 32 green, 34 blue) to spans. Reuses all existing rendering."""
    parts = re.split(r"(\x1b\[[0-9;]*m)", text)
    cls = {"31": "r", "32": "g", "34": "b", "35": "p"}
    out = []
    for part in parts:
        m = re.fullmatch(r"\x1b\[([0-9;]*)m", part)
        if m:
            code = m.group(1)
            out.append("</span>" if code in ("", "0") else f'<span class="{cls.get(code, "")}">')
        else:
            out.append(_htmlmod.escape(part))
    return "".join(out)


def _html_report(cluster_name: str, text_report: str) -> str:
    """Wrap the (colour-coded) text report in a self-contained HTML page."""
    body = _ansi_to_html(text_report)
    return (
        "<!DOCTYPE html><html><head><meta charset=\"utf-8\">"
        f"<title>Rebalancing report - {_htmlmod.escape(cluster_name)}</title><style>"
        "body{background:#fff;color:#222;margin:1.5rem;}"
        "h2{font-family:Segoe UI,Arial,sans-serif;}"
        "pre{font-family:Consolas,Menlo,monospace;font-size:13px;line-height:1.35;"
        "white-space:pre;overflow-x:auto;}"
        ".r{color:#c0392b;font-weight:bold}.g{color:#1e8449}.b{color:#1f4e9c}"
        ".p{color:#8e44ad;font-weight:bold}"
        "</style></head><body>"
        f"<h2>Redis Enterprise rebalancing report &mdash; {_htmlmod.escape(cluster_name)}</h2>"
        f"<pre>{body}</pre></body></html>"
    )


def fmt_bytes(n: int) -> str:
    if n == 0:
        return "0 B"
    sign = "-" if n < 0 else ""
    n = abs(n)
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    i = min(int(math.log(n, 1024)), len(units) - 1)
    return f"{sign}{n / (1024 ** i):.1f} {units[i]}"


def _table(headers: List[str], rows: List[List[Any]]) -> str:
    cols = len(headers)
    widths = [_vlen(h) for h in headers]
    for r in rows:
        for i in range(cols):
            widths[i] = max(widths[i], _vlen(r[i]))
    line = "  ".join(_pad(h, widths[i]) for i, h in enumerate(headers))
    sep = "  ".join("-" * widths[i] for i in range(cols))
    body = "\n".join(
        "  ".join(_pad(r[i], widths[i]) for i in range(cols)) for r in rows
    )
    return f"{line}\n{sep}\n{body}"


def render_current_layout(
    cluster: Cluster, views: List[NodeView], score: BalanceScore,
    reqs: Dict[int, Dict[str, Any]],
) -> str:
    out: List[str] = []
    scoped = in_scope_databases(cluster)
    skipped = out_of_scope_databases(cluster)

    out.append("=" * 86)
    out.append(f"STEP 1 - CURRENT LAYOUT + SCORE   cluster: {cluster.name}")
    out.append("=" * 86)
    out.append(
        f"Nodes: {len(cluster.nodes)}   In-scope DBs (RAM/redis): {len(scoped)}"
        f"   Out-of-scope DBs: {len(skipped)}"
        f"   Rack-aware: {'yes' if cluster.rack_aware else 'no'}"
    )
    out.append(f"RAM availability source: {cluster.ram_source}")
    cc = cluster.config.cluster
    out.append(f"CPU model (cluster default): shard={cc['shard_cpu']:g} vCPU, "
               f"endpoint={cc['endpoint_cpu']:g} vCPU (per-DB overrides may apply)")
    out.append("")

    out.append("Per-node resources (available vs. required)")
    node_rows = []
    for v in sorted(views, key=lambda x: x.uid):
        node_rows.append([
            v.uid, v.addr, v.status, v.cores,
            f"{v.required_vcpu:g}", _free_vcpu(v.free_vcpu),
            fmt_bytes(v.total_memory), fmt_bytes(v.provisioned_memory),
            fmt_bytes(v.available_ram), f"{v.master_shards}/{v.replica_shards}",
            v.endpoints, v.rack_id or "-",
        ])
    out.append(_table(
        ["node", "addr", "status", "vCPU", "vCPU req", "vCPU free",
         "RAM total", "RAM req(shards)", "RAM avail(prov)", "shards M/R",
         "endpoints", "rack"],
        node_rows,
    ))
    out.append("")

    out.append("In-scope databases")
    db_rows = []
    for d in sorted(scoped, key=lambda x: x.uid):
        req = reqs[d.uid]
        db_rows.append([
            d.uid, d.name, fmt_bytes(d.memory_size), req["total_shards"],
            "yes" if d.replication else "no", d.shard_placement,
            d.proxy_policy, fmt_bytes(d.per_shard_memory),
            fmt_bytes(req["memory"]), f"{req['vcpu']:g}", _db_rules_tag(cluster, d),
        ])
    out.append(_table(
        ["db", "name", "mem limit", "shards(M+R)", "repl",
         "placement", "proxy policy", "per-shard mem", "mem req", "cpu req", "rules"],
        db_rows,
    ) if db_rows else "  (none)")
    out.append("")

    if skipped:
        out.append("Out-of-scope databases (NOT rebalanced)")
        sk_rows = [
            [d.uid, d.name, "flex/auto-tiering" if d.is_flex else d.db_type]
            for d in sorted(skipped, key=lambda x: x.uid)
        ]
        out.append(_table(["db", "name", "reason"], sk_rows))
        out.append("")

    out.append("Balance score (0-100, higher = more evenly utilised across nodes)")
    out.append(_table(
        ["component", "CV", "score"],
        [
            ["memory utilisation", f"{score.memory_cv:.3f}", f"{score.memory_score:.1f}"],
            ["cpu utilisation", f"{score.cpu_cv:.3f}", f"{score.cpu_score:.1f}"],
            ["OVERALL", "-", f"{score.overall:.1f}"],
        ],
    ))
    out.append("")

    warns = capacity_warnings(views)
    if warns:
        out.append("Capacity warnings (current layout)")
        for w in warns:
            out.append(f"  ! {w}")
        out.append("")

    active_nodes = [v for v in views if v.status != "down"]
    if not scoped:
        verdict = "No in-scope RAM databases found - nothing to balance."
    elif len(active_nodes) < 2:
        verdict = "Fewer than two active nodes - rebalancing is not applicable."
    elif score.is_balanced:
        verdict = (
            f"Cluster is already well balanced (overall {score.overall:.1f}/100). "
            "Rebalancing is unlikely to help materially."
        )
    else:
        verdict = (
            f"Cluster can be balanced better (overall {score.overall:.1f}/100). "
            "Proceed to Step 2 to compute a desired layout."
        )
    out.append("VERDICT: " + verdict)
    return "\n".join(out)


def layout_as_dict(
    cluster: Cluster, views: List[NodeView], score: BalanceScore,
    reqs: Dict[int, Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "cluster": cluster.name,
        "rack_aware": cluster.rack_aware,
        "ram_source": cluster.ram_source,
        "cpu_model": {"shard_vcpu": cluster.config.cluster["shard_cpu"],
                      "endpoint_vcpu": cluster.config.cluster["endpoint_cpu"],
                      "note": "cluster defaults; per-DB overrides may apply"},
        "score": {
            "overall": round(score.overall, 2),
            "memory_score": round(score.memory_score, 2),
            "cpu_score": round(score.cpu_score, 2),
            "memory_cv": round(score.memory_cv, 4),
            "cpu_cv": round(score.cpu_cv, 4),
            "is_balanced": score.is_balanced,
        },
        "nodes": [
            {
                "uid": v.uid, "addr": v.addr, "status": v.status, "cores": v.cores,
                "hostable": v.node.hostable, "max_shards": v.max_shards,
                "required_vcpu": v.required_vcpu, "free_vcpu": v.free_vcpu,
                "total_memory": v.total_memory, "provisioned_memory": v.provisioned_memory,
                "provisional_ram": v.provisional_ram, "available_ram": v.available_ram,
                "ram_capacity": v.ram_capacity, "used_memory": v.used_memory,
                "master_shards": v.master_shards, "replica_shards": v.replica_shards,
                "endpoints": v.endpoints, "rack_id": v.rack_id,
            }
            for v in sorted(views, key=lambda x: x.uid)
        ],
        "warnings": capacity_warnings(views),
        "databases_in_scope": [
            {
                "uid": d.uid, "name": d.name, "memory_size": d.memory_size,
                "shards_count": d.shards_count, "replication": d.replication,
                "total_shards": reqs[d.uid]["total_shards"],
                "shard_placement": d.shard_placement, "proxy_policy": d.proxy_policy,
                "per_shard_memory": d.per_shard_memory,
                "memory_required": reqs[d.uid]["memory"],
                "vcpu_required": reqs[d.uid]["vcpu"],
                "endpoints": reqs[d.uid]["endpoints"],
            }
            for d in sorted(in_scope_databases(cluster), key=lambda x: x.uid)
        ],
        "databases_out_of_scope": [
            {"uid": d.uid, "name": d.name, "reason": "flex" if d.is_flex else d.db_type}
            for d in sorted(out_of_scope_databases(cluster), key=lambda x: x.uid)
        ],
    }


# --------------------------------------------------------------------------- #
# STEP 2 rendering
# --------------------------------------------------------------------------- #
def _bar(frac: float, width: int = 8) -> str:
    # ASCII-only for portability across terminals / redirected output.
    frac = max(0.0, min(1.0, frac))
    filled = int(round(frac * width))
    return "#" * filled + "." * (width - filled)


def _util(load: NodeLoad, cap: NodeCapacity) -> Tuple[float, float]:
    mem = load.provisioned_memory / cap.ram_ceiling if cap.ram_ceiling > 0 else 0.0
    cpu = _req_vcpu_load(load) / cap.cores if cap.cores > 0 else 0.0
    return mem, cpu


def _arrow(cur: float, des: float) -> str:
    d = des - cur
    if d > 0.5:
        return "  ^"
    if d < -0.5:
        return "  v"
    return ""


def render_desired(
    cluster: Cluster, caps: Dict[int, NodeCapacity],
    current_loads: Dict[int, NodeLoad], cur_score: BalanceScore,
    state: "_Live", des_score: BalanceScore,
    moves: List[Dict[str, Any]], feas: Dict[str, Any],
    blocked_info: Tuple[bool, int, int], force: bool = False,
) -> str:
    out: List[str] = []
    out.append("=" * 86)
    out.append(f"STEP 2 - DESIRED LAYOUT + SCORE      cluster: {cluster.name}")
    out.append("=" * 86)

    if feas["feasible"]:
        out.append("FEASIBILITY:  ACHIEVABLE with current resources - no extra CPU/RAM needed.")
    elif force:
        out.append("FEASIBILITY:  ADDITIONAL RESOURCES REQUIRED - overridden by --force "
                   "(target below OVER-COMMITS resources; not deployable).")
    else:
        out.append("FEASIBILITY:  ADDITIONAL RESOURCES REQUIRED.")
    out.append("")

    # Score current -> desired
    out.append("Balance score        current --> desired     change")
    for label, cs, ds in (
        ("memory", cur_score.memory_score, des_score.memory_score),
        ("cpu", cur_score.cpu_score, des_score.cpu_score),
        ("OVERALL", cur_score.overall, des_score.overall),
    ):
        sign = "+" if ds >= cs else ""
        arrow = _arrow(cs, ds)
        out.append(f"  {label:<10}        {cs:5.1f}  -->  {ds:5.1f}      {sign}{ds - cs:5.1f}{arrow}")
    out.append("")

    # Per-node utilisation bars, current -> desired
    out.append("Per-node utilisation       RAM (prov/ceiling)            CPU (req/cores)")
    out.append("node  rack   current -> desired              current -> desired")
    for n in sorted(cluster.nodes, key=lambda x: x.uid):
        c = caps[n.uid]
        if not c.hostable:
            continue
        cm, cc = _util(current_loads[n.uid], c)
        dm, dc = _util(state.loads[n.uid], c)
        out.append(
            f" {n.uid:<4} {(n.rack_id or '-'):<5}  "
            f"{_bar(cm)} {_pct3(cm)}  -> {_bar(dm)} {_pct3(dm)}   "
            f"{_bar(cc)} {_pct3(cc)} -> {_bar(dc)} {_pct3(dc)}"
        )
    out.append(
        f"                       spread CV: RAM {cur_score.memory_cv:.2f}->{des_score.memory_cv:.2f}"
        f"   CPU {cur_score.cpu_cv:.2f}->{des_score.cpu_cv:.2f}"
    )
    out.append("")

    # Recommendations (single-operation: shard moves and/or endpoint relocations)
    if not moves:
        out.append("Recommended changes: none.")
    else:
        by_db: Dict[int, Dict[str, Any]] = {}
        for mv in moves:
            d = by_db.setdefault(mv["db"], {"name": mv["db_name"], "m": 0, "r": 0, "ep": 0, "bytes": 0})
            if mv["kind"] == "endpoint":
                d["ep"] += 1
            else:
                d["m" if mv["role"] == "master" else "r"] += 1
            d["bytes"] += mv["bytes"]
        n_shard = sum(1 for mv in moves if mv["kind"] == "shard")
        n_ep = sum(1 for mv in moves if mv["kind"] == "endpoint")
        total_bytes = sum(mv["bytes"] for mv in moves)
        out.append("Recommended changes (summary; ordered plan comes in Step 3)")
        out.append(f"  * {n_shard} shard move(s) + {n_ep} endpoint move(s) across "
                   f"{len(by_db)} database(s); ~{fmt_bytes(total_bytes)} provisioned RAM relocated")
        for db_uid in sorted(by_db):
            d = by_db[db_uid]
            bits = []
            if d["m"]:
                bits.append(f"{d['m']} master")
            if d["r"]:
                bits.append(f"{d['r']} replica")
            if d["ep"]:
                bits.append(f"{d['ep']} endpoint")
            extra = f"  ({fmt_bytes(d['bytes'])})" if d["bytes"] else "  (no data moved)"
            out.append(f"  * {d['name']:<16}: move {', '.join(bits)}{extra}")
        out.append("  * operations: single shard move or endpoint relocation")
        out.append("  * constraints respected: HA anti-affinity, rack, dense/sparse, "
                   "shard<=max, CPU<=cores")
    out.append("")

    blocked, min_shard, free = blocked_info
    if not feas["feasible"]:
        out.append("  Resource shortfall:")
        for line in feas["lines"]:
            out.append(f"     {line}")
        if feas["recommendation"]:
            out.append(f"  Recommendation: {feas['recommendation']}")
        if force:
            out.append(f"  --force: the target above (score {des_score.overall:.1f}) is produced "
                       "ANYWAY and OVER-COMMITS resources.")
            out.append("  It is NOT deployable - add the capacity above, then drop --force.")
        else:
            out.append(f"  Best achievable score with current resources: {max(des_score.overall, 0):.1f}")
            out.append("  No migration plan is produced while infeasible (use --force to override).")
    elif des_score.is_balanced:
        worst_ram = max(
            (_util(state.loads[u], caps[u])[0] for u in caps if caps[u].hostable), default=0.0)
        worst_cpu = max(
            (_util(state.loads[u], caps[u])[1] for u in caps if caps[u].hostable), default=0.0)
        if moves:
            out.append(f"RESULT: balanced; no node above {worst_ram*100:.0f}% RAM "
                       f"or {worst_cpu*100:.0f}% CPU.")
        else:
            out.append("RESULT: cluster is already balanced within tolerance; no changes needed.")
    elif blocked:
        need = max(0, min_shard - free)
        out.append("ALERT: cluster is imbalanced but cannot be rebalanced by migration -")
        out.append("       it is too full to move shards between nodes.")
        out.append(f"  The hottest node's smallest shard ({fmt_bytes(min_shard)}) does not fit the")
        out.append(f"  largest free headroom on any other node ({fmt_bytes(free)}).")
        if need > 0:
            out.append(f"  Recommendation: free or add >= {fmt_bytes(need)} RAM headroom on an "
                       "under-utilised node (or add a node), then re-run.")
        else:
            out.append("  Recommendation: add RAM headroom / a node, then re-run.")
    else:
        verb = "improved" if moves else "could not be improved"
        out.append(f"NOTE: balance {verb} ({cur_score.overall:.1f} -> {des_score.overall:.1f}); "
                   "residual imbalance is limited by")
        out.append("  placement policies (dense/sparse, HA, rack) and/or shard sizes.")
        out.append("  To improve further, add RAM headroom or a node and re-run.")
    return "\n".join(out)


def desired_as_dict(
    cluster: Cluster, caps: Dict[int, NodeCapacity],
    current_loads: Dict[int, NodeLoad], cur_score: BalanceScore,
    state: "_Live", des_score: BalanceScore,
    moves: List[Dict[str, Any]], feas: Dict[str, Any],
    blocked_info: Tuple[bool, int, int], force: bool = False,
) -> Dict[str, Any]:
    blocked, min_shard, free = blocked_info
    return {
        "cluster": cluster.name,
        "feasible": feas["feasible"],
        "forced": force,
        "deployable": not force and feas["feasible"],
        "rebalance_blocked_by_memory": blocked,
        "score": {
            "current": round(cur_score.overall, 2),
            "desired": round(des_score.overall, 2),
            "memory_current": round(cur_score.memory_score, 2),
            "memory_desired": round(des_score.memory_score, 2),
            "cpu_current": round(cur_score.cpu_score, 2),
            "cpu_desired": round(des_score.cpu_score, 2),
        },
        "nodes": [
            {
                "uid": u,
                "rack_id": caps[u].rack_id,
                "ram_ceiling": caps[u].ram_ceiling,
                "cores": caps[u].cores,
                "current": {
                    "provisioned_memory": current_loads[u].provisioned_memory,
                    "shards": current_loads[u].total_shards,
                    "endpoints": current_loads[u].endpoints,
                    "required_vcpu": _req_vcpu_load(current_loads[u]),
                },
                "desired": {
                    "provisioned_memory": state.loads[u].provisioned_memory,
                    "shards": state.loads[u].total_shards,
                    "endpoints": state.loads[u].endpoints,
                    "required_vcpu": _req_vcpu_load(state.loads[u]),
                },
            }
            for u in sorted(caps) if caps[u].hostable
        ],
        "moves": moves,
        "feasibility": {
            "ram_short": feas["ram_short"], "cpu_short": feas["cpu_short"],
            "lines": feas["lines"], "recommendation": feas["recommendation"],
        },
        "migration_block": {
            "blocked": blocked,
            "smallest_blocked_shard": min_shard,
            "largest_free_headroom": free,
        },
    }


# --------------------------------------------------------------------------- #
# STEP 3: ordered rebalancing plan (READ-ONLY - presents steps, runs nothing)
#
# The Step 2 optimiser commits one operation at a time against a live model,
# each validated against the state at that moment, so its `moves` list is
# already a valid EXECUTION ORDER. Step 3 replays it to show, per step, what
# moves and the running score, plus the rladmin command Step 4 would run.
#
# LIMITATION L13: plan assumes ONLINE shard migration. Redis Enterprise reserves
#   the target shard's memory up-front (covered by the RAM-ceiling check), so no
#   intermediate step violates capacity; real migration throttling/timing and
#   the exact endpoint-bind command (needs `rladmin status endpoints`, see L12)
#   are finalised in Step 4. Step 3 itself changes nothing on the cluster.
# --------------------------------------------------------------------------- #
def build_plan(cluster: Cluster, caps: Dict[int, NodeCapacity],
               moves: List[Dict[str, Any]], db_caps: Dict[int, Optional["SpreadCap"]],
               force: bool = False):
    """Replay the ordered ops on a fresh live model, capturing the running score
    and re-validating each step. With force, RAM is relaxed in the check (the
    over-commit is shown via utilisation and the FORCED banner, not as INVALID).
    Returns (steps, start_score)."""
    shard_by_uid = {s.uid: s for s in cluster.shards}
    state = _Live(cluster, caps)
    start = score_from_loads(state.loads, caps)
    prev = start.overall
    steps: List[Dict[str, Any]] = []
    for i, mv in enumerate(moves, 1):
        extra: Dict[str, Any] = {}
        if mv["kind"] == "shard":
            shard = shard_by_uid[mv["shard"]]
            db = state.scope[mv["db"]]
            valid = state.valid_move(shard, mv["dst"], db, db_caps, ignore_ram=force)
            state.do_move(shard, mv["dst"], db)
            what = f"{state.role[shard.uid]} shard {shard.uid} (slots {shard.slots or 'n/a'})"
            slots = shard.slots
        elif mv["kind"] == "failover":
            db = state.scope[mv["db"]]
            state.do_failover(db, mv["master_shard"], mv["slave_shard"])
            valid = True
            _why = ("co-locate master with endpoint" if mv.get("align")
                    else "promotes replica, no data moved")
            what = (f"failover master shard {mv['master_shard']} "
                    f"(node{mv['src']}->node{mv['dst']}; {_why})")
            slots = shard_by_uid[mv["master_shard"]].slots
            extra = {"master_shard": mv["master_shard"], "slave_shard": mv["slave_shard"],
                     "align": mv.get("align", False)}
        else:  # endpoint relocation (independent endpoint)
            db = state.scope[mv["db"]]
            valid = state.valid_ep_move(db, mv["src"], mv["dst"])
            state.do_ep_move(db, mv["src"], mv["dst"])
            what = "endpoint"
            slots = ""
        sc = score_from_loads(state.loads, caps)
        delta = sc.overall - prev
        prev = sc.overall
        steps.append({
            "step": i, "kind": mv["kind"], "db": mv["db"], "db_name": mv["db_name"],
            "role": mv["role"], "shard": mv.get("shard"), "slots": slots,
            "src": mv["src"], "dst": mv["dst"], "bytes": mv["bytes"],
            "what": what, "valid": valid, "score_after": sc.overall, "delta": delta,
            **extra,
        })
    # Re-align endpoints to the (post-move) masters so planned_state matches the
    # state optimize() scored - this is what makes endpoint_rebind_commands emit an
    # endpoint_to_shards for any DB whose actual endpoints drifted off its masters.
    align_endpoints(state)
    return steps, start, state


_POLICY_ABBR = {"all-master-shards": "ams", "all-master-proxies": "amp",
                "all-nodes": "all", "single": "single"}


def _policy_abbr(policy: str) -> str:
    return _POLICY_ABBR.get(policy, policy)


def _db_topology(cluster: Cluster, state: "_Live", db: Database):
    """For one DB under `state`: (shards-by-node, endpoint-bound-node-set)."""
    by_node: Dict[int, List[Tuple[str, int]]] = defaultdict(list)
    for s in cluster.shards_by_bdb.get(db.uid, []):
        by_node[state.place[s.uid]].append((state.role.get(s.uid, s.role), s.uid))
    if db.uid in state.scope:
        ep_nodes = set(state.ep.get(db.uid, set()))
    else:  # out-of-scope (flex): derive from policy, best-effort
        ep_nodes = placement_endpoint_nodes(cluster, Placement(state.place), db)
    return by_node, ep_nodes


def _grid(headers: List[str], rows: List[List[Any]]) -> str:
    """Like _table, but a cell may be a list[str] rendered as STACKED lines, so
    a logical row can span several physical lines (one shard per line). A row that
    is a plain str is rendered as a FULL-WIDTH rule of that char (e.g. '=' or '-')."""
    ncol = len(headers)
    widths = [_vlen(h) for h in headers]
    norm: List[Any] = []
    for r in rows:
        if isinstance(r, str):           # full-width rule marker
            norm.append(r)
            continue
        cells = [c if isinstance(c, list) else [str(c)] for c in r]
        norm.append(cells)
        for i, c in enumerate(cells):
            for line in c:
                widths[i] = max(widths[i], _vlen(line))
    total = sum(widths) + 2 * (ncol - 1)  # columns are joined by two spaces
    out = ["  ".join(_pad(h, widths[i]) for i, h in enumerate(headers))]
    out.append("=" * total)  # full-width separator under the header
    for cells in norm:
        if isinstance(cells, str):
            out.append((cells[:1] or "-") * total)
            continue
        height = max((len(c) for c in cells), default=1)
        for li in range(height):
            out.append("  ".join(
                _pad(cells[i][li] if li < len(cells[i]) else "", widths[i])
                for i in range(ncol)))
    return "\n".join(out)


def _action_token(db: Database, role: str, uid: int, sign: str) -> str:
    """e.g. '-db4:M20', '+db4:R5'  (same notation as the topology map)."""
    return f"{sign}{db.name}:{'M' if role == 'master' else 'R'}{uid}"


def _node_plan_data(cluster: Cluster, caps: Dict[int, NodeCapacity],
                    cur: "_Live", planned: "_Live") -> List[Dict[str, Any]]:
    """Per-node current/planned utilisation plus the migrations (shard + endpoint,
    leaving '-' / arriving '+') that drive each node's change."""
    nodes = [n.uid for n in sorted(cluster.nodes, key=lambda x: x.uid)]
    actions: Dict[int, List[str]] = {u: [] for u in nodes}
    for db in sorted(cluster.databases, key=lambda d: d.uid):
        cur_by, cur_ep = _db_topology(cluster, cur, db)
        pl_by, pl_ep = _db_topology(cluster, planned, db)
        for u in nodes:
            cur_set, pl_set = set(cur_by.get(u, [])), set(pl_by.get(u, []))
            for role, uid in sorted(cur_set - pl_set, key=lambda x: x[1]):
                actions[u].append(_action_token(db, role, uid, "-"))
            for role, uid in sorted(pl_set - cur_set, key=lambda x: x[1]):
                actions[u].append(_action_token(db, role, uid, "+"))
        for u in sorted(cur_ep - pl_ep):
            actions[u].append(f"-{db.name}:EP")
        for u in sorted(pl_ep - cur_ep):
            actions[u].append(f"+{db.name}:EP")

    data = []
    for u in nodes:
        c = caps[u]
        mc, cc = _util(cur.loads[u], c)
        mn, cn = _util(planned.loads[u], c)
        data.append({
            "node": u, "rack": c.rack_id or "-",
            "mem_cur": mc, "mem_new": mn, "cpu_cur": cc, "cpu_new": cn,
            "actions": actions[u],
        })
    return data


def render_cluster_map(cluster: Cluster, caps: Dict[int, NodeCapacity],
                       cur: "_Live", planned: "_Live", show_unchanged: bool = True,
                       full_topology: bool = False, detail_unchanged: bool = False) -> str:
    """One nodes-as-columns map. TOP: per-node utilisation (rack, Mem/CPU
    cur/new/chg). Split. BOTTOM: per database an EP row and a shards row, with a
    blank line between databases. Migrations appear as +arriving / -leaving on
    the shard/EP cells. Colours: over-100% red; chg-down & '-' green; chg-up &
    '+' blue; failovers (role flip in place, 'Rn>Mn') purple. All sections share
    node columns, so everything lines up."""
    data = _node_plan_data(cluster, caps, cur, planned)
    nodes = [d["node"] for d in data]
    u = {d["node"]: d for d in data}
    ncol = 2 + len(nodes)
    headers = ["", "NODE ID ->"] + [str(n) for n in nodes]
    rows: List[List[Any]] = []

    # --- TOP: per-node utilisation (pivoted) ---
    rows.append(["UTILISATION", "rack"] + [u[n]["rack"] for n in nodes])
    if full_topology:  # current-only view (no plan) - skip the new/chg rows
        rows.append(["", "Mem"] + [_pct(u[n]["mem_cur"]) for n in nodes])
        rows.append(["", "CPU"] + [_pct(u[n]["cpu_cur"]) for n in nodes])
    else:
        rows.append(["", "Mem cur"] + [_pct(u[n]["mem_cur"]) for n in nodes])
        rows.append(["", "Mem new"] + [_pct(u[n]["mem_new"]) for n in nodes])
        rows.append(["", "Mem chg"] + [_chg((u[n]["mem_new"] - u[n]["mem_cur"]) * 100) for n in nodes])
        rows.append(["", "CPU cur"] + [_pct(u[n]["cpu_cur"]) for n in nodes])
        rows.append(["", "CPU new"] + [_pct(u[n]["cpu_new"]) for n in nodes])
        rows.append(["", "CPU chg"] + [_chg((u[n]["cpu_new"] - u[n]["cpu_cur"]) * 100) for n in nodes])

    # --- split: full-width rule between UTILISATION and DATABASES ---
    rows.append("=")
    _db_hdr = "DATABASES" if full_topology else "DATABASE CHANGES"
    rows.append([_db_hdr, ""] + [""] * len(nodes))   # short divider (no long indent)

    def _db_tag(db: Database) -> str:
        if db.is_flex or db.db_type != "redis":
            return " (oos)"
        return " (excluded)" if cluster.config.excluded(db) else ""

    def _db_changed(db: Database) -> bool:
        for shard in cluster.shards_by_bdb.get(db.uid, []):
            if cur.place.get(shard.uid) != planned.place.get(shard.uid):
                return True
            if cur.role.get(shard.uid) != planned.role.get(shard.uid):
                return True  # failover: role swapped in place (endpoint may be unchanged)
        _, c_ep = _db_topology(cluster, cur, db)
        _, p_ep = _db_topology(cluster, planned, db)
        return set(c_ep) != set(p_ep)

    dbs = sorted(cluster.databases, key=lambda d: d.uid)
    if full_topology or detail_unchanged:
        # Render EVERY DB as an EP+shards block (uid order). full_topology has no plan
        # (cur == planned). detail_unchanged (verbose) keeps the plan deltas on changed
        # DBs while ALSO showing unchanged DBs' current placement (no deltas) - so all
        # shards/endpoints are visible per node, to see why a node is loaded.
        oneliners, blocks = [], dbs
    else:
        oneliners = [db for db in dbs if not _db_changed(db)] if show_unchanged else []
        blocks = [db for db in dbs if _db_changed(db)]

    # --- unchanged DBs: one line each (only when not detailing them) ---
    for db in oneliners:
        rows.append([db.name + _db_tag(db), "No changes"] + [""] * len(nodes))

    # --- database blocks: EP + shards, each preceded by a full-width rule ---
    # (with no leading one-line list, every block gets a leading rule).
    lead_sep = bool(oneliners) or not show_unchanged
    for idx, db in enumerate(blocks):
        if idx > 0 or lead_sep:
            rows.append("-")
        tag = _db_tag(db)
        _, cur_ep = _db_topology(cluster, cur, db)
        _, pl_ep = _db_topology(cluster, planned, db)

        ep_cells = []
        for n in nodes:
            inc, inp = n in cur_ep, n in pl_ep
            cell = "EP" if (inc and inp) else "+EP" if inp else "-EP" if inc else ""
            ep_cells.append(_color_action(cell))
        rows.append([db.name + tag, f"EP {db.uid}:1 ({_policy_abbr(db.proxy_policy)})"] + ep_cells)

        # Shards: one entry per shard; a MOVED shard puts '-Mx' (source) and
        # '+Mx' (dest) on the SAME line. Pack entries into lines so no two share
        # a node column -> moved +/- stay aligned across the row.
        entries: List[Dict[int, str]] = []
        for shard in sorted(cluster.shards_by_bdb.get(db.uid, []), key=lambda s: s.uid):
            cn, pn = cur.place.get(shard.uid), planned.place.get(shard.uid)
            cr = "M" if cur.role.get(shard.uid) == "master" else "R"
            pr = "M" if planned.role.get(shard.uid) == "master" else "R"
            if cn == pn and cr == pr:                       # unchanged
                entries.append({cn: f"{cr}{shard.uid}"})
            elif cn == pn:                                  # role flip in place (failover)
                entries.append({cn: _purple(f"{cr}{shard.uid}>{pr}{shard.uid}")})
            else:                                           # migrated (slave / non-repl master)
                entries.append({cn: f"-{cr}{shard.uid}", pn: f"+{pr}{shard.uid}"})
        packed: List[Dict[int, str]] = []
        for e in entries:
            line = next((ln for ln in packed if all(k not in ln for k in e)), None)
            if line is None:
                packed.append(dict(e))
            else:
                line.update(e)
        sh_cells: List[Any] = []
        for n in nodes:
            col = [_color_action(ln[n]) if n in ln else "" for ln in packed]
            sh_cells.append(col if col else "")
        rows.append([db.name + tag, "shards"] + sh_cells)

    if blocks:
        rows.append("=")   # full-width separator after the last database

    title = ("CLUSTER MAP (current topology + utilisation)" if full_topology
             else "CLUSTER MAP (utilisation, and planned database changes)")
    return title + "\n" + _grid(headers, rows)


def _topology_dict(cluster: Cluster, state: "_Live") -> List[Dict[str, Any]]:
    out = []
    for db in sorted(cluster.databases, key=lambda d: d.uid):
        by_node, ep_nodes = _db_topology(cluster, state, db)
        out.append({
            "db": db.uid, "name": db.name, "endpoint_id": f"{db.uid}:1",
            "proxy_policy": db.proxy_policy, "endpoint_nodes": sorted(ep_nodes),
            "in_scope": not db.is_flex and db.db_type == "redis",
            "excluded": (not db.is_flex and db.db_type == "redis"
                         and cluster.config.excluded(db)),
            "shards": {
                str(n): [("M" if r == "master" else "R") + str(u)
                         for r, u in sorted(v, key=lambda x: x[1])]
                for n, v in sorted(by_node.items())
            },
        })
    return out


def _step_command(st: Dict[str, Any]) -> str:
    if st["kind"] == "shard":
        return f"rladmin migrate shard {st['shard']} target_node {st['dst']}"
    # Independent endpoint relocation (respect_endpoint_policy=false): bind to a node.
    return f"rladmin bind db db:{st['db']} endpoint {st['db']}:1 node {st['dst']}"


def endpoint_rebind_commands(cluster: Cluster, cur: "_Live", planned: "_Live"):
    """For policy-following DBs (single / all-master-shards, respecting the policy)
    whose endpoint node set changes because masters moved, the proxies must be
    re-aligned to the current master shards. One command per such DB realises it:
        rladmin migrate db db:<id> endpoint_to_shards commit
    which binds a proxy on every master node (all-master-shards/-proxies) or the
    single proxy on the majority-masters node (single) - add/remove/move as needed,
    without changing proxy_policy. Returns [(db, command), ...]."""
    out = []
    for db in movable_databases(cluster):
        if not cluster.config.respect_endpoint(db) or db.proxy_policy == "all-nodes":
            continue  # independent endpoints / all-nodes don't need a policy rebind
        _, cur_ep = _db_topology(cluster, cur, db)
        _, pl_ep = _db_topology(cluster, planned, db)
        if set(cur_ep) != set(pl_ep):
            out.append((db, f"rladmin migrate db db:{db.uid} endpoint_to_shards commit"))
    return out


def _op_from_step(s: Dict[str, Any]) -> Dict[str, Any]:
    """Normalise a plan step into a deploy/describe op dict (every field the rladmin
    and REST deployers may read, plus a human label)."""
    if s["kind"] == "failover":
        return {"kind": "failover", "db": s["db"], "db_name": s["db_name"],
                "master_shard": s["master_shard"], "slave_shard": s["slave_shard"],
                "src": s["src"], "dst": s["dst"], "bytes": 0,
                "label": f"failover db:{s['db']} master shard {s['master_shard']} "
                         f"(node{s['src']}->node{s['dst']}, no data moved)"}
    if s["kind"] == "shard":
        return {"kind": "shard", "db": s["db"], "db_name": s["db_name"],
                "shard": s["shard"], "dst": s["dst"], "src": s.get("src"),
                "bytes": s.get("bytes", 0), "role": s.get("role", "shard"),
                "label": f"migrate {s.get('role', 'shard')} shard {s['shard']} "
                         f"-> node {s['dst']}"}
    return {"kind": "ep_node", "db": s["db"], "db_name": s["db_name"], "dst": s["dst"],
            "bytes": 0, "label": f"{s['db_name']}: relocate endpoint -> node {s['dst']}"}


def _execution_order(steps: List[Dict[str, Any]],
                     ep_rebinds: List[Tuple["Database", str]]) -> List[Dict[str, Any]]:
    """Flatten a plan into a CAPACITY-SAFE execution order: data ops (shard migrations
    + failovers) in the planner-validated `steps` order (which build_plan already
    checked free-before-fill), and each DB's endpoint ops (independent moves +
    endpoint_to_shards re-binds) placed right after that DB's LAST data op, so proxies
    realign to the moved masters. Endpoints are RAM/shard-neutral, so anchoring them
    there never disturbs the data ops' capacity safety. DBs with only an endpoint
    re-bind (no data ops) come last.

    This replaces the previous per-database grouping, which could run one DB's
    'fill node X' op before another DB's 'free node X' op and transiently over-commit."""
    data = [s for s in steps if s["kind"] in ("shard", "failover")]
    ep_by_db: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for s in steps:
        if s["kind"] == "endpoint":
            ep_by_db[s["db"]].append(_op_from_step(s))
    for db, _cmd in ep_rebinds:
        ep_by_db[db.uid].append({"kind": "ep_policy", "db": db.uid, "db_name": db.name,
                                 "policy": db.proxy_policy, "bytes": 0,
                                 "label": f"{db.name}: rebind endpoints to masters "
                                          f"(policy {db.proxy_policy})"})
    last_idx: Dict[int, int] = {}
    for i, s in enumerate(data):
        last_idx[s["db"]] = i
    ordered: List[Dict[str, Any]] = []
    done: set = set()
    for i, s in enumerate(data):
        ordered.append(_op_from_step(s))
        db = s["db"]
        if last_idx.get(db) == i and db in ep_by_db and db not in done:
            ordered.extend(ep_by_db[db])
            done.add(db)
    for db, ops in ep_by_db.items():   # endpoint-only DBs (no data ops)
        if db not in done:
            ordered.extend(ops)
            done.add(db)
    return ordered


def execution_order_violations(cluster: Cluster, caps: Dict[int, NodeCapacity],
                               ordered_ops: List[Dict[str, Any]]) -> List[str]:
    """Replay ordered_ops on a fresh model; return any point where a shard migration
    would exceed its target node's RAM ceiling or shard-count limit AT THE MOMENT it
    runs (what the live cluster would reject). Endpoints are RAM/shard-neutral.
    Callers apply this only in non---force mode (force over-commits by design).
    Normally empty - the emitted order IS the validated plan order; this is a guard so
    a future change can't silently push a doomed order to the cluster."""
    shard_by_uid = {s.uid: s for s in cluster.shards}
    live = _Live(cluster, caps)
    problems: List[str] = []
    for op in ordered_ops:
        if op["kind"] == "failover":
            live.do_failover(cluster.db_by_uid[op["db"]], op["master_shard"], op["slave_shard"])
        elif op["kind"] == "shard":
            db = cluster.db_by_uid[op["db"]]
            dst, c = op["dst"], caps[op["dst"]]
            if live.loads[dst].provisioned_memory + db.per_shard_memory > c.ram_ceiling + 1:
                problems.append(f"shard {op['shard']} -> node {dst}: RAM ceiling exceeded mid-plan")
            elif c.max_shards is not None and \
                    live.loads[dst].total_shards + live.other[dst] + 1 > c.max_shards:
                problems.append(f"shard {op['shard']} -> node {dst}: shard-count limit exceeded mid-plan")
            live.do_move(shard_by_uid[op["shard"]], dst, db)
    return problems


def render_commands(cluster: Cluster, caps: Dict[int, NodeCapacity],
                    steps: List[Dict[str, Any]], cur_state: Optional["_Live"],
                    planned_state: Optional["_Live"], deployer) -> str:
    """The Step-4 commands (display only), copy-paste ready, in CAPACITY-SAFE
    execution order (see _execution_order): data ops in the planner-validated plan
    order, each DB's endpoint re-bind right after that DB's last data op. Style
    matches the input source (rladmin CLI, or REST API calls for --rest)."""
    describer = deployer if deployer is not None else _RladminDeployer(None)
    out = [f"Execute the following {describer.label} commands to balance the cluster: (verify status after each step)",
           "-" * 90]
    rebinds = (endpoint_rebind_commands(cluster, cur_state, planned_state)
               if cur_state is not None and planned_state is not None else [])
    ordered = _execution_order(steps, rebinds)
    for op in ordered:
        out.append(describer.describe(op))
    if not ordered:
        out.append("(no changes - nothing to execute)")
    return "\n".join(out)


def render_plan(
    cluster: Cluster, caps: Dict[int, NodeCapacity],
    current_loads: Dict[int, NodeLoad], cur_score: BalanceScore,
    state: "_Live", des_score: BalanceScore,
    moves: List[Dict[str, Any]], steps: List[Dict[str, Any]],
    feas: Dict[str, Any], blocked_info: Tuple[bool, int, int],
    force: bool = False, raw_move_count: Optional[int] = None,
    planned_state: Optional["_Live"] = None,
    deployer=None, for_execute: bool = False,
) -> str:
    out: List[str] = []
    blocked, min_shard, free = blocked_info
    out.append("=" * 86)
    out.append(f"STEP 3 - REBALANCING PLAN          cluster: {cluster.name}")
    out.append("=" * 86)
    out.append("READ-ONLY: this presents the plan only. Nothing is changed on the cluster")
    out.append("(commands below are NOT executed here; Step 4 deploys them).")
    if force and not feas["feasible"]:
        out.append("MODE: --force (resource override) - a plan IS provided below even though the")
        out.append("cluster lacks resources; it may OVER-COMMIT nodes and is NOT deployable as-is.")
    out.append("")

    # Endpoint<->master alignment re-binds (endpoint_to_shards) are NOT in `moves`
    # (they are derived from the actual-vs-planned endpoint diff); count them so a
    # cluster that is resource-balanced but endpoint-misaligned still yields a plan.
    ep_rebinds = (endpoint_rebind_commands(cluster, _Live(cluster, caps), planned_state)
                  if planned_state is not None else [])

    # No-plan situations -------------------------------------------------- #
    if not feas["feasible"] and not force:
        out.append("NO PLAN - additional resources required first.")
        out.append("  Shortfall to balance:")
        for line in feas["lines"]:
            out.append(f"     {line}")
        if feas["recommendation"]:
            out.append(f"  Recommendation: {feas['recommendation']}")
        out.append("  (Re-run with --force to produce an over-committed plan anyway.)")
        return "\n".join(out)
    if not moves and not ep_rebinds:
        if not feas["feasible"]:  # forced, but no migration improves balance
            if des_score.is_balanced:
                reason = "the cluster is already balanced, so migration cannot improve it"
            else:
                reason = ("no single shard move / endpoint relocation improves balance under the "
                          "policy constraints (HA, rack, dense/sparse, shard limits)")
            out.append(f"FORCED PLAN: 0 migration steps - {reason}.")
            out.append("The shortfall is a resource deficit that migration cannot fix; "
                       "add capacity to resolve it:")
            for line in feas["lines"]:
                out.append(f"     {line}")
            if feas["recommendation"]:
                out.append(f"  Recommendation: {feas['recommendation']}")
        elif des_score.is_balanced:
            out.append(f"NO PLAN NEEDED - cluster is already balanced (score {cur_score.overall:.1f}/100).")
        elif blocked:
            need = max(0, min_shard - free)
            out.append("NO PLAN - cluster is imbalanced but too full to migrate shards.")
            out.append(f"  Hottest node's smallest shard ({fmt_bytes(min_shard)}) does not fit the")
            out.append(f"  largest free headroom on any other node ({fmt_bytes(free)}).")
            tail = f">= {fmt_bytes(need)} RAM headroom" if need > 0 else "RAM headroom / a node"
            out.append(f"  Recommendation: add {tail}, then re-run.")
        else:
            out.append(f"NO PLAN - balance cannot be improved with single moves/endpoint moves "
                       f"(score {cur_score.overall:.1f}). Residual is limited by placement")
            out.append("  policies (dense/sparse, HA, rack) and/or shard sizes.")
        # --force: append the CURRENT topology map (per-DB shard + endpoint placement)
        # so the balanced layout can be reviewed even when nothing more can change.
        if force and planned_state is not None:
            out.append("")
            out.append(render_cluster_map(cluster, caps, _Live(cluster, caps),
                                          planned_state, full_topology=True))
        return "\n".join(out)

    # Plan summary -------------------------------------------------------- #
    n_shard = sum(1 for s in steps if s["kind"] == "shard")
    n_ep = sum(1 for s in steps if s["kind"] == "endpoint")
    n_failover = sum(1 for s in steps if s["kind"] == "failover")
    total_bytes = sum(s["bytes"] for s in steps)
    n_total = len(steps) + len(ep_rebinds)
    out.append(f"Plan: {n_total} step(s)  |  score {cur_score.overall:.1f} -> {des_score.overall:.1f}"
               f"  ({des_score.overall - cur_score.overall:+.1f})  |  {n_shard} shard move(s), "
               f"{n_failover} failover(s), {n_ep + len(ep_rebinds)} endpoint re-bind(s)"
               f"  |  ~{fmt_bytes(total_bytes)} moved")
    if raw_move_count is not None and raw_move_count > len(steps):
        out.append(f"(compacted from {raw_move_count} to {len(steps)} operations - same final "
                   "balance, fewer migrations: pass-through moves of interchangeable shards removed)")
    if planned_state is not None:
        consolidated = []
        for db in movable_databases(cluster):
            if (db.shard_placement != "dense" or not cluster.config.respect_placement(db)
                    or not cluster.config.consolidate_dense(db)):
                continue
            masters = [s for s in cluster.shards_by_bdb[db.uid] if s.role == "master"]
            cur_n = len({s.node_uid for s in masters})
            new_n = len({planned_state.place[s.uid] for s in masters})
            if masters and new_n < cur_n:
                consolidated.append(f"{db.name}({cur_n}->{new_n} nodes)")
        if consolidated:
            out.append("Dense consolidation (policy priority): packed " + ", ".join(consolidated)
                       + "; the endpoint follows the masters.")
    out.append("")

    # Balance improvement (how the plan helps) ---------------------------- #
    host = [u for u in sorted(caps) if caps[u].hostable]
    cur_cpu = [_util(current_loads[u], caps[u])[1] for u in host if caps[u].cores > 0]
    new_cpu = [_util(state.loads[u], caps[u])[1] for u in host if caps[u].cores > 0]
    cur_ram = [_util(current_loads[u], caps[u])[0] for u in host if caps[u].ram_ceiling > 0]
    new_ram = [_util(state.loads[u], caps[u])[0] for u in host if caps[u].ram_ceiling > 0]

    def spread(xs: List[float]) -> float:  # max-min, in percentage points
        return (max(xs) - min(xs)) * 100 if xs else 0.0

    out.append("Balance improvement (lower spread = more even; spread = busiest - idlest node):")
    out.append(f"  overall score : {cur_score.overall:5.1f}  -> {des_score.overall:5.1f}   "
               f"({des_score.overall - cur_score.overall:+.1f})")
    if cur_cpu:
        out.append(f"  CPU spread    : {spread(cur_cpu):5.0f}pp -> {spread(new_cpu):5.0f}pp   "
                   f"(busiest node {max(cur_cpu)*100:.0f}% -> {max(new_cpu)*100:.0f}%)")
    if cur_ram and max(cur_ram + new_ram) > 0.0:
        out.append(f"  RAM spread    : {spread(cur_ram):5.0f}pp -> {spread(new_ram):5.0f}pp   "
                   f"(busiest node {max(cur_ram)*100:.0f}% -> {max(new_ram)*100:.0f}%)")
    else:
        out.append("  RAM spread    : n/a (no provisioned RAM to balance in this cluster)")
    out.append("")

    # Ordered steps - 'chg' shows how much each migration improves the score #
    _op_label = {"shard": "shard", "failover": "failovr", "endpoint": "endpt"}
    rows = []
    any_invalid = False
    for s in steps:
        any_invalid = any_invalid or not s["valid"]
        if s["kind"] == "endpoint":
            what = "endpoint"
        elif s["kind"] == "failover":
            what = f"failover #{s['shard']}" + (" (align)" if s.get("align") else "")
        else:
            what = f"{s['role']} #{s['shard']}"
        rows.append([
            s["step"],
            _op_label.get(s["kind"], s["kind"]),
            s["db_name"],
            what,
            f"node{s['src']} -> node{s['dst']}",
            fmt_bytes(s["bytes"]) if s["bytes"] else "-",
            f"{s['score_after']:.1f}",
            f"{s['delta']:+.1f}",
        ])
    # Endpoint re-binds (endpoint_to_shards) aren't in `steps` - append them so the
    # ordered list is complete when alignment needs an endpoint move.
    for db, _cmd in ep_rebinds:
        rows.append([len(rows) + 1, "endpt", db.name, "endpoint_to_shards",
                     "-> master node(s)", "-", f"{des_score.overall:.1f}", "align"])
    out.append("Ordered steps (score = balance after the step; chg = improvement from that step):")
    out.append(_table(
        ["#", "op", "db", "shard/ep", "move", "size", "score", "chg"],
        rows,
    ))
    if any_invalid:
        out.append("  (! a step exceeds a capacity limit - only possible under --force)")
    out.append("")

    # One unified nodes-as-columns map: utilisation on top, databases + migrations below.
    # verbose: show every DB's shard/endpoint placement (unchanged DBs included) so a
    # node's full load is visible, not just the DBs the plan touches.
    cur_state = _Live(cluster, caps) if planned_state is not None else None
    if cur_state is not None:
        out.append(render_cluster_map(cluster, caps, cur_state, planned_state,
                                      detail_unchanged=True))
        out.append("")

    out.append(render_commands(cluster, caps, steps, cur_state, planned_state, deployer))
    # The 'Honoured at EVERY step' footer and the trailing 'further balancing'
    # NOTE are planning context; suppress them when executing to keep the deploy
    # flow focused.
    if not for_execute:
        out.append("")
        if force:
            out.append("Honoured at EVERY step: HA anti-affinity, rack-awareness, dense/sparse, "
                       "shard<=max.  (RAM/CPU limits OVERRIDDEN by --force.)")
        else:
            out.append("Honoured at EVERY step: HA anti-affinity, rack-awareness, dense/sparse, "
                       "shard<=max, CPU<=cores, RAM ceiling.")

    if force and not feas["feasible"]:
        out.append("")
        out.append(f"The {len(steps)}-step plan above reaches {des_score.overall:.1f}/100 but "
                   "OVER-COMMITS resources (shortfall below).")
        out.append("It is provided for PLANNING ONLY and is NOT deployable as-is.")
        out.append("  Resource shortfall:")
        for line in feas["lines"]:
            out.append(f"     {line}")
        if feas["recommendation"]:
            out.append(f"  Recommendation: {feas['recommendation']}")
        out.append("  Add the capacity above and drop --force to obtain a deployable plan.")
    elif blocked or not des_score.is_balanced:
        if not for_execute:
            out.append("")
            out.append(f"NOTE: after this plan the cluster reaches {des_score.overall:.1f}/100. "
                       "Further balancing would require")
            out.append("  added RAM headroom or an additional node.")
    else:
        out.append("")
        out.append(f"RESULT: after this plan the cluster is balanced ({des_score.overall:.1f}/100).")
    return "\n".join(out)


def plan_as_dict(
    cluster: Cluster, cur_score: BalanceScore, des_score: BalanceScore,
    steps: List[Dict[str, Any]], feas: Dict[str, Any],
    blocked_info: Tuple[bool, int, int], force: bool = False,
    caps: Optional[Dict[int, NodeCapacity]] = None,
    planned_state: Optional["_Live"] = None,
) -> Dict[str, Any]:
    blocked, min_shard, free = blocked_info
    result = {
        "cluster": cluster.name,
        "read_only": True,
        "feasible": feas["feasible"],
        "forced": force,
        "deployable": not force and feas["feasible"],
        "score": {"current": round(cur_score.overall, 2), "final": round(des_score.overall, 2)},
        "step_count": len(steps),
        "steps": [
            {
                "step": s["step"], "operation": s["kind"], "db": s["db"],
                "db_name": s["db_name"], "role": s["role"], "shard": s["shard"],
                "slots": s["slots"], "from_node": s["src"], "to_node": s["dst"],
                "bytes": s["bytes"], "score_after": round(s["score_after"], 2),
                "valid": s["valid"], "command": _step_command(s),
            }
            for s in steps
        ],
        "rebalance_blocked_by_memory": blocked,
        "feasibility": {
            "ram_short": feas["ram_short"], "cpu_short": feas["cpu_short"],
            "lines": feas["lines"], "recommendation": feas["recommendation"],
        },
    }
    if caps is not None:
        cur_live = _Live(cluster, caps)
        result["topology"] = {
            "current": _topology_dict(cluster, cur_live),
            "planned": _topology_dict(cluster, planned_state) if planned_state else None,
        }
        if planned_state is not None:
            result["node_plan"] = _node_plan_data(cluster, caps, cur_live, planned_state)
    return result


# --------------------------------------------------------------------------- #
# REST API input (additive; standalone). Stdlib only (urllib + ssl).
# This first step fetches read-only state and prints an `rladmin status`-
# equivalent dump. It does NOT touch any of the rladmin code paths.
# --------------------------------------------------------------------------- #
class RestClient:
    """Client for the Redis Enterprise Software REST API: read-only for discovery,
    plus POST/PUT for the deploy step (shard migration + endpoint re-bind)."""

    def __init__(self, fqdn: str, user: str, password: str, port: int = 9443,
                 verify_tls: bool = False, timeout: int = 30) -> None:
        self.base = f"https://{fqdn}:{port}"
        self.timeout = timeout
        self._auth = "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()
        self._ctx = ssl.create_default_context()
        if not verify_tls:
            self._ctx.check_hostname = False
            self._ctx.verify_mode = ssl.CERT_NONE

    def _open(self, method: str, path: str, body=None, timeout: Optional[float] = None):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        headers = {"Authorization": self._auth, "Accept": "application/json"}
        if data is not None:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(self.base + path, data=data, method=method, headers=headers)
        return urllib.request.urlopen(
            req, timeout=self.timeout if timeout is None else timeout, context=self._ctx)

    def get(self, path: str, timeout: Optional[float] = None):
        try:
            with self._open("GET", path, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            if exc.code == 401:
                raise SystemExit("REST auth failed (401). Check --rest-user / --rest-password.")
            raise SystemExit(f"REST GET {path} -> HTTP {exc.code}: {exc.read()[:300]!r}")
        except urllib.error.URLError as exc:
            raise SystemExit(f"REST GET {path} failed: {exc.reason} (check --rest-fqdn).")
        except (ValueError, OSError) as exc:
            raise SystemExit(f"REST GET {path} error: {exc}")

    def mutate(self, method: str, path: str, body=None, timeout: Optional[float] = None):
        """POST/PUT for the deploy step. Returns (ok, status, data); NEVER raises,
        so a single failed op is reported without crashing the run."""
        try:
            with self._open(method, path, body, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", "replace")
                try:
                    data = json.loads(raw) if raw.strip() else {}
                except ValueError:
                    data = raw
                return True, getattr(resp, "status", 200), data
        except urllib.error.HTTPError as exc:
            return False, exc.code, exc.read().decode("utf-8", "replace")[:300]
        except urllib.error.URLError as exc:
            return False, 0, str(exc.reason)
        except OSError as exc:
            return False, 0, str(exc)

    def wait_action(self, action_uid: str, timeout: float = 900, interval: float = 2):
        """Poll /v1/actions/<uid> until terminal, within a wall-clock `timeout`.
        Returns (ok, message)."""
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False, f"action {action_uid}: timed out after {timeout:.0f}s"
            try:
                with self._open("GET", f"/v1/actions/{action_uid}",
                                timeout=remaining) as resp:
                    info = json.loads(resp.read().decode("utf-8"))
            except (urllib.error.URLError, ValueError, OSError) as exc:
                return False, f"action {action_uid}: status poll failed ({exc})"
            status = str(info.get("status", "")).lower()
            if status in ("completed", "finished", "success", "succeeded"):
                return True, f"action {action_uid}: {status}"
            if status in ("failed", "error", "aborted", "cancelled"):
                return False, f"action {action_uid}: {status} ({info.get('error', '')})"
            time.sleep(min(interval, max(0.0, deadline - time.monotonic())))


# Redis Enterprise provisional-RAM model (see "Provisional RAM", RE docs):
#   ceiling   = total - reserved(6%) - provision_threshold(MAX of % and absolute)
#   available = FREE_RAM - room_for_growth - reserved(6%) - provision_threshold
#   room_for_growth = sum over shards on the node of max(0, fair_share - used)
#   fair_share      = db.memory_size / shards_count / (2 if replication else 1)
_RESERVED_PCT = 0.06          # fixed internal safety buffer for cluster processes
_PROVISION_THRESHOLD_PCT = 0.12   # default; overridden by cluster policy if present


def _rest_provisional_ram(nodes, bdbs, shards, stats, shard_used, policy):
    """Reproduce rladmin's PROVISIONAL_RAM (available, ceiling) per node uid,
    in bytes, from REST data. Returns {uid: (available, ceiling)}."""
    policy = policy or {}
    # Cluster policy key is "..._p" (percent); accept "..._percent" too, just in case.
    pct = policy.get("redis_provision_node_threshold_p",
                     policy.get("redis_provision_node_threshold_percent"))
    pct = (float(pct) / 100.0) if pct not in (None, "") else _PROVISION_THRESHOLD_PCT
    abs_thr = policy.get("redis_provision_node_threshold")
    try:
        abs_thr = float(abs_thr) if abs_thr not in (None, "") else 0.0
    except (ValueError, TypeError):
        abs_thr = 0.0

    bdb_by = {b["uid"]: b for b in bdbs}

    # fair share (bytes) each physical shard of a db reserves for growth.
    fair_share: Dict[int, float] = {}
    for b in bdbs:
        msize = b.get("memory_size") or 0
        sc = b.get("shards_count") or 1
        repl = 2 if b.get("replication") else 1
        fair_share[b["uid"]] = float(msize) / float(sc) / float(repl)

    # room for growth per node = sum of (fair_share - used), clamped at 0 per shard.
    room: Dict[int, float] = defaultdict(float)
    for s in shards:
        nu = int(s["node_uid"])
        fs = fair_share.get(s["bdb_uid"], 0.0)
        used = shard_used.get(int(s["uid"]))
        if used is None:
            used = s.get("used_memory") or 0
        room[nu] += max(0.0, fs - float(used or 0))

    out: Dict[int, tuple] = {}
    for n in nodes:
        uid = int(n["uid"])
        total = float(n.get("total_memory") or 0)
        st = stats.get(uid, {})
        reserved = total * _RESERVED_PCT
        threshold = max(abs_thr, total * pct)
        # Ceiling (max provisionable) has no direct stat field -> derive it:
        #   total - reserved(6%) - provision_threshold(default 12%).
        ceiling = total - reserved - threshold
        # RE publishes the live available value directly; prefer it. Fall back to
        # the documented formula (FREE_RAM - room_for_growth - reserved - threshold)
        # only when the stat is missing (older versions / offline capture).
        available = st.get("provisional_memory")
        if available in (None, ""):
            free = st.get("free_memory")
            available = (float(free) - room[uid] - reserved - threshold
                         ) if free not in (None, "") else None
        else:
            available = float(available)
        out[uid] = (available, ceiling)
    return out


def _rest_keyed_by_uid(raw) -> Dict[int, Dict[str, Any]]:
    """Normalise a /v1/.../stats/last payload (dict-keyed or list) into
    {uid(int): record}."""
    out: Dict[int, Dict[str, Any]] = {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            if isinstance(v, dict):
                try:
                    out[int(v.get("uid", k))] = v
                except (ValueError, TypeError):
                    pass
    elif isinstance(raw, list):
        for v in raw:
            if isinstance(v, dict) and v.get("uid") is not None:
                try:
                    out[int(v["uid"])] = v
                except (ValueError, TypeError):
                    pass
    return out


def _rest_node_stats(client: RestClient) -> Dict[int, Dict[str, Any]]:
    """uid -> per-node live stats (free/available/provisional memory). Best-effort."""
    try:
        return _rest_keyed_by_uid(client.get("/v1/nodes/stats/last"))
    except SystemExit:
        return {}


def _rest_shard_used(client: RestClient) -> Dict[int, Any]:
    """uid -> shard used_memory (lives in stats, not the /v1/shards config). Best-effort."""
    try:
        return {uid: rec.get("used_memory")
                for uid, rec in _rest_keyed_by_uid(client.get("/v1/shards/stats/last")).items()}
    except SystemExit:
        return {}


def discover_from_rest(client: RestClient) -> Cluster:
    """Build the cluster inventory from the REST API (read-only).

    Populates the SAME Cluster/Node/Database/Shard model as discover() and
    discover_from_status_file(), so the Step 1-3 scoring and planning code is
    identical regardless of input source. REST feeds configured limits and the
    real proxy_policy directly (more precise than the status-file path).
    """
    nodes = client.get("/v1/nodes")
    bdbs = client.get("/v1/bdbs")
    shards = client.get("/v1/shards")
    try:
        policy = client.get("/v1/cluster/policy")
    except SystemExit:
        policy = {}
    try:
        cinfo = client.get("/v1/cluster")
    except SystemExit:
        cinfo = {}
    stats = _rest_node_stats(client)
    shard_used = _rest_shard_used(client)

    cluster = Cluster(
        name=(cinfo.get("name") if isinstance(cinfo, dict) else None) or "redis-enterprise-cluster",
        ram_source="REST /v1/nodes/stats/last (provisional_memory); shard memory = configured limit",
    )
    cluster.config = Config()  # default; overridden by --config in run()
    cluster.rack_aware = bool(policy.get("rack_aware")) if isinstance(policy, dict) else False

    # Provisional RAM (available, ceiling) per node, RE-style. We store the
    # AVAILABLE value as Node.provisional_ram (same semantic as rladmin's
    # PROVISIONAL_RAM numerator); ram_capacity = provisioned + provisional.
    prov_ram = _rest_provisional_ram(nodes, bdbs, shards, stats, shard_used, policy)
    for n in nodes:
        uid = int(n["uid"])
        status = (n.get("status") or "active").lower()
        ms = n.get("max_redis_servers")
        max_shards = int(ms) if ms not in (None, "") else None
        avail, _ceiling = prov_ram.get(uid, (None, None))
        rack = (n.get("rack_id") or "").strip() or None
        if rack:
            cluster.rack_aware = True
        accept = n.get("accept_servers", True)
        hostable = status not in ("down",) and bool(accept) and max_shards != 0
        cluster.nodes.append(Node(
            uid=uid,
            addr=n.get("addr") or "?",
            total_memory=int(n.get("total_memory") or 0),
            cores=int(n.get("cores") or 0),
            status=status,
            rack_id=rack,
            provisional_ram=int(avail) if avail is not None else None,
            max_shards=max_shards,
            hostable=hostable,
        ))

    # Shards (also a fallback shard-count source for databases).
    masters_per_db: Dict[int, int] = defaultdict(int)
    for s in shards:
        bdb_uid, node_uid, shard_uid = int(s["bdb_uid"]), int(s["node_uid"]), int(s["uid"])
        role = (s.get("role") or "master").lower()
        used = shard_used.get(shard_uid)
        if used is None:
            used = s.get("used_memory") or 0
        cluster.shards.append(Shard(
            uid=shard_uid, role=role, bdb_uid=bdb_uid, node_uid=node_uid,
            used_memory=int(used or 0), slots=str(s.get("assigned_slots") or ""),
        ))
        if role == "master":
            masters_per_db[bdb_uid] += 1

    # Actual endpoint placement per DB: resolve each bdb endpoint's addr(es) to a
    # node uid (for endpoint<->master alignment detection). addr -> uid map covers
    # both internal and external node addresses.
    addr2uid: Dict[str, int] = {}
    for n in nodes:
        ext = n.get("external_addr")
        addrs = [n.get("addr")] + (ext if isinstance(ext, list) else [ext])
        for a in addrs:
            if a:
                addr2uid[a] = int(n["uid"])
    ep_by_db: Dict[int, set] = {}
    for b in bdbs:
        node_uids: set = set()
        for e in (b.get("endpoints") or []):
            if not isinstance(e, dict):
                continue
            raw = e.get("addr")
            for a in (raw if isinstance(raw, list) else ([raw] if raw else [])):
                u = addr2uid.get(a)
                if u is not None:
                    node_uids.add(u)
        if node_uids:
            ep_by_db[int(b["uid"])] = node_uids
    cluster.endpoints_by_db = ep_by_db

    # Databases.
    for b in bdbs:
        db_id = int(b["uid"])
        sc = b.get("shards_count")
        shards_count = int(sc) if sc not in (None, "") else masters_per_db.get(db_id, 0)
        proxy_policy = (b.get("proxy_policy") or "").strip().lower()
        if not proxy_policy:  # fall back to the (per-)endpoint policy, else 'single'.
            eps = b.get("endpoints") or []
            if eps and isinstance(eps[0], dict):
                proxy_policy = (eps[0].get("proxy_policy") or "").strip().lower()
        bigstore_ram = b.get("bigstore_ram_size") or 0
        cluster.databases.append(Database(
            uid=db_id,
            name=b.get("name") or f"bdb:{db_id}",
            memory_size=int(b.get("memory_size") or 0),
            shards_count=shards_count,
            replication=bool(b.get("replication")),
            sharding=bool(b.get("sharding")) or shards_count > 1,
            shard_placement=(b.get("shards_placement") or "dense").lower(),
            proxy_policy=proxy_policy or "single",
            db_type=(b.get("type") or "redis").lower(),
            is_flex=bool(b.get("bigstore")) or float(bigstore_ram or 0) > 0,
        ))

    cluster.index()
    return cluster


def _rest_client_from_args(args: argparse.Namespace) -> Optional[RestClient]:
    fqdn, user = args.rest_fqdn, args.rest_user
    password = args.rest_password or os.environ.get("RL_REST_PASSWORD")
    missing = [name for name, val in (("--rest-fqdn", fqdn), ("--rest-user", user),
                                      ("--rest-password", password)) if not val]
    if missing:
        sys.stderr.write("REST needs: " + ", ".join(missing) +
                         "  (password may also come from env RL_REST_PASSWORD)\n")
        return None
    port_src = args.rest_port if args.rest_port is not None else os.environ.get("RL_REST_PORT")
    try:
        port = int(port_src) if port_src not in (None, "") else 9443
    except (TypeError, ValueError):
        sys.stderr.write(f"Invalid --rest-port {port_src!r}; must be an integer.\n")
        return None
    return RestClient(fqdn, user, password, port=port)  # self-signed TLS (RE default)


def _merge_exclude_nodes(cluster: Cluster, cli_value: Optional[str]) -> None:
    """Merge the --exclude-nodes CLI value (comma/semicolon-separated uids) into the
    cluster config's exclude_nodes list, and warn about unknown uids or a fully
    excluded cluster. The exclusion itself is applied later (apply_node_exclusions)."""
    cli: set = set()
    for tok in (cli_value or "").replace(";", ",").split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            cli.add(int(tok))
        except ValueError:
            sys.stderr.write(f"Ignoring --exclude-nodes entry {tok!r} (not an integer).\n")
    merged = set(cluster.config.excluded_nodes()) | cli
    if not merged:
        return
    cluster.config.cluster["exclude_nodes"] = sorted(merged)
    known = {n.uid for n in cluster.nodes}
    unknown = merged - known
    if unknown:
        sys.stderr.write("WARNING: exclude_nodes references unknown node uid(s): "
                         + ", ".join(map(str, sorted(unknown))) + "\n")
    if not any(n.uid not in merged and n.hostable for n in cluster.nodes):
        sys.stderr.write("WARNING: excluding those nodes leaves no hostable node to balance "
                         "onto; the plan will be empty.\n")


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Redis Enterprise Software database rebalancing tool. Input from live "
                    "rladmin (default; path via --rladmin-path), a captured status file "
                    "(--status-file), or the REST API (--rest) - all feed the same scoring/planning. "
                    "'plan' (default) runs the read-only steps 1-3 (current layout, desired "
                    "layout, rebalancing plan). 'execute' runs the plan then performs the "
                    "migrations after approval - supported with live rladmin OR --rest "
                    "(--status-file is plan-only).",
    )
    p.add_argument(
        "mode", nargs="?", choices=["plan", "execute"], default="plan",
        help="plan (default): produce the read-only plan (steps 1-3). "
             "execute: produce the plan, then run the migrations after approval "
             "(live rladmin or --rest input).",
    )

    inp = p.add_argument_group(
        "input source",
        "Choose ONE source. Default: live rladmin on this cluster node. rladmin and "
        "--rest can 'execute'; --status-file is plan-only (offline).")
    inp.add_argument("--rladmin-path", default="rladmin", metavar="PATH",
                     help="DEFAULT input: live rladmin. PATH to the rladmin binary (default: "
                          "'rladmin' on PATH; on a cluster node it is usually "
                          "/opt/redislabs/bin/rladmin).")
    inp.add_argument("--status-file", metavar="FILE",
                     help="INPUT (PLAN ONLY): read a captured `rladmin status extra all > FILE` "
                          "instead of a live cluster. Shard memory uses USED_MEMORY since the "
                          "configured limit is not in status output.")
    inp.add_argument("--rest", action="store_true",
                     help="INPUT: use the cluster REST API. Can plan AND execute (shard migration "
                          "+ endpoint policy re-bind); endpoint-to-node relocation is rladmin-only. "
                          "Requires --rest-fqdn/--rest-user/--rest-password.")

    rules = p.add_argument_group("rules")
    rules.add_argument("--config", metavar="FILE",
                       help="JSON config: cluster-wide + per-database rules (CPU weights, "
                            "respect_shard_placement, consolidate_dense, respect_endpoint_policy, "
                            "exclude_from_balancing, exclude_nodes). Per-DB entries override "
                            "cluster defaults.")
    rules.add_argument("--force", action="store_true",
                       help="RESOURCE OVERRIDE: still produce a balancing plan when the cluster "
                            "lacks RAM/CPU (the plan may over-commit nodes). Warns and is NOT "
                            "deployable; policy constraints (HA, rack, dense/sparse, shard "
                            "limits) are still enforced.")
    rules.add_argument("--exclude-nodes", metavar="UIDS",
                       help="Comma-separated node uids to leave untouched (e.g. 3,5): nothing "
                            "migrates onto them and their shards stay put. Merged with the "
                            "config 'exclude_nodes' list.")

    out = p.add_argument_group("output")
    out.add_argument("--format", choices=["text", "json", "html"], default="text",
                     help="Output format: text (default), json (machine-readable), or html "
                          "(self-contained report; redirect to a .html file). Text colour is "
                          "auto (only on a terminal).")
    out.add_argument("--verbose", action="store_true",
                     help="Print the full report (Steps 1-3 + node-removal analysis). Without it, "
                          "text output is concise: just the cluster map (changed DBs) and the "
                          "Step-4 commands.")

    rest = p.add_argument_group("rest (REST API input)")
    rest.add_argument("--rest-fqdn", metavar="FQDN", help="Cluster FQDN/host for the REST API.")
    rest.add_argument("--rest-user", metavar="USER", help="REST API user (cluster admin).")
    rest.add_argument("--rest-password", metavar="PW",
                      help="REST API password (or env RL_REST_PASSWORD).")
    rest.add_argument("--rest-port", metavar="PORT", type=int, default=None,
                      help="REST API port (default 9443, or env RL_REST_PORT).")
    return p


class _Ctx:
    """All analysis artifacts, computed once and shared across the steps."""
    def __init__(self, cluster: Cluster, force: bool = False) -> None:
        self.force = force
        self.cluster = cluster
        apply_node_exclusions(self.cluster)   # honour config/CLI exclude_nodes
        current = Placement.current(self.cluster)
        eps = self.cluster.endpoints_by_db
        self.current_loads = compute_loads(self.cluster, current, eps)
        self.caps = node_capacities(self.cluster, self.current_loads)
        # Step 1 artifacts
        self.cur_views = build_node_views(self.cluster, current, eps)
        self.cur_reqs = {d.uid: database_requirements(self.cluster, current, d)
                         for d in in_scope_databases(self.cluster)}
        self.s1_score = score_layout(self.cur_views)
        self.cur_score = score_from_loads(self.current_loads, self.caps)
        # Step 2 artifacts (force relaxes RAM/CPU resource limits)
        self.state, greedy_moves, self.db_caps = optimize(self.cluster, self.caps, force=force)
        # Reduce churn: replace with a minimal-move equivalent plan when possible.
        compacted = compact_plan(self.cluster, self.caps, self.state, greedy_moves,
                                 self.db_caps, force=force)
        self.moves = compacted if compacted is not None else greedy_moves
        self.raw_move_count = len(greedy_moves)
        self.des_score = score_from_loads(self.state.loads, self.caps)
        self.feas = feasibility(self.cluster, self.caps, self.state)
        self.blocked_info = (False, 0, 0)
        if not self.des_score.is_balanced:
            self.blocked_info = rebalance_blocked_by_memory(
                self.cluster, self.caps, self.state, self.db_caps)
        # Step 3 artifacts
        self.steps, _, self.planned_state = build_plan(
            self.cluster, self.caps, self.moves, self.db_caps, force=force)


# Health-gate wall-clock budgets after a migration op are configurable per cluster
# (shard_migration_check_timeout / endpoint_migration_check_timeout, default 30s;
# see CONFIG_DEFAULTS and Config.shard_check_timeout / endpoint_check_timeout). They
# must be generous enough for at least one full `rladmin status extra all` (a heavy
# command that reconnects and dumps every section) to complete AND the op to settle.
# REST status reads return instantly, so the REST gate waits this many seconds before
# its FIRST health poll (bounded by the timeout) - otherwise it could report OK before
# a just-issued op has begun reconfiguring (the settling the proxy watchdog reflects).
REST_SETTLE_WAIT = 3


# --------------------------------------------------------------------------- #
# Deployers - turn abstract plan ops into rladmin commands or REST calls, and
# expose a health() check for the between-steps gate. Execute is supported over
# rladmin (all ops) and REST (shard migration + endpoint policy re-bind);
# --status-file has no live cluster. An op is a dict:
#   {kind: 'shard'|'ep_node'|'ep_policy', ...}
#   shard     -> migrate one shard to dst node
#   ep_node   -> relocate a single-proxy endpoint to a specific node
#                (only when respect_endpoint_policy=false; rladmin-only)
#   ep_policy -> re-align a DB's endpoints to its (new) master shards. rladmin:
#                `migrate db .. endpoint_to_shards commit`. REST has no such action
#                -> `PUT /v1/bdbs/<id>` {proxy_policy, endpoint} (equivalent end state)
# --------------------------------------------------------------------------- #
class _RladminDeployer:
    label = "rladmin"

    def __init__(self, client: RladminClient) -> None:
        self.client = client

    @staticmethod
    def supports(op: Dict[str, Any]) -> bool:
        return True  # rladmin can do all three op kinds

    @staticmethod
    def _args(op: Dict[str, Any]) -> List[str]:
        if op["kind"] == "shard":
            return ["migrate", "shard", str(op["shard"]), "target_node", str(op["dst"])]
        if op["kind"] == "failover":
            return ["failover", "db", f"db:{op['db']}", "shard", str(op["master_shard"])]
        if op["kind"] == "ep_node":
            return ["bind", "db", f"db:{op['db']}", "endpoint", f"{op['db']}:1",
                    "node", str(op["dst"])]
        # ep_policy: re-bind the DB's endpoint(s) to its (new) master set. Verified
        # on the lab (2026-07): endpoint_to_shards binds a proxy on EVERY master node
        # for all-master-shards/-proxies, and the single proxy on the majority-masters
        # node for single policy - full add/remove/move, idempotent, policy-preserving.
        return ["migrate", "db", f"db:{op['db']}", "endpoint_to_shards", "commit"]

    def describe(self, op: Dict[str, Any]) -> str:
        return "rladmin " + " ".join(self._args(op))

    def run(self, op: Dict[str, Any]) -> Tuple[bool, str]:
        rc, output = self.client.execute(self._args(op))
        return rc == 0, (output or "").strip()[:300]

    def health(self, cmd_timeout: float) -> Tuple[bool, List[str]]:
        return cluster_health(self.client, cmd_timeout)


class _RestDeployer:
    """Executes over the REST API. shard migrate -> POST /v1/shards/<uid>/actions/
    migrate {target_node_uid}; endpoint re-bind -> PUT /v1/bdbs/<db> with BOTH
    proxy_policy AND the endpoint uid (the endpoint field forces RE to re-evaluate
    and prune stale proxies; proxy_policy alone is a no-op). NOTE: REST has no
    `endpoint_to_shards` action (unlike rladmin), so this PUT is the native
    equivalent - it re-aligns proxies to the current master set with the same end
    state. ep_node (pin a single endpoint to a specific node) is rladmin-only."""
    label = "REST"

    def __init__(self, client: RestClient) -> None:
        self.client = client

    @staticmethod
    def supports(op: Dict[str, Any]) -> bool:
        return op["kind"] in ("shard", "failover", "ep_policy")

    def describe(self, op: Dict[str, Any]) -> str:
        if op["kind"] == "shard":
            return (f'POST /v1/shards/{op["shard"]}/actions/migrate  '
                    f'{{"target_node_uid": {op["dst"]}}}')
        if op["kind"] == "failover":
            return f'POST /v1/shards/{op["master_shard"]}/actions/failover'
        if op["kind"] == "ep_policy":
            return (f'PUT /v1/bdbs/{op["db"]}  '
                    f'{{"proxy_policy": "{op["policy"]}", "endpoint": "{op["db"]}:1"}}')
        return f'(endpoint->node not supported over REST) {op["label"]}'

    def run(self, op: Dict[str, Any]) -> Tuple[bool, str]:
        if op["kind"] == "shard":
            ok, status, data = self.client.mutate(
                "POST", f"/v1/shards/{op['shard']}/actions/migrate",
                {"target_node_uid": int(op["dst"])})
            if not ok:
                return False, f"HTTP {status}: {data}"
            action = data.get("action_uid") if isinstance(data, dict) else None
            if action:  # migrate is async -> wait for the action to finish
                return self.client.wait_action(action)
            return True, f"HTTP {status}"
        if op["kind"] == "failover":
            ok, status, data = self.client.mutate(
                "POST", f"/v1/shards/{op['master_shard']}/actions/failover", {})
            if not ok:
                return False, f"HTTP {status}: {data}"
            action = data.get("action_uid") if isinstance(data, dict) else None
            return self.client.wait_action(action) if action else (True, f"HTTP {status}")
        if op["kind"] == "ep_policy":
            # proxy_policy alone is a no-op; the 'endpoint' field forces re-eval + prune.
            ok, status, data = self.client.mutate(
                "PUT", f"/v1/bdbs/{op['db']}",
                {"proxy_policy": op["policy"], "endpoint": f"{op['db']}:1"})
            return ok, (f"HTTP {status}" if ok else f"HTTP {status}: {data}")
        return False, "endpoint->node re-bind not supported over REST"

    def health(self, cmd_timeout: float) -> Tuple[bool, List[str]]:
        return rest_health(self.client, cmd_timeout)


def cluster_health(client: RladminClient, cmd_timeout: float = 60) -> Tuple[bool, List[str]]:
    """Run a FRESH `rladmin status extra all` and report any node/db/shard/endpoint
    not in a healthy state. Returns (ok, problems). Used as the deploy health gate.
    cmd_timeout bounds the status subprocess (a stuck/unresponsive check counts as
    a problem, not a hang)."""
    try:
        text = client.status_all(refresh=True, timeout=cmd_timeout)
    except SystemExit as exc:
        return False, [f"status check unavailable: {exc}"]
    secs = split_status_sections(text)
    problems: List[str] = []
    for row in parse_status_table(_find_section(secs, "NODES"), "NODE:ID"):
        st = (row.get("STATUS") or "").upper()
        if st and st != "OK":
            problems.append(f"{row.get('NODE:ID', 'node')} STATUS={row.get('STATUS')}")
    for row in parse_status_table(_find_section(secs, "DATABASES"), "DB:ID"):
        st = (row.get("STATUS") or "").lower()
        if st and st not in ("active", "ok"):
            problems.append(f"{row.get('DB:ID', 'db')} ({row.get('NAME', '')}) "
                            f"STATUS={row.get('STATUS')}")
    for row in parse_status_table(_find_section(secs, "SHARDS"), "DB:ID"):
        st = (row.get("STATUS") or "").upper()
        wd = (row.get("WATCHDOG_STATUS") or "").upper()
        if (st and st not in ("OK", "ACTIVE")) or (wd and wd != "OK"):
            problems.append(f"shard {row.get('ID', '?')} ({row.get('DB:ID', '')}) on "
                            f"{row.get('NODE', '?')}: STATUS={row.get('STATUS', '?')}"
                            f" WATCHDOG={row.get('WATCHDOG_STATUS', '-')}")
    for row in parse_status_table(_find_section(secs, "ENDPOINTS"), "DB:ID"):
        wd = (row.get("WATCHDOG_STATUS") or "").upper()
        if wd and wd != "OK":
            problems.append(f"endpoint {row.get('ID', '?')} ({row.get('DB:ID', '')}) "
                            f"WATCHDOG={row.get('WATCHDOG_STATUS')}")
    return (not problems), problems


_ACTION_IN_PROGRESS = ("pending", "active", "queued", "running", "starting", "initializing")


def rest_health(client: RestClient, cmd_timeout: float = 30) -> Tuple[bool, List[str]]:
    """REST equivalent of cluster_health: flag any node/db/shard not active/ok via
    /v1/nodes, /v1/bdbs, /v1/shards. NOTE: the rladmin WATCHDOG_STATUS (e.g. 'internal
    disable', 'multiple nodes: [...]') during shard/endpoint settling is a proxy/DMC
    view NOT exposed on the REST shard/endpoint objects; to catch that window, also
    poll /v1/actions and treat any in-progress state-machine/task as 'not settled yet'
    (so the gate keeps waiting until the cluster has no operation in flight). Each GET
    is bounded by cmd_timeout so a stuck check counts as a problem, not a hang.
    Returns (ok, problems)."""
    try:
        nodes = client.get("/v1/nodes", timeout=cmd_timeout)
        bdbs = client.get("/v1/bdbs", timeout=cmd_timeout)
        shards = client.get("/v1/shards", timeout=cmd_timeout)
    except SystemExit as exc:
        return False, [f"status check unavailable: {exc}"]
    problems: List[str] = []
    for n in nodes if isinstance(nodes, list) else []:
        st = str(n.get("status", "")).lower()
        if st and st != "active":
            problems.append(f"node:{n.get('uid')} status={n.get('status')}")
    # Per-DB shard-instance count, to detect a not-yet-cleaned-up migration (a leftover
    # copy on the source node leaves MORE instances than the DB should have).
    inst_count: Dict[int, int] = {}
    for s in shards if isinstance(shards, list) else []:
        try:
            inst_count[int(s.get("bdb_uid"))] = inst_count.get(int(s.get("bdb_uid")), 0) + 1
        except (TypeError, ValueError):
            continue
    for b in bdbs if isinstance(bdbs, list) else []:
        st = str(b.get("status", "")).lower()
        if st and st != "active":  # 'active-change-pending' etc. -> keep waiting
            problems.append(f"db:{b.get('uid')} ({b.get('name', '')}) status={b.get('status')}")
        sc = b.get("shards_count")
        if sc not in (None, "") and int(sc) > 0:  # expected total instances = masters * (2 if repl)
            expected = int(sc) * (2 if b.get("replication") else 1)
            actual = inst_count.get(int(b.get("uid")), expected)
            if actual != expected:
                problems.append(f"db:{b.get('uid')} ({b.get('name', '')}) has {actual} shard "
                                f"instance(s), expected {expected} (migration not fully settled?)")
    for s in shards if isinstance(shards, list) else []:
        st = str(s.get("status", "")).lower()
        ds = str(s.get("detailed_status", "")).lower()
        if (st and st != "active") or (ds and ds not in ("ok", "none")):
            problems.append(f"shard {s.get('uid')} (db:{s.get('bdb_uid')}) "
                            f"status={s.get('status')} detailed={s.get('detailed_status')}")
    # In-progress cluster operations (shard migration / endpoint reconfiguration /
    # rebalancing). The proxy watchdog settling that rladmin shows is not on the
    # shard/endpoint REST objects, but the driving state-machine IS visible here, so
    # a running action means the previous op has not fully settled -> keep waiting.
    try:
        acts = client.get("/v1/actions", timeout=cmd_timeout)
    except SystemExit:
        acts = None  # older RE / transient: don't fail the whole check on this alone
    items: List[Any] = []
    if isinstance(acts, dict):
        items = (acts.get("actions") or []) + (acts.get("state-machines") or [])
    elif isinstance(acts, list):
        items = acts
    for a in items:
        if not isinstance(a, dict):
            continue
        st = str(a.get("status", "")).lower()
        if st in _ACTION_IN_PROGRESS:
            who = a.get("object_name") or a.get("name") or a.get("action_uid") or "?"
            prog = a.get("progress")
            problems.append(f"operation '{a.get('name', '?')}' on {who} {st}"
                            + (f" ({prog}%)" if prog not in (None, "") else ""))
    return (not problems), problems


def wait_cluster_ok(health, timeout: float = DEFAULT_CHECK_TIMEOUT, interval: float = 3,
                    settle: float = 0.0, min_hold: float = 0.0) -> Tuple[bool, List[str]]:
    """Poll a deployer's health(cmd_timeout) -> (ok, problems) until OK, within a HARD
    wall-clock budget of `timeout` seconds from now (monotonic).

    Each check is given the FULL remaining budget as its own timeout, so a slow
    `rladmin status extra all` is allowed to finish (it is never chopped to a couple
    of seconds). To keep the hard cap, we simply stop STARTING new checks once too
    little budget remains for one to complete - never firing a doomed micro-check
    (which is what produced the misleading 'did not respond within 2s'). On failure we
    return the last real status, or - if even the first, full-budget check could not
    finish - a clean 'within <timeout>s' timeout.

    `settle` delays the FIRST check by that many seconds. `min_hold` is a MINIMUM quiet
    period: OK is not declared before it elapses, and the check keeps re-verifying
    throughout - so a state that only looks clean because REST can't see a lingering
    proxy/DMC reconciliation is given time to actually settle. Both are bounded so a
    full check still fits in the budget. Returns (ok, problems)."""
    start = time.monotonic()
    deadline = start + timeout
    _fit = max(0.0, timeout - max(interval, 3.0))  # leave room for one real check
    hold_until = start + min(min_hold, _fit)       # don't return OK before this
    problems: List[str] = []
    if settle > 0:
        time.sleep(min(settle, _fit))
    slowest = 0.0          # longest observed check, to size the "enough budget?" floor
    first = True
    while True:
        remaining = deadline - time.monotonic()
        # Need enough time for a check to actually complete; otherwise stop (don't fire
        # a check that would be killed mid-command). The first check always runs.
        if remaining <= 0 or (not first and remaining < max(slowest, 3.0)):
            break
        t0 = time.monotonic()
        ok, problems = health(remaining)       # full remaining budget for this check
        slowest = max(slowest, time.monotonic() - t0)
        first = False
        if ok and time.monotonic() >= hold_until:   # clean AND minimum hold elapsed
            return True, problems
        # not OK, or OK but still within the minimum hold -> keep polling
        remaining = deadline - time.monotonic()
        if remaining < max(slowest, 3.0):       # no room for another real check
            break
        time.sleep(min(interval, remaining))    # re-verify, never past the deadline
    return False, problems


def deploy(ctx: _Ctx, deployer, args: argparse.Namespace, verbose: bool = False) -> int:
    """STEP 4: execute the planned migrations - the ONLY step that mutates the
    cluster. Warns, then requires explicit operator approval before any change.
    Reached for the live rladmin OR REST input (--status-file is plan-only).

    After EACH shard/endpoint operation completes, the cluster status is verified
    (rladmin `status extra all`, or the REST /v1 status for the REST deployer),
    including shard/endpoint WATCHDOG status; the next op proceeds only when status
    is OK. The wait is a hard wall-clock budget from the cluster config -
    shard_migration_check_timeout after a shard move / failover and
    endpoint_migration_check_timeout after an endpoint re-bind (both default 30s).
    If still not OK within the budget, the deploy aborts."""
    out = (["", "=" * 86, f"STEP 4 - DEPLOY (via {deployer.label})", "=" * 86]
           if verbose else [""])
    # Endpoint<->master alignment re-binds (endpoint_to_shards) are derived from the
    # actual-vs-planned endpoint diff, not from ctx.moves - so a resource-balanced
    # but endpoint-misaligned cluster still has something to deploy.
    ep_rebinds = endpoint_rebind_commands(
        ctx.cluster, _Live(ctx.cluster, ctx.caps), ctx.planned_state)

    # --force is a planning override. It only BLOCKS execution when the resulting
    # plan actually OVER-COMMITS resources (infeasible). If the cluster has enough
    # RAM/CPU for the plan, it is safe to execute regardless of --force.
    if args.force and not ctx.feas["feasible"]:
        out.append("DEPLOY DISABLED - this --force plan OVER-COMMITS resources; it may place")
        out.append("shards beyond RAM/CPU limits and is NOT safe to execute. Add the recommended")
        out.append("capacity, then re-run. Resource shortfall:")
        for line in ctx.feas.get("lines", []):
            out.append(f"  {line}")
        print("\n".join(out))
        return 0

    # No deployable plan: the cluster lacks resources to balance and --force was not
    # given (Step 3 shows 'NO PLAN'). Do NOT offer execution.
    if not ctx.feas["feasible"] and not args.force:
        out.append("NO PLAN to deploy - the cluster lacks resources to place all shards.")
        out.append("Add capacity (see the shortfall above), then re-run. Nothing was changed.")
        for line in ctx.feas.get("lines", []):
            out.append(f"  {line}")
        print("\n".join(out))
        return 0

    if not ctx.moves and not ep_rebinds:
        out.append("Nothing to deploy: no rebalancing plan was produced (see verdict above).")
        print("\n".join(out))
        return 0

    # Capacity-safe execution order: data ops in the planner-validated order, each
    # DB's endpoint re-bind right after that DB's last data op (see _execution_order).
    ordered = _execution_order(ctx.steps, ep_rebinds)

    # Drop ops the chosen deployer can't do (e.g. endpoint->node over REST) and
    # surface them as manual rladmin follow-ups rather than failing mid-deploy.
    _rl = _RladminDeployer(None)  # type: ignore[arg-type]  # only for describe()
    unsupported = [o for o in ordered if not deployer.supports(o)]
    ordered = [o for o in ordered if deployer.supports(o)]

    # Self-check: the emitted order must not transiently exceed node capacity as it
    # runs. It won't in normal mode (it IS the validated plan order), but guard so a
    # future change can't push a doomed order to the live cluster. --force
    # intentionally over-commits, so this check is skipped there.
    if not args.force:
        transient = execution_order_violations(ctx.cluster, ctx.caps, ordered)
        if transient:
            out.append("DEPLOY DISABLED - the execution order would transiently exceed node")
            out.append("capacity (a migration would be rejected mid-plan). This is a tool")
            out.append("ordering error; please report it. Details:")
            for p in transient:
                out.append(f"  - {p}")
            print("\n".join(out))
            return 1

    n_migrate = sum(1 for o in ordered if o["kind"] == "shard")
    n_failover = sum(1 for o in ordered if o["kind"] == "failover")
    n_eps = sum(1 for o in ordered if o["kind"] in ("ep_node", "ep_policy"))
    n_total = n_migrate + n_failover + n_eps
    total_bytes = sum(o.get("bytes", 0) for o in ordered if o["kind"] == "shard")

    out.append("*** WARNING: this will MODIFY THE LIVE CLUSTER. ***")
    out.append(f"It will execute {n_migrate} shard migration(s) (~{fmt_bytes(total_bytes)}), "
               f"{n_failover} failover(s) and {n_eps} endpoint re-bind(s) via {deployer.label}.")
    out.append("Migrations are online but may cause clients disconnection and transient load.")
    if unsupported:
        out.append(f"NOTE: {len(unsupported)} op(s) are not supported over "
                   f"{deployer.label} and will be SKIPPED; run these via rladmin:")
        out.extend(f"   {_rl.describe(o)}   # {o['label']}" for o in unsupported)
    print("\n".join(out))

    # Approval is ALWAYS required before any change (interactive).
    try:
        ans = input(f"\nType 'yes' to approve and execute these {n_total} "
                    "operation(s) (anything else aborts): ").strip()
    except EOFError:
        ans = ""
    if ans.lower() != "yes":
        print("Aborted by operator. No changes made.")
        return 0

    data_done = ep_done = 0
    is_rest = deployer.label == "REST"
    shard_wait = ctx.cluster.config.shard_check_timeout()       # cluster config, default 30s
    ep_wait = ctx.cluster.config.endpoint_check_timeout()       # cluster config, default 30s
    # REST-only: the API can't observe the proxy/DMC routing reconciliation that lingers
    # after a migration, so hold a minimum settle period (re-verifying) before the next op.
    rest_hold = ctx.cluster.config.rest_post_op_settle() if is_rest else 0.0
    status_src = "rladmin status extra all" if not is_rest else "REST /v1 status + actions"

    def gate(after: str, timeout: float) -> bool:
        """Verify cluster status after an op; wait up to `timeout`s (hard wall-clock)
        for it to return to OK, incl. shard/endpoint watchdog status. On failure,
        print the errors + recovery guidance."""
        hold = f", settle {rest_hold:g}s" if rest_hold else ""
        print(f"    verifying cluster status ({status_src}, up to {timeout:g}s{hold})...")
        # REST reads are instant -> give a just-issued op a moment to start settling
        # before the first poll (rladmin's status dump is already slow enough).
        settle = REST_SETTLE_WAIT if is_rest else 0.0
        ok_health, problems = wait_cluster_ok(deployer.health, timeout=timeout,
                                              interval=3, settle=settle, min_hold=rest_hold)
        if ok_health:
            print("    status OK")
            return True
        print(f"\n*** ABORTING: cluster did not return to OK within {timeout:g}s after {after}. ***")
        print(f"Errors reported by {status_src}:")
        for p in problems:
            print(f"  - {p}")
        print(f"\nCompleted so far: {data_done} data op(s), {ep_done} endpoint "
              f"re-bind(s). Remaining operations were NOT applied.")
        print(f"Fix the cluster status (inspect with `{status_src}`), then re-run "
              "'execute' to complete the remaining rebalancing.")
        return False

    # Execute in the single capacity-safe order (data ops interleave across DBs as the
    # plan validated them; each DB's endpoint re-bind follows its last data op).
    for op in ordered:
        is_ep = op["kind"] in ("ep_node", "ep_policy")
        print(f"  {deployer.describe(op)}   # {op['label']}")
        ok, msg = deployer.run(op)
        if not ok:
            print(f"    FAILED: {msg}")
            print(f"\nStopped after {data_done} data op(s) and {ep_done} endpoint "
                  f"re-bind(s). Remaining operations were NOT applied. Re-run 'plan'.")
            return 1
        print(f"    OK{(' - ' + msg) if msg else ''}")
        if is_ep:
            ep_done += 1
        else:
            data_done += 1
        if not gate(f"{op['kind']} on db:{op['db']}", ep_wait if is_ep else shard_wait):
            return 1

    print(f"\nDeploy complete: {data_done} data op(s) (migrations + failovers), {ep_done} "
          f"endpoint re-bind(s) via {deployer.label} (capacity-safe plan order).")
    if unsupported:
        print(f"\n{len(unsupported)} op(s) were SKIPPED ({deployer.label} can't do them). "
              "Run these via rladmin to finish:")
        for o in unsupported:
            print(f"  {_rl.describe(o)}   # {o['label']}")
    print("\nRe-run 'plan' to verify the new balance score.")
    return 0


def run(args: argparse.Namespace) -> int:
    fmt = args.format
    # 'execute' is interactive; it always uses text output for the approval flow.
    if args.mode == "execute" and fmt != "text":
        sys.stderr.write("Note: 'execute' uses text output for the interactive approval; "
                         "--format ignored.\n")
        fmt = "text"
    # Colour: HTML converts ANSI->spans (force on); JSON has none; text is auto
    # (colour only when stdout is a terminal).
    if fmt == "html":
        set_color(True)
    elif fmt == "json":
        set_color(False)
    else:
        set_color(bool(getattr(sys.stdout, "isatty", lambda: False)()))

    chosen = [name for name, on in (("--status-file", bool(args.status_file)),
                                    ("--rest", bool(args.rest))) if on]
    if len(chosen) > 1:
        sys.stderr.write(f"Use only one input source; got {', '.join(chosen)}.\n")
        return 2

    # rladmin and REST inputs can execute (Step 4). --status-file is PLAN-ONLY
    # (no live cluster); for it, deployer is None and plan_only_reason explains why.
    plan_only_reason = None
    if args.rest:
        rest_client = _rest_client_from_args(args)
        if rest_client is None:
            return 2
        cluster = discover_from_rest(rest_client)
        deployer = _RestDeployer(rest_client)          # shard migrate + endpoint re-bind
    elif args.status_file:
        cluster = discover_from_status_file(args.status_file)
        deployer = None
        plan_only_reason = (
            "--status-file input is analysis-only (no live cluster to act on). To EXECUTE, run "
            "the tool with the live rladmin or --rest input against the cluster.")
    else:
        client = RladminClient(rladmin_path=args.rladmin_path)  # DEFAULT: live rladmin
        cluster = discover(client)
        deployer = _RladminDeployer(client)
    cluster.config = load_config(args.config)
    _merge_exclude_nodes(cluster, getattr(args, "exclude_nodes", None))
    ctx = _Ctx(cluster, force=args.force)

    if fmt == "json":
        print(json.dumps({
            "force": args.force,
            "step1_current": layout_as_dict(ctx.cluster, ctx.cur_views, ctx.s1_score, ctx.cur_reqs),
            "step2_desired": desired_as_dict(
                ctx.cluster, ctx.caps, ctx.current_loads, ctx.cur_score, ctx.state,
                ctx.des_score, ctx.moves, ctx.feas, ctx.blocked_info, ctx.force),
            "step3_plan": plan_as_dict(
                ctx.cluster, ctx.cur_score, ctx.des_score, ctx.steps, ctx.feas,
                ctx.blocked_info, ctx.force, ctx.caps, ctx.planned_state),
            "node_removal_capacity": node_removal_capacity(ctx.cluster, ctx.caps),
        }, indent=2))
        return 0

    # --verbose: the full report (Steps 1-3 + node-removal). Default: concise -
    # just the cluster map (changed DBs only) and the Step-4 commands.
    if args.verbose:
        report = "\n\n".join([
            render_current_layout(ctx.cluster, ctx.cur_views, ctx.s1_score, ctx.cur_reqs),
            render_desired(ctx.cluster, ctx.caps, ctx.current_loads, ctx.cur_score, ctx.state,
                           ctx.des_score, ctx.moves, ctx.feas, ctx.blocked_info, ctx.force),
            render_plan(ctx.cluster, ctx.caps, ctx.current_loads, ctx.cur_score, ctx.state,
                        ctx.des_score, ctx.moves, ctx.steps, ctx.feas, ctx.blocked_info, ctx.force,
                        ctx.raw_move_count, ctx.planned_state,
                        deployer=deployer, for_execute=(args.mode == "execute")),
            render_node_removal(ctx.cluster, ctx.caps),
        ])
    elif not ctx.feas["feasible"] and not args.force:
        # No deployable plan - concise NO PLAN note (mirrors verbose Step 3).
        lines = ["NO PLAN - the cluster lacks resources to balance; add capacity, then re-run.",
                 "  Shortfall:"]
        lines += [f"    {ln}" for ln in ctx.feas["lines"]]
        if ctx.feas.get("recommendation"):
            lines.append(f"  Recommendation: {ctx.feas['recommendation']}")
        report = "\n".join(lines)
    else:
        cur_live = _Live(ctx.cluster, ctx.caps)
        ep_rebinds = endpoint_rebind_commands(ctx.cluster, cur_live, ctx.planned_state)
        if args.force and not ctx.steps and not ep_rebinds:
            # --force but nothing to change: show the CURRENT topology map (per-DB
            # shard + endpoint placement) so the balanced layout can be reviewed.
            report = ("No further rebalancing possible (--force): the current layout "
                      "is shown below for review.\n\n"
                      + render_cluster_map(ctx.cluster, ctx.caps, cur_live,
                                           ctx.planned_state, full_topology=True))
        else:
            report = "\n\n".join([
                render_cluster_map(ctx.cluster, ctx.caps, cur_live, ctx.planned_state,
                                   show_unchanged=False),
                render_commands(ctx.cluster, ctx.caps, ctx.steps, cur_live, ctx.planned_state, deployer),
            ])
    if fmt == "html":
        print(_html_report(ctx.cluster.name, report))
        return 0
    print(report)

    if args.mode == "execute":
        if deployer is not None:          # live rladmin: the only executing input
            return deploy(ctx, deployer, args, verbose=args.verbose)
        # --status-file / --rest: plan is complete; execution is not available here.
        print("\n" + "=" * 86)
        print("STEP 4 - DEPLOY (not available for this input)")
        print("=" * 86)
        print(plan_only_reason)
        return 0
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    return run(build_parser().parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
