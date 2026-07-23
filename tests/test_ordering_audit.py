"""Audit + regression tests for two capacity concerns.

1. valid_move / valid_ep_move enforce RAM + shard-count + HA/rack, but NOT CPU -
   the planner can place a shard or endpoint onto a node past its cores (documented
   design; TestNoCpuWall records it).

2. ORDERING (now fixed): the OLD deploy()/render_commands grouped ops by database,
   which could run one DB's 'fill node X' before another DB's 'free node X' and
   transiently over-commit. balance._execution_order now emits the planner-validated
   order (endpoints after each DB's last data op). TestExecutionOrdering proves the
   old grouping over-committed on a concrete scenario while the new order does not.
"""
import unittest
from collections import defaultdict

import helpers as h

balance = h.balance
GB = 2 ** 30


# --------------------------------------------------------------------------- #
# builders / helpers (all read-only against balance)
# --------------------------------------------------------------------------- #
def mk_cluster(prov_avail_gb, dbspec):
    """prov_avail_gb: {node_uid: GB}. dbspec: {db_uid: (n_masters, per_shard_gb,
    [node per master])}. Non-replicated DBs (all masters, freely movable)."""
    nodes = [balance.Node(uid=u, addr=f"10.0.0.{u}", total_memory=200 * GB, cores=64,
                          status="OK", provisional_ram=int(g * GB), max_shards=100,
                          hostable=True)
             for u, g in prov_avail_gb.items()]
    dbs, shards, eps, suid = [], [], {}, 1
    for d, (ms, ps, placement) in dbspec.items():
        dbs.append(balance.Database(
            uid=d, name=f"db{d}", memory_size=int(ps * GB * ms), shards_count=ms,
            replication=False, sharding=ms > 1, shard_placement="sparse",
            proxy_policy="all-master-shards", db_type="redis", is_flex=False))
        mnodes = set()
        for i, nd in enumerate(placement):
            shards.append(balance.Shard(uid=suid, role="master", bdb_uid=d, node_uid=nd,
                                        used_memory=int(ps * GB), slots=f"{i*100}-{i*100+99}"))
            mnodes.add(nd)
            suid += 1
        eps[d] = mnodes
    cl = balance.Cluster(name="audit")
    cl.nodes, cl.databases, cl.shards = nodes, dbs, shards
    cl.endpoints_by_db, cl.config = eps, balance.Config()
    cl.index()
    return cl


def old_db_grouped_order(ctx):
    """The PRE-FIX ordering: data ops grouped by DB (first-appearance), which is what
    deploy()/render_commands used to emit. Returned as normalised op dicts."""
    data = [s for s in ctx.steps if s["kind"] in ("shard", "failover")]
    groups = defaultdict(list)
    for s in data:
        groups[s["db"]].append(s)
    ordered = []
    for _db, ops in groups.items():
        ordered.extend(ops)
    return [balance._op_from_step(s) for s in ordered]


def rebinds_for(cluster, ctx, caps):
    return balance.endpoint_rebind_commands(
        cluster, balance._Live(cluster, caps), ctx.planned_state)


# The reproducing scenario (round numbers; found by search, RNG-independent here):
#   db1: 2 masters both on node2 (1.4GB each);  db2: 2 masters both on node3 (2.8GB each)
#   node3 ceiling = 5.6 (its db2 shards) + 1.0 avail = 6.6GB
# Balanced plan spreads db1 out (one -> node3) and db2 out (one off node3). The
# validated order frees node3 (db2 leaves) before db1 fills it; the DB-grouped
# order does db1's fill first -> node3 transiently needs 7.0GB > 6.6GB.
def _repro():
    return mk_cluster({1: 1.7, 2: 2.7, 3: 1.0, 4: 0.5},
                      {1: (2, 1.4, [2, 2]), 2: (2, 2.8, [3, 3])})


class TestExecutionOrdering(unittest.TestCase):
    def setUp(self):
        self.cl = _repro()
        self.ctx = balance._Ctx(self.cl, force=False)
        self.caps = balance.node_capacities(self.cl, self.ctx.current_loads)
        self.rebinds = rebinds_for(self.cl, self.ctx, self.caps)
        self.assertTrue(self.ctx.feas["feasible"], "scenario must yield a deployable plan")

    def test_old_db_grouped_order_would_overcommit(self):
        # Regression witness: the pre-fix DB-grouped order transiently over-commits.
        old = old_db_grouped_order(self.ctx)
        viol = balance.execution_order_violations(self.cl, self.caps, old)
        self.assertTrue(viol, "expected the old DB-grouped order to over-commit here")

    def test_new_execution_order_is_capacity_safe(self):
        # The fix: balance._execution_order emits the validated order -> no transient.
        ordered = balance._execution_order(self.ctx.steps, self.rebinds)
        viol = balance.execution_order_violations(self.cl, self.caps, ordered)
        self.assertEqual(viol, [], f"new execution order must be capacity-safe: {viol}")


class TestNoCpuWall(unittest.TestCase):
    """valid_move / valid_ep_move do not enforce CPU; a shard/endpoint can be placed
    onto a node already past its cores (CPU is a soft objective only)."""

    def _saturated_target_cluster(self):
        # node1 has tiny cores and 3 shards -> required vCPU (3) > cores (2), but
        # plenty of RAM headroom. node2 holds a separate movable DB.
        nodes = [balance.Node(uid=1, addr="10.0.0.1", total_memory=200 * GB, cores=2,
                              status="OK", provisional_ram=100 * GB, max_shards=100, hostable=True),
                 balance.Node(uid=2, addr="10.0.0.2", total_memory=200 * GB, cores=64,
                              status="OK", provisional_ram=100 * GB, max_shards=100, hostable=True)]
        shards, eps, suid = [], {}, 1
        dbx = balance.Database(uid=1, name="dbx", memory_size=3 * (GB // 100), shards_count=3,
                               replication=False, sharding=True, shard_placement="sparse",
                               proxy_policy="all-master-shards", db_type="redis", is_flex=False)
        for i in range(3):
            shards.append(balance.Shard(uid=suid, role="master", bdb_uid=1, node_uid=1,
                                        used_memory=GB // 100, slots=f"{i*100}-{i*100+99}"))
            suid += 1
        eps[1] = {1}
        dby = balance.Database(uid=2, name="dby", memory_size=GB, shards_count=1,
                               replication=False, sharding=False, shard_placement="sparse",
                               proxy_policy="all-master-shards", db_type="redis", is_flex=False)
        mover = balance.Shard(uid=suid, role="master", bdb_uid=2, node_uid=2,
                              used_memory=GB, slots="0-16383")
        shards.append(mover)
        eps[2] = {2}
        cl = balance.Cluster(name="cpu")
        cl.nodes, cl.databases, cl.shards = nodes, [dbx, dby], shards
        cl.endpoints_by_db, cl.config = eps, balance.Config()
        cl.index()
        return cl, dby, mover

    def test_valid_move_ignores_cpu(self):
        cl, dby, mover = self._saturated_target_cluster()
        loads = balance.compute_loads(cl, balance.Placement.current(cl), cl.endpoints_by_db)
        caps = balance.node_capacities(cl, loads)
        live = balance._Live(cl, caps)
        eligible = [u for u, c in caps.items() if c.hostable]
        db_caps = balance.build_spread_caps(live, eligible)
        # node1 is already over its cores (required vCPU > 2); moving dby's master there
        # is still allowed - proof there is no CPU capacity wall.
        self.assertGreater(balance._req_vcpu_load(live.loads[1]), caps[1].cores)
        self.assertTrue(live.valid_move(mover, 1, dby, db_caps),
                        "valid_move should permit a move onto a CPU-saturated node")

    def test_valid_ep_move_ignores_cpu_and_ram(self):
        cl, _dby, _mover = self._saturated_target_cluster()
        loads = balance.compute_loads(cl, balance.Placement.current(cl), cl.endpoints_by_db)
        caps = balance.node_capacities(cl, loads)
        live = balance._Live(cl, caps)
        # relocating dby's endpoint onto the CPU-saturated node1 is permitted too
        # (valid_ep_move only checks hostable + not-already-bound).
        self.assertTrue(live.valid_ep_move(cl.db_by_uid[2], src=2, dst=1))


if __name__ == "__main__":
    unittest.main(verbosity=2)
