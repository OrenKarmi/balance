"""White-box unit tests for the planner internals: compaction correctness (the
failover + pass-through fix), the greedy convergence guarantee, scoring, and the
valid_move hard constraints.
"""
import unittest

import helpers as h

balance = h.balance


def _live_and_caps(name):
    cluster = balance.discover_from_status_file(h.fixture_path(name))
    cluster.config = balance.Config()
    caps = balance.node_capacities(
        cluster, balance.compute_loads(cluster, balance.Placement.current(cluster),
                                       cluster.endpoints_by_db))
    state = balance._Live(cluster, caps)
    eligible = [u for u, c in caps.items() if c.hostable]
    db_caps = balance.build_spread_caps(state, eligible)
    return cluster, caps, state, db_caps


class TestCompaction(unittest.TestCase):
    def test_drops_reversing_failovers_and_passthrough(self):
        cluster, caps, live, db_caps = _live_and_caps("mem_imbalanced")
        moves = []
        db = next(d for d in balance.movable_databases(cluster) if d.replication)

        # (a) redundant failover pair on one HA group -> net no-op
        grp = None
        for s in cluster.shards_by_bdb[db.uid]:
            p = live.group_pair(db, s)
            if p:
                grp = p
                break
        self.assertIsNotNone(grp, "need a replicated HA group")
        m, sl = grp
        for a, b_ in ((m, sl), (sl, m)):  # fail over, then fail back
            mn, sn = live.place[a], live.place[b_]
            live.do_failover(db, a, b_)
            moves.append({"kind": "failover", "db": db.uid, "db_name": db.name,
                          "master_shard": a, "slave_shard": b_, "role": "failover",
                          "shard": a, "src": mn, "dst": sn, "bytes": 0})

        # (b) reversing shard move: route a slave A -> B -> A (nets to no move).
        sx = next(s for s in cluster.shards_by_bdb[db.uid] if live.role[s.uid] == "slave")
        a = live.place[sx.uid]
        b_target = next((u for u in caps
                         if u != a and live.valid_move(sx, u, db, db_caps, ignore_ram=True)),
                        None)
        if b_target is None:
            self.skipTest("no valid target to build a reversing move")
        for dst in (b_target, a):   # move away, then back
            src = live.place[sx.uid]
            live.do_move(sx, dst, db)
            moves.append({"kind": "shard", "shard": sx.uid, "db": db.uid,
                          "db_name": db.name, "role": "slave", "src": src,
                          "dst": dst, "bytes": db.per_shard_memory})

        comp = balance.compact_plan(cluster, caps, live, moves, db_caps, force=True)
        self.assertIsNotNone(comp, "compaction should have fired")
        self.assertLess(len(comp), len(moves), "compaction must be strictly smaller")
        self.assertEqual(sum(1 for x in comp if x["kind"] == "failover"), 0,
                         "reversing failover pair must be dropped")

        # replaying the compacted plan is valid and score-equivalent
        steps, _start, planned = balance.build_plan(cluster, caps, comp, db_caps, force=True)
        self.assertTrue(all(st["valid"] for st in steps))
        self.assertAlmostEqual(
            balance.score_from_loads(planned.loads, caps).overall,
            balance.score_from_loads(live.loads, caps).overall, places=5)

    def test_compaction_never_worsens(self):
        # For every plannable fixture, compaction (inside _Ctx) yields a plan no
        # larger than the raw greedy plan, and never introduces invalid steps.
        for name in h.fixtures():
            with self.subTest(fixture=name):
                _cluster, ctx = h.build_ctx(name, force=True)
                self.assertLessEqual(len(ctx.moves), ctx.raw_move_count)
                self.assertTrue(all(st["valid"] for st in ctx.steps))


class TestScoring(unittest.TestCase):
    def test_balanced_scores_higher_than_imbalanced(self):
        _c1, ctx_bal = h.build_ctx("balanced", force=False)
        _c2, ctx_imb = h.build_ctx("mem_imbalanced", force=False)
        self.assertGreater(ctx_bal.cur_score.overall, ctx_imb.cur_score.overall)
        self.assertTrue(ctx_bal.cur_score.is_balanced)

    def test_score_bounds(self):
        for name in h.fixtures():
            with self.subTest(fixture=name):
                _c, ctx = h.build_ctx(name, force=False)
                for sc in (ctx.cur_score.overall, ctx.des_score.overall):
                    self.assertGreaterEqual(sc, 0.0)
                    self.assertLessEqual(sc, 100.0 + 1e-6)


class TestValidMove(unittest.TestCase):
    def test_replicated_master_cannot_migrate(self):
        cluster, caps, live, db_caps = _live_and_caps("mem_imbalanced")
        db = next(d for d in balance.movable_databases(cluster) if d.replication)
        master = next(s for s in cluster.shards_by_bdb[db.uid]
                      if live.role[s.uid] == "master")
        other = next(u for u in caps if u != live.place[master.uid])
        self.assertFalse(live.valid_move(master, other, db, db_caps, ignore_ram=True),
                         "a replicated master's process must never migrate")

    def test_anti_affinity_blocks_colocation(self):
        cluster, caps, live, db_caps = _live_and_caps("mem_imbalanced")
        db = next(d for d in balance.movable_databases(cluster) if d.replication)
        # find a slave and the node hosting its group's master
        for s in cluster.shards_by_bdb[db.uid]:
            if live.role[s.uid] != "slave":
                continue
            pair = live.group_pair(db, s)
            if not pair:
                continue
            master_uid, _ = pair
            master_node = live.place[master_uid]
            if live.place[s.uid] != master_node:
                self.assertFalse(
                    live.valid_move(s, master_node, db, db_caps, ignore_ram=True),
                    "a replica must not co-locate with its master")
                return
        self.skipTest("no suitable slave/master pair found")

    def test_rack_awareness_blocks_same_rack(self):
        cluster, caps, live, db_caps = _live_and_caps("rack_aware")
        self.assertTrue(cluster.rack_aware)
        for s in cluster.shards_by_bdb[cluster.databases[0].uid]:
            if live.role[s.uid] != "slave":
                continue
            pair = live.group_pair(cluster.databases[0], s)
            if not pair:
                continue
            master_node = live.place[pair[0]]
            master_rack = caps[master_node].rack_id
            same_rack = [u for u, c in caps.items()
                         if c.rack_id == master_rack and u != master_node
                         and u != live.place[s.uid]]
            if same_rack:
                self.assertFalse(
                    live.valid_move(s, same_rack[0], cluster.databases[0], db_caps,
                                    ignore_ram=True),
                    "rack-awareness must block a replica onto its master's rack")
                return
        self.skipTest("no same-rack target available to test")


if __name__ == "__main__":
    unittest.main(verbosity=2)
