"""Policy-invariant and regression-guard tests.

For every fixture, in both normal and --force modes, the planned layout must
uphold the tool's hard guarantees (anti-affinity, rack-awareness, shard limits,
no replicated-master migration, endpoint alignment, out-of-scope untouched) and
must never contain reversing/pass-through churn. These are scenario-agnostic, so
they catch regressions even in inputs we did not specifically design for.
"""
import unittest

import helpers as h


class TestInvariants(unittest.TestCase):
    pass


def _make_invariant_test(name, force):
    def test(self):
        cluster, ctx = h.build_ctx(name, force=force)
        violations = h.check_invariants(name, cluster, ctx)
        self.assertEqual(violations, [], "\n  ".join([f"{name} (force={force}):"] + violations))
    return test


def _make_convergence_test(name, force):
    def test(self):
        cluster, _ctx = h.build_ctx(name, force=force)
        n_shards = len(cluster.shards)
        raw = h.raw_greedy_move_count(cluster, force=force)
        # A converged search emits few moves; a thrashing one approaches max_iter
        # (5000). Generous bound that still catches the oscillation regression.
        bound = 4 * n_shards + 20
        self.assertLessEqual(
            raw, bound,
            f"{name} (force={force}): greedy emitted {raw} moves for {n_shards} "
            f"shards (> {bound}) - search is not converging")
    return test


def _make_score_test(name):
    def test(self):
        _cluster, ctx = h.build_ctx(name, force=False)
        if name in h.SCORE_MAY_DROP:
            self.skipTest("alignment/dense-consolidation may lower resource score")
        self.assertGreaterEqual(
            ctx.des_score.overall, ctx.cur_score.overall - 1e-6,
            f"{name}: desired score {ctx.des_score.overall:.2f} < current "
            f"{ctx.cur_score.overall:.2f}")
    return test


# Materialise one test method per (fixture, mode) so failures name the scenario.
for _name in h.fixtures():
    for _force in (False, True):
        suffix = f"{_name}_{'force' if _force else 'plan'}"
        setattr(TestInvariants, f"test_invariants_{suffix}",
                _make_invariant_test(_name, _force))
        setattr(TestInvariants, f"test_convergence_{suffix}",
                _make_convergence_test(_name, _force))
    setattr(TestInvariants, f"test_score_{_name}", _make_score_test(_name))


class TestOscillationRegression(unittest.TestCase):
    """Direct guard for the greedy-oscillation bug, on the real-world capture that
    originally provoked the two-state limit cycle (~5000 reversing moves)."""

    def test_capture_converges(self):
        cluster = h.balance.discover_from_status_file(h.fixture_path("oscillation"))
        cluster.config = h.balance.Config()
        raw = h.raw_greedy_move_count(cluster, force=True)
        self.assertLess(raw, 200,
                        f"greedy emitted {raw} moves under --force - oscillation regressed")


if __name__ == "__main__":
    unittest.main(verbosity=2)
