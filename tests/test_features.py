"""Tests for the configurable REST port and configurable node exclusion features."""
import argparse
import os
import unittest
from unittest import mock

import helpers as h

balance = h.balance


# --------------------------------------------------------------------------- #
# configurable REST port
# --------------------------------------------------------------------------- #
def _rest_args(**kw):
    d = dict(rest_fqdn="host.example", rest_user="admin",
             rest_password="pw", rest_port=None)
    d.update(kw)
    return argparse.Namespace(**d)


class TestRestPort(unittest.TestCase):
    def test_client_default_port(self):
        c = balance.RestClient("h", "u", "p")
        self.assertTrue(c.base.endswith(":9443"), c.base)

    def test_client_custom_port(self):
        c = balance.RestClient("h", "u", "p", port=9444)
        self.assertTrue(c.base.endswith(":9444"), c.base)

    def test_from_args_default(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("RL_REST_PORT", None)
            c = balance._rest_client_from_args(_rest_args())
        self.assertIsNotNone(c)
        self.assertTrue(c.base.endswith(":9443"), c.base)

    def test_from_args_flag(self):
        c = balance._rest_client_from_args(_rest_args(rest_port=9444))
        self.assertTrue(c.base.endswith(":9444"), c.base)

    def test_from_args_env(self):
        with mock.patch.dict(os.environ, {"RL_REST_PORT": "9555"}):
            c = balance._rest_client_from_args(_rest_args(rest_port=None))
        self.assertTrue(c.base.endswith(":9555"), c.base)

    def test_flag_overrides_env(self):
        with mock.patch.dict(os.environ, {"RL_REST_PORT": "9555"}):
            c = balance._rest_client_from_args(_rest_args(rest_port=9444))
        self.assertTrue(c.base.endswith(":9444"), c.base)

    def test_bad_env_port_rejected(self):
        with mock.patch.dict(os.environ, {"RL_REST_PORT": "not-a-port"}):
            c = balance._rest_client_from_args(_rest_args(rest_port=None))
        self.assertIsNone(c, "a non-integer port should be rejected")

    def test_cli_flag_parsed(self):
        # --rest-port must reach argparse (type=int). Reject a non-int cleanly.
        rc, _out, err = h.run_cli("plan", "--rest", "--rest-fqdn", "h", "--rest-user",
                                  "u", "--rest-password", "p", "--rest-port", "abc")
        self.assertNotEqual(rc, 0)
        self.assertIn("rest-port", (err).lower())


# --------------------------------------------------------------------------- #
# configurable node exclusion
# --------------------------------------------------------------------------- #
def _ctx_excl(name, exclude, force=False):
    cluster = balance.discover_from_status_file(h.fixture_path(name))
    cluster.config = balance.Config({"cluster": {"exclude_nodes": list(exclude)}})
    return cluster, balance._Ctx(cluster, force=force)


class TestNodeExclusion(unittest.TestCase):
    def test_excluded_node_is_non_hostable(self):
        _cluster, ctx = _ctx_excl("cpu_imbalanced", [1])
        self.assertFalse(ctx.caps[1].hostable)

    def test_excluded_node_never_sourced_or_targeted(self):
        # Across every fixture, excluding a node must leave it completely untouched:
        # no step sources/targets it, and none of its shards move off.
        for name in h.fixtures():
            for excl in (1, 2):
                for force in (False, True):
                    with self.subTest(fixture=name, excl=excl, force=force):
                        cluster, ctx = _ctx_excl(name, [excl], force=force)
                        for st in ctx.steps:
                            self.assertNotEqual(st.get("src"), excl,
                                                f"{name}: step {st['step']} sources node {excl}")
                            self.assertNotEqual(st.get("dst"), excl,
                                                f"{name}: step {st['step']} targets node {excl}")
                        for s in cluster.shards:
                            if s.node_uid == excl:
                                self.assertEqual(
                                    ctx.planned_state.place[s.uid], excl,
                                    f"{name}: shard {s.uid} moved off excluded node {excl}")

    def test_invariants_hold_with_exclusion(self):
        for name in ("mem_imbalanced", "cpu_imbalanced", "rack_aware"):
            with self.subTest(fixture=name):
                cluster, ctx = _ctx_excl(name, [1])
                self.assertEqual(h.check_invariants(name, cluster, ctx), [])

    def test_cli_exclude_nodes_flag(self):
        rc, out, err = h.run_cli("plan", "--status-file",
                                 h.fixture_path("cpu_imbalanced"),
                                 "--exclude-nodes", "1", "--format", "json")
        self.assertEqual(rc, 0, err)
        import json
        d = json.loads(out)
        for st in d["step3_plan"]["steps"]:
            self.assertNotEqual(st["from_node"], 1)
            self.assertNotEqual(st["to_node"], 1)
        node1 = next(n for n in d["step1_current"]["nodes"] if n["uid"] == 1)
        self.assertFalse(node1["hostable"], "excluded node should render as non-hostable")

    def test_cli_unknown_uid_warns(self):
        rc, _out, err = h.run_cli("plan", "--status-file",
                                  h.fixture_path("cpu_imbalanced"), "--exclude-nodes", "999")
        self.assertEqual(rc, 0)
        self.assertIn("unknown node uid", err.lower())

    def test_config_and_cli_merge(self):
        cluster = balance.discover_from_status_file(h.fixture_path("cpu_imbalanced"))
        cluster.config = balance.Config({"cluster": {"exclude_nodes": [2]}})
        balance._merge_exclude_nodes(cluster, "1,3")
        self.assertEqual(cluster.config.excluded_nodes(), {1, 2, 3})

    def test_exclusion_changes_the_plan(self):
        # Sanity: excluding the hot node yields a different plan than not excluding it.
        _c1, ctx_plain = _ctx_excl("cpu_imbalanced", [])
        _c2, ctx_excl = _ctx_excl("cpu_imbalanced", [1])
        plain = {(s["kind"], s.get("src"), s.get("dst")) for s in ctx_plain.steps}
        excl = {(s["kind"], s.get("src"), s.get("dst")) for s in ctx_excl.steps}
        self.assertNotEqual(plain, excl)


if __name__ == "__main__":
    unittest.main(verbosity=2)
