"""Unit tests for the REST health gate: rest_health classification and the
wait_cluster_ok settle/hold polling loop. Uses fake clients and a fake clock so
the tests are deterministic and never sleep.
"""
import unittest
from unittest import mock

import helpers as h

balance = h.balance


class FakeRestClient:
    """Stands in for balance.RestClient: .get(path, timeout) returns canned JSON.
    A path mapped to the RAISE sentinel raises SystemExit (older RE / transient)."""
    RAISE = object()

    def __init__(self, payloads):
        self.payloads = payloads

    def get(self, path, timeout=None):
        val = self.payloads.get(path, [])
        if val is self.RAISE:
            raise SystemExit(f"GET {path} failed")
        return val


def _healthy_payloads():
    return {
        "/v1/nodes": [{"uid": 1, "status": "active"}, {"uid": 2, "status": "active"}],
        "/v1/bdbs": [{"uid": 1, "name": "db1", "status": "active",
                      "shards_count": 1, "replication": True}],
        "/v1/shards": [{"uid": 1, "bdb_uid": 1, "status": "active", "detailed_status": "ok"},
                       {"uid": 2, "bdb_uid": 1, "status": "active", "detailed_status": "ok"}],
        "/v1/actions": [],
    }


class TestRestHealth(unittest.TestCase):
    def test_healthy_cluster_ok(self):
        ok, problems = balance.rest_health(FakeRestClient(_healthy_payloads()))
        self.assertTrue(ok, problems)
        self.assertEqual(problems, [])

    def test_node_not_active_flagged(self):
        p = _healthy_payloads()
        p["/v1/nodes"][1]["status"] = "down"
        ok, problems = balance.rest_health(FakeRestClient(p))
        self.assertFalse(ok)
        self.assertTrue(any("node:2" in x for x in problems), problems)

    def test_shard_instance_mismatch_flagged(self):
        # replicated 1-shard DB expects 2 instances; supply only 1 -> not settled.
        p = _healthy_payloads()
        p["/v1/shards"] = [{"uid": 1, "bdb_uid": 1, "status": "active", "detailed_status": "ok"}]
        ok, problems = balance.rest_health(FakeRestClient(p))
        self.assertFalse(ok)
        self.assertTrue(any("instance" in x for x in problems), problems)

    def test_in_progress_action_flagged(self):
        p = _healthy_payloads()
        p["/v1/actions"] = [{"name": "migrate_shard", "status": "running",
                             "object_name": "db:1", "progress": 42}]
        ok, problems = balance.rest_health(FakeRestClient(p))
        self.assertFalse(ok)
        self.assertTrue(any("operation" in x for x in problems), problems)

    def test_bad_shard_detailed_status_flagged(self):
        p = _healthy_payloads()
        p["/v1/shards"][0]["detailed_status"] = "importing"
        ok, problems = balance.rest_health(FakeRestClient(p))
        self.assertFalse(ok)

    def test_missing_actions_endpoint_tolerated(self):
        # An older RE without /v1/actions must not fail the whole check.
        p = _healthy_payloads()
        p["/v1/actions"] = FakeRestClient.RAISE
        ok, problems = balance.rest_health(FakeRestClient(p))
        self.assertTrue(ok, problems)


class FakeClock:
    """Monotonic clock that advances a fixed step on each read; sleep() jumps it."""
    def __init__(self, step=1.0):
        self.t = 0.0
        self.step = step

    def monotonic(self):
        self.t += self.step
        return self.t

    def sleep(self, secs):
        self.t += secs


class TestWaitClusterOk(unittest.TestCase):
    def _run(self, health, **kw):
        clock = FakeClock()
        with mock.patch.object(balance.time, "monotonic", clock.monotonic), \
             mock.patch.object(balance.time, "sleep", clock.sleep):
            return balance.wait_cluster_ok(health, **kw)

    def test_returns_ok_when_healthy(self):
        ok, problems = self._run(lambda _t: (True, []), timeout=30, interval=1)
        self.assertTrue(ok)
        self.assertEqual(problems, [])

    def test_recovers_after_transient_problems(self):
        calls = {"n": 0}

        def health(_remaining):
            calls["n"] += 1
            return (calls["n"] >= 3, [] if calls["n"] >= 3 else ["settling"])

        ok, _problems = self._run(health, timeout=60, interval=1)
        self.assertTrue(ok)
        self.assertGreaterEqual(calls["n"], 3)

    def test_times_out_when_never_healthy(self):
        ok, problems = self._run(lambda _t: (False, ["stuck"]), timeout=20, interval=1)
        self.assertFalse(ok)
        self.assertEqual(problems, ["stuck"])

    def test_min_hold_reverifies(self):
        calls = {"n": 0}

        def health(_remaining):
            calls["n"] += 1
            return (True, [])

        ok, _ = self._run(health, timeout=120, interval=1, min_hold=10)
        self.assertTrue(ok)
        self.assertGreater(calls["n"], 1, "min_hold should re-verify health more than once")


if __name__ == "__main__":
    unittest.main(verbosity=2)
