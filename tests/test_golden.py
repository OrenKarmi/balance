"""Golden/snapshot tests: the full `--format json` output for a few representative
fixtures is captured and compared byte-for-byte, so any unintended change in
planning behaviour shows up as a diff.

The planner is deterministic, so snapshots are stable. When you change behaviour
ON PURPOSE, refresh the snapshots:

    BALANCE_UPDATE_GOLDEN=1 python -m unittest tests.test_golden

Review the resulting diff before committing.
"""
import json
import os
import unittest

import helpers as h

GOLDEN_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "golden")
UPDATE = os.environ.get("BALANCE_UPDATE_GOLDEN") == "1"

# (fixture, extra CLI flags) -> snapshot file name
CASES = [
    ("mem_imbalanced", ()),
    ("cpu_imbalanced", ()),
    ("endpoint_misaligned", ()),
    ("dense_placement", ()),
    ("rack_aware", ()),
    ("force_overcommit", ("--force",)),
    ("non_replicated", ()),
    ("all_nodes_policy", ()),
]


def _canonical(name, extra):
    return json.dumps(h.plan_json(name, *extra), sort_keys=True, indent=2) + "\n"


class TestGolden(unittest.TestCase):
    pass


def _make(name, extra):
    def test(self):
        os.makedirs(GOLDEN_DIR, exist_ok=True)
        tag = name + ("__force" if "--force" in extra else "")
        path = os.path.join(GOLDEN_DIR, tag + ".json")
        actual = _canonical(name, extra)
        if UPDATE:
            with open(path, "w", encoding="utf-8", newline="\n") as fh:
                fh.write(actual)
            self.skipTest(f"updated golden {tag}")
            return
        self.assertTrue(os.path.exists(path),
                        f"missing golden {path}; run with BALANCE_UPDATE_GOLDEN=1")
        with open(path, encoding="utf-8") as fh:
            expected = fh.read()
        self.assertEqual(
            actual, expected,
            f"{tag}: output drifted from golden. If intended, refresh with "
            "BALANCE_UPDATE_GOLDEN=1 and review the diff.")
    return test


for _name, _extra in CASES:
    _tag = _name + ("_force" if "--force" in _extra else "")
    setattr(TestGolden, f"test_golden_{_tag}", _make(_name, _extra))


if __name__ == "__main__":
    unittest.main(verbosity=2)
