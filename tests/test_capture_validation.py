"""discover_from_status_file must REJECT an incomplete capture (missing CLUSTER
NODES / DATABASES / ENDPOINTS / SHARDS section) with a clear SystemExit, rather
than silently degrade or crash. A complete capture is accepted.
"""
import os
import re
import tempfile
import unittest

import helpers as h

balance = h.balance


def _write_tmp(text):
    fd, path = tempfile.mkstemp(suffix=".status")
    os.close(fd)
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(text)
    return path


class TestCaptureValidation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # A known-complete capture to mutate. Use a committed fixture (repo-root
        # status.txt is git-ignored, so it is absent in a fresh CI checkout).
        with open(h.fixture_path("mem_imbalanced"), encoding="utf-8") as fh:
            cls.full = fh.read()

    def _reject(self, label, text):
        path = _write_tmp(text)
        try:
            with self.assertRaises(SystemExit, msg=f"{label} should be rejected"):
                balance.discover_from_status_file(path)
        finally:
            os.remove(path)

    def test_full_capture_accepted(self):
        path = _write_tmp(self.full)
        try:
            cl = balance.discover_from_status_file(path)
            self.assertTrue(cl.nodes and cl.databases and cl.shards)
        finally:
            os.remove(path)

    def test_missing_cluster_nodes_rejected(self):
        self._reject("no CLUSTER NODES", self.full.replace("CLUSTER NODES:", "CLUSTER XXXXX:"))

    def test_missing_databases_rejected(self):
        self._reject("no DATABASES",
                     re.sub(r"\nDATABASES:.*?(?=\nENDPOINTS:)", "\n", self.full, flags=re.S))

    def test_missing_endpoints_rejected(self):
        self._reject("no ENDPOINTS",
                     re.sub(r"\nENDPOINTS:.*?(?=\nSHARDS:)", "\n", self.full, flags=re.S))

    def test_missing_shards_rejected(self):
        self._reject("no SHARDS", re.sub(r"\nSHARDS:.*$", "\n", self.full, flags=re.S))

    def test_present_but_empty_shards_rejected(self):
        # SHARDS header present but no rows, while DATABASES has rows -> inconsistent.
        gutted = re.sub(r"(\nSHARDS:\n).*$", r"\1", self.full, flags=re.S)
        self._reject("empty SHARDS with databases", gutted)


if __name__ == "__main__":
    unittest.main(verbosity=2)
