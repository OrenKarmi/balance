"""Black-box CLI tests: run the real `balance.py` as a subprocess and assert on
exit codes, output formats, mode behaviour, determinism, and config handling.
"""
import json
import os
import unittest

import helpers as h


class TestPlanModes(unittest.TestCase):
    def test_json_parses_and_has_sections(self):
        for name in h.fixtures():
            with self.subTest(fixture=name):
                d = h.plan_json(name)
                for key in ("step1_current", "step2_desired", "step3_plan"):
                    self.assertIn(key, d)
                # every emitted step must be valid
                for st in d["step3_plan"]["steps"]:
                    self.assertTrue(st["valid"], f"{name}: invalid step {st['step']}")

    def test_classification_matches_expectation(self):
        # Count actual rladmin commands from the text plan: endpoint-only plans emit
        # an `endpoint_to_shards` command with zero numbered steps, so step_count
        # alone understates them.
        for name in h.fixtures():
            with self.subTest(fixture=name):
                d = h.plan_json(name)
                _rc, out, _err = h.run_cli("plan", "--status-file", h.fixture_path(name))
                cmds = h.rladmin_commands(out)
                if name in h.NEEDS_FORCE:
                    self.assertFalse(d["step2_desired"]["feasible"],
                                     f"{name}: expected infeasible without --force")
                    self.assertFalse(d["step3_plan"]["deployable"])
                elif name in h.NOOP:
                    self.assertEqual(cmds, [], f"{name}: expected no commands")
                else:
                    self.assertGreaterEqual(len(cmds), 1,
                                            f"{name}: expected a deployable plan")
                    self.assertTrue(d["step3_plan"]["deployable"])

    def test_text_and_no_commands_for_noop(self):
        for name in h.NOOP:
            with self.subTest(fixture=name):
                rc, out, err = h.run_cli("plan", "--status-file", h.fixture_path(name))
                self.assertEqual(rc, 0, err)
                self.assertEqual(h.rladmin_commands(out), [],
                                 f"{name}: expected no rladmin commands")

    def test_needs_force_recovers_with_force(self):
        for name in h.NEEDS_FORCE:
            with self.subTest(fixture=name):
                d = h.plan_json(name, "--force")
                self.assertTrue(d["force"])
                self.assertTrue(d["step2_desired"]["forced"],
                                f"{name}: --force should set forced=True")

    def test_verbose_has_all_steps(self):
        rc, out, err = h.run_cli("plan", "--status-file",
                                 h.fixture_path("mem_imbalanced"), "--verbose")
        self.assertEqual(rc, 0, err)
        for marker in ("STEP 1", "STEP 2", "STEP 3"):
            self.assertIn(marker, out)

    def test_html_format(self):
        rc, out, err = h.run_cli("plan", "--status-file",
                                 h.fixture_path("mem_imbalanced"), "--format", "html")
        self.assertEqual(rc, 0, err)
        self.assertIn("<html", out.lower())
        self.assertIn(".r{", out)   # colour CSS present

    def test_determinism(self):
        for name in ("mem_imbalanced", "cpu_imbalanced", "force_overcommit"):
            with self.subTest(fixture=name):
                extra = ("--force",) if name in h.NEEDS_FORCE else ()
                _rc1, out1, _ = h.run_cli("plan", "--status-file",
                                          h.fixture_path(name), *extra)
                _rc2, out2, _ = h.run_cli("plan", "--status-file",
                                          h.fixture_path(name), *extra)
                self.assertEqual(out1, out2, f"{name}: non-deterministic output")


class TestExecuteRefusal(unittest.TestCase):
    def test_execute_on_status_file_is_analysis_only(self):
        rc, out, err = h.run_cli("execute", "--status-file",
                                 h.fixture_path("mem_imbalanced"))
        combined = (out + err).lower()
        self.assertIn("analysis-only", combined,
                      "execute on a status file should refuse to act")


class TestConfigExclusion(unittest.TestCase):
    CFG = os.path.join(h.FIX_DIR, "excluded_db.config.json")

    def _moved_dbs(self, *extra):
        d = h.plan_json("excluded_db", *extra)
        return {m["db"] for m in d["step2_desired"]["moves"]}

    def test_excluded_db_not_moved(self):
        self.assertTrue(os.path.exists(self.CFG))
        with_cfg = self._moved_dbs("--config", self.CFG)
        self.assertNotIn(2, with_cfg, "db2 is excluded but was moved")

    def test_without_config_db2_would_move(self):
        # Sanity: the exclusion is what protects db2 (it moves otherwise).
        without = self._moved_dbs()
        self.assertIn(2, without,
                      "expected db2 to move without the exclusion config")


if __name__ == "__main__":
    unittest.main(verbosity=2)
