#!/usr/bin/env python3
"""Single-command test runner for the balance.py suite (stdlib unittest only).

    python tests/run.py            # run everything
    python tests/run.py -v         # verbose
    python tests/run.py test_rest  # run one module

Regenerate fixtures / golden snapshots:
    python tests/fixtures/_gen.py
    BALANCE_UPDATE_GOLDEN=1 python tests/run.py test_golden
"""
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def main() -> int:
    sys.path.insert(0, HERE)  # so `import helpers` and `test_*` resolve
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    verbosity = 2 if ("-v" in sys.argv or "--verbose" in sys.argv) else 1
    loader = unittest.TestLoader()
    if args:
        suite = unittest.TestSuite()
        for name in args:
            suite.addTests(loader.loadTestsFromName(name))
    else:
        suite = loader.discover(HERE, pattern="test_*.py", top_level_dir=HERE)
    result = unittest.TextTestRunner(verbosity=verbosity).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
