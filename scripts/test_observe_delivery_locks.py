import importlib.util
import io
import json
from contextlib import redirect_stderr
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("lock_observer", Path(__file__).with_name("observe-delivery-locks.py"))
observer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(observer)


class ObserverSafetyTest(unittest.TestCase):
    def run_fixture(self, directory, contents, command):
        log = Path(directory) / "experiment.log"
        output = Path(directory) / "observations.json"
        log.write_text(contents)
        argv = ["observer", "--log", str(log), "--output", str(output)]
        with patch.object(sys, "argv", argv), patch.object(observer, "command", command):
            code = observer.main()
        return code, json.loads(output.read_text())

    def test_non_testcontainer_is_rejected_before_any_mysql_command(self):
        container = "a" * 64
        calls = []
        def command(args):
            calls.append(args)
            return subprocess.CompletedProcess(args, 0, "mysql:8.4.8|false\n", "private diagnostic text")
        with tempfile.TemporaryDirectory() as directory:
            code, report = self.run_fixture(directory,
                f"Container mysql:8.4.8 is starting: {container}\ntc.mysql:8.4.8 -- Container is started\n", command)
        self.assertEqual(code, 1)
        self.assertEqual(report["status"], "OBSERVER_ERROR")
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][1], "inspect")
        self.assertNotIn(container, json.dumps(report))
        self.assertNotIn("private diagnostic text", json.dumps(report))

    def test_command_timeout_exports_only_type_and_no_arguments_or_stderr(self):
        def command(args):
            raise subprocess.TimeoutExpired(["private-command-argument"], 8,
                                            output="private-output", stderr="private-error")
        with tempfile.TemporaryDirectory() as directory:
            code, report = self.run_fixture(directory,
                "Container mysql:8.4.8 is starting: " + "b" * 64 + "\ntc.mysql:8.4.8 -- Container is started\n", command)
        self.assertEqual(code, 1)
        self.assertEqual(report["errorType"], "TimeoutExpired")
        self.assertNotIn("private-", json.dumps(report))

    def test_existing_observations_are_never_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "experiment.log"
            output = Path(directory) / "observations.json"
            output.write_text("preserved original")
            with patch.object(sys, "argv", ["observer", "--log", str(log), "--output", str(output)]), \
                    patch.object(observer, "command") as command, redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    observer.main()
                command.assert_not_called()
            self.assertEqual(output.read_text(), "preserved original")

    def test_finished_experiment_stops_without_touching_any_container(self):
        def command(args):
            raise AssertionError("must not execute")
        with tempfile.TemporaryDirectory() as directory:
            code, report = self.run_fixture(directory, "BUILD SUCCESSFUL\n", command)
        self.assertEqual(code, 0)
        self.assertEqual(report["status"], "EXPERIMENT_ENDED")
        self.assertEqual(report["samples"], [])

    def test_unexpected_query_fields_are_rejected_instead_of_exported(self):
        fields = "\n".join(key + "\t0" for key in observer.KEYS)
        result = subprocess.CompletedProcess([], 0, fields + "\nprivate_row_identifier\tsecret\n", "")
        with patch.object(observer, "command", return_value=result):
            with self.assertRaises(ValueError):
                observer.observe("fake")


if __name__ == "__main__":
    unittest.main()
