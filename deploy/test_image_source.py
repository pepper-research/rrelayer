"""Exercise the production workflow's actual pre-checkout and identity code."""
import os
from pathlib import Path
import re
import tempfile
import textwrap
import unittest
from unittest.mock import patch


WORKFLOW = Path(__file__).resolve().parents[1] / ".github/workflows/deploy-prod.yml"
DEFAULT_SHA = "a" * 40
SELECTED_SHA = "b" * 40


def step_script(name):
    content = WORKFLOW.read_text()
    step = content.split("      - name: " + name + "\n", 1)[1]
    step = step.split("      - name: ", 1)[0]
    body = textwrap.dedent(step.split("        run: |\n", 1)[1])
    return body.split("python3 - <<'PYTHON'\n", 1)[1].split("\nPYTHON", 1)[0]


class ImageSourceTests(unittest.TestCase):
    def run_step(self, name, values, git_result=None):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            environment = {**values, "GITHUB_OUTPUT": str(output)}
            with patch.dict(os.environ, environment, clear=True):
                with patch("subprocess.check_output", return_value=git_result) as git:
                    try:
                        exec(compile(step_script(name), str(WORKFLOW), "exec"), {})
                    except SystemExit:
                        self.assertFalse(output.exists(), "Rejected source emitted an identity")
                        raise
            return output.read_text(), git

    def test_empty_manual_and_push_default_to_workflow_sha(self):
        for values in ({"WORKFLOW_SOURCE_REVISION": DEFAULT_SHA},
                       {"WORKFLOW_SOURCE_REVISION": DEFAULT_SHA, "REQUESTED_SOURCE_REVISION": ""}):
            with self.subTest(values=values):
                output, git = self.run_step("Resolve image source", values)
                self.assertEqual(output, f"sha={DEFAULT_SHA}\n")
                git.assert_not_called()

    def test_explicit_full_sha_overrides_workflow_ref(self):
        output, git = self.run_step("Resolve image source", {
            "WORKFLOW_SOURCE_REVISION": DEFAULT_SHA,
            "REQUESTED_SOURCE_REVISION": SELECTED_SHA,
        })
        self.assertEqual(output, f"sha={SELECTED_SHA}\n")
        git.assert_not_called()

    def test_malformed_and_shell_input_rejected_before_git(self):
        for value in ("main", "a" * 39, "a" * 41, "A" * 40, " " + SELECTED_SHA,
                      SELECTED_SHA + "\n", "$(touch unwanted)",
                      SELECTED_SHA + "; echo injected", "\nsha=" + SELECTED_SHA):
            with self.subTest(value=value):
                with self.assertRaises(SystemExit):
                    self.run_step("Resolve image source", {
                        "WORKFLOW_SOURCE_REVISION": DEFAULT_SHA,
                        "REQUESTED_SOURCE_REVISION": value,
                    })

    def test_invalid_default_rejected(self):
        with self.assertRaises(SystemExit):
            self.run_step("Resolve image source", {"WORKFLOW_SOURCE_REVISION": "main"})

    def test_actual_matching_checkout_identity(self):
        output, git = self.run_step("Verify checked out source", {
            "EXPECTED_SOURCE_REVISION": SELECTED_SHA,
        }, SELECTED_SHA + "\n")
        self.assertEqual(output, f"sha={SELECTED_SHA}\n")
        git.assert_called_once_with(["git", "rev-parse", "HEAD"], text=True)

    def test_mismatched_or_malformed_checkout_rejected(self):
        for actual in (DEFAULT_SHA, "b" * 39, "B" * 40, SELECTED_SHA + "\nextra"):
            with self.subTest(actual=actual):
                with self.assertRaises(SystemExit):
                    self.run_step("Verify checked out source", {
                        "EXPECTED_SOURCE_REVISION": SELECTED_SHA,
                    }, actual)

    def test_workflow_consumers_use_actual_checked_out_identity(self):
        content = WORKFLOW.read_text()
        actual = "${{ steps.checked-source.outputs.sha }}"
        requested = "${{ steps.requested-source.outputs.sha }}"
        self.assertLess(content.index("- name: Resolve image source"), content.index("- name: Checkout"))
        self.assertLess(content.index("- name: Verify checked out source"), content.index("- name: Configure AWS credentials"))
        self.assertIn("ref: " + requested, content)
        for consumer in ("SOURCE_REVISION=" + actual, "RRELAYER_RELEASE_ID=" + actual,
                         "SOURCE_SHA: " + actual, "name: rrelayer-candidate-" + actual,
                         "}/${{ env.ECR_REPOSITORY }}:" + actual + "-${{ github.run_id }}"):
            self.assertIn(consumer, content)
        self.assertEqual(content.count("${{ github.sha }}"), 1)
        self.assertIn("'accepted':False", content)
        self.assertIn("platforms: linux/arm64", content)
        self.assertIn("RRELAYER_CONFIG=deploy/rrelayer-prod.yaml", content)
        self.assertNotRegex(content, r"register-task-definition|update-service|run-task|deploy-task-definition")


if __name__ == "__main__":
    unittest.main()
