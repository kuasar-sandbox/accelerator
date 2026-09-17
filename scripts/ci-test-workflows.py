#!/usr/bin/env python3
"""Offline portability contracts; no GitHub access, apt or real native build.

Usage: python3 scripts/ci-test-workflows.py ../kuasar-sandbox
Release bootstrap references must be immutable upstream commits.
"""
import importlib.util
import os
from pathlib import Path
import re
import runpy
import subprocess
import sys
import tempfile
from unittest.mock import patch

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.dont_write_bytecode = True


def check(platform):
    shared = runpy.run_path(str(platform / "ci/hosted/test-workflows.py"))
    expression = shared["expression"]
    shared["check"]()
    subprocess.run([sys.executable, str(platform / "ci/hosted/test-bootstrap.py")], check=True)
    profiles = {
        "release.yml": {"preflight": "release-control", "build": "accelerator", "publish": "release-control", "cleanup": "control"},
        "delete-preview.yml": {"delete": "release-control"},
        "reconcile-latest.yml": {"reconcile": "control"},
    }
    pins = set()
    workflows = {}
    for filename, expected in profiles.items():
        document = yaml.safe_load((ROOT / ".github/workflows" / filename).read_text())
        assert document["permissions"] == {"contents": "read"}
        jobs = document["jobs"]
        workflows[filename] = document
        assert jobs.keys() == expected.keys()
        for name, job in jobs.items():
            permissions = job.get("permissions", document["permissions"])
            assert permissions == ({"actions": "read", "contents": "write"} if name == "publish" else
                                   {"actions": "write", "contents": "read"} if name == "cleanup" else
                                   {"contents": "write"} if name in ("delete", "reconcile") else {"contents": "read"})
            steps = {step["name"]: step for step in job["steps"]}
            names = list(steps)
            assert len(names) == len(job["steps"])
            checkout = steps["Check out pinned hosted bootstrap"]
            bootstrap = steps["Bootstrap standard runner"]
            assert names.index(checkout["name"]) < names.index(bootstrap["name"])
            assert bootstrap["run"] == f"bash trusted/platform/ci/hosted/bootstrap.sh --profile {expected[name]}"
            assert checkout["with"]["repository"] == "kuasar-sandbox/kuasar-sandbox"
            assert checkout["with"]["path"] == "trusted/platform"
            assert checkout["with"]["token"] == "${{ github.token }}"
            pin = checkout["with"]["ref"]
            assert re.fullmatch(r"[0-9a-f]{40}", pin), "bootstrap pin must be a full upstream SHA"
            pins.add(pin)
            for private in (False, True):
                context = {"github": {"event": {"repository": {"private": private}}},
                           "env": {"KUASAR_HOSTED": "false" if private else "true"}}
                pool = "kuasar-e2e" if name == "build" else "kuasar-control"
                assert expression(job["runs-on"], context) == (
                    ["self-hosted", "Linux", "X64", pool] if private else ["ubuntu-24.04"])
                for step in (checkout, bootstrap):
                    assert expression(step["if"], context) == (not private)
                if name == "build":
                    assert expression(job["env"]["KUASAR_HOSTED"], context) == (not private)
                    assert expression(job["timeout-minutes"], context) == (45 if private else 90)
                    assert set(job["env"]) == {"TARGET_ARCH", "KUASAR_HOSTED"}
                    for step_name in ("Check out trusted build checks", "Check hosted CI regression contracts",
                                      "Attach job-local native source cache", "Check the released accelerator ABI"):
                        assert expression(steps[step_name]["if"], context) == (not private)
                for step in job["steps"]:
                    if any(value in step.get("run", "") for value in ("/var/cache", "goproxy.cn", "GOTOOLCHAIN=local")):
                        assert expression(step["if"], context) == private
            for step in job["steps"]:
                assert "${{" not in step.get("shell", "bash"), "shell must be literal"
                assert "actions/cache@" not in step.get("uses", "")
                assert "create-github-app-token" not in step.get("uses", "")
                if "actions/checkout@" in step.get("uses", ""):
                    assert step["with"]["persist-credentials"] is False
                if "Install pinned GitHub CLI" == step["name"]:
                    assert names.index(bootstrap["name"]) < names.index(step["name"])
                    assert step["run"] == "bash scripts/install-gh-cli.sh"
            if name != "build":
                # Root tooling checkout must precede the nested bootstrap checkout,
                # so checkout's cleanup cannot erase trusted/platform.
                root_checkout = next(s for s in job["steps"] if "actions/checkout@" in s.get("uses", ""))
                assert root_checkout is not checkout
                assert names.index(root_checkout["name"]) < names.index(checkout["name"])
    assert len(pins) == 1
    print(f"bootstrap pin: {pins.pop()} (preparation only; publication/qualification are separate)")

    release = workflows["release.yml"]
    event = release.get("on", release.get(True))
    assert set(event["workflow_dispatch"]["inputs"]) == {
        "version", "source_ref", "source_sha", "aggregate_version", "aggregate_sha"}
    jobs = release["jobs"]
    assert jobs["build"]["needs"] == "preflight"
    assert jobs["publish"]["needs"] == ["preflight", "build"]
    assert jobs["cleanup"]["needs"] == "publish"
    for filename in ("release.yml", "delete-preview.yml"):
        assert workflows[filename]["concurrency"] == {
            "group": "component-mutation-${{ github.repository }}-${{ inputs.version }}", "cancel-in-progress": False}
    build = {s["name"]: s for s in jobs["build"]["steps"]}
    names = list(build)
    trusted = build["Check out trusted build checks"]
    assert trusted["with"]["ref"] == "${{ github.workflow_sha }}"
    assert trusted["with"]["path"] == "trusted/accelerator"
    for name in ("Check out exact component source", "Retry exact component source checkout"):
        assert build[name]["with"]["ref"] == "${{ needs.preflight.outputs.source_sha }}"
        assert build[name]["with"]["path"] == "src/accelerator"
        assert names.index("Bootstrap standard runner") < names.index(name)
        assert names.index("Check hosted CI regression contracts") < names.index(name)
    assert build["Retry exact component source checkout"]["if"] == "steps.checkout-source.outcome == 'failure'"
    for name in ("Attach native source cache", "Attach job-local native source cache", "Build and test accelerator", "Package the component release"):
        assert build[name]["working-directory"] == "src/accelerator"
    for name in ("Build and test accelerator", "Package the component release"):
        assert build[name]["shell"] == "bash"
        assert 'if [ "$KUASAR_HOSTED" = true ]; then\n  taskset -pc "$KUASAR_BUILD_CPUS" "$$" >/dev/null\nfi' in build[name]["run"]
    assert 'ln -sfn "$KUASAR_TARBALL_CACHE" build/tarball' in build["Attach job-local native source cache"]["run"]
    assert "NO_ROCKSDB" not in str(jobs["build"])
    assert "umask 022" in build["Build and test accelerator"]["run"]
    assert "continue-on-error" not in build["Build and test accelerator"]
    assert "continue-on-error" not in build["Check the released accelerator ABI"]
    assert build["Check the released accelerator ABI"]["run"] == "python3 trusted/accelerator/scripts/ci-check-abi.py src/accelerator/bin/x86_64"
    assert names.index("Build and test accelerator") < names.index("Check the released accelerator ABI") < names.index("Package the component release")
    package = build["Package the component release"]["run"]
    assert '[ "$(git rev-parse HEAD)" = "${{ needs.preflight.outputs.source_sha }}" ]' in package
    assert 'SOURCE_DATE_EPOCH=$(git show -s --format=%ct "${{ needs.preflight.outputs.source_sha }}")' in package
    assert 'bash scripts/release.sh package "$VERSION" "$TARGET_ARCH" release-bundle' in package
    upload = build["Upload validated release bundle"]["with"]
    assert upload["path"] == "src/accelerator/release-bundle"
    assert upload["retention-days"] == 1 and upload["if-no-files-found"] == "error"
    publish = {s["name"]: s for s in jobs["publish"]["steps"]}
    assert publish["Download validated release bundle"]["with"]["name"] == upload["name"]
    assert publish["Download validated release bundle"]["with"]["path"] == "release-bundle"
    for job, name in ((jobs["preflight"], "Validate release request"), (jobs["publish"], "Publish component release")):
        step = next(s for s in job["steps"] if s["name"] == name)
        assert 'bash scripts/validate-preview-line.sh' in step["run"]
        assert step["env"]["AGGREGATE_SHA"] == "${{ inputs.aggregate_sha }}"
        assert step["env"]["AGGREGATE_VERSION"] == "${{ inputs.aggregate_version }}"
        assert step["env"]["GH_TOKEN"] == "${{ github.token }}"
        for variable in ("SOURCE_REF", "SOURCE_SHA", "VERSION"):
            binding = f"inputs.{variable.lower()}" if name == "Validate release request" else f"needs.preflight.outputs.{variable.lower()}"
            assert step["env"][variable] == "${{ " + binding + " }}"
    delete = workflows["delete-preview.yml"]
    delete_event = delete.get("on", delete.get(True))["workflow_dispatch"]["inputs"]
    assert set(delete_event) == {"version", "source_sha", "mode"}
    assert delete_event["mode"]["options"] == ["incomplete", "gc"]
    deletion = delete["jobs"]["delete"]["steps"][-1]
    assert deletion["run"] == 'bash scripts/delete-preview.sh accelerator "$VERSION" "$SOURCE_SHA" "$MODE"'
    reconcile = workflows["reconcile-latest.yml"]
    assert set(reconcile.get("on", reconcile.get(True))) == {"workflow_run", "schedule", "workflow_dispatch"}
    assert reconcile["jobs"]["reconcile"]["steps"][-1]["run"] == "bash scripts/publish-release.sh reconcile"
    cleanup = jobs["cleanup"]["steps"][-1]["run"]
    assert 'actions/runs/$GITHUB_RUN_ID/artifacts?per_page=100' in cleanup
    assert 'repos/$GITHUB_REPOSITORY/actions/artifacts/$artifact_id' in cleanup
    pr = yaml.safe_load((ROOT / ".github/workflows/integration-tests.yml").read_text())
    assert pr["jobs"]["ci"]["uses"] == "kuasar-sandbox/kuasar-sandbox/.github/workflows/ci-entry.yml@main"
    check_build_shell(build)
    check_source_workspace()
    print("accelerator workflows: visibility, trust, paths, permissions and release contracts PASS")


def check_build_shell(steps):
    """Execute the actual YAML build shell with a fake make and real taskset."""
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        source = root / "src/accelerator"
        source.mkdir(parents=True)
        tools = root / "tools"
        tools.mkdir()
        make = tools / "make"
        make.write_text(f"#!{sys.executable}\nimport os, sys\n"
                        "with open(os.environ['MAKE_LOG'], 'a') as log:\n"
                        "    print(sys.argv[1:], os.getcwd(), sorted(os.sched_getaffinity(0)), file=log)\n")
        make.chmod(0o755)
        cpus = sorted(os.sched_getaffinity(0))
        for hosted in (False, True):
            log = root / f"make-{hosted}.log"
            env = dict(os.environ, PATH=f"{tools}:{os.environ['PATH']}", MAKE_LOG=str(log),
                       KUASAR_HOSTED=str(hosted).lower(), KUASAR_BUILD_CPUS=str(cpus[0]))
            subprocess.run(["bash", "-e", "-o", "pipefail", "-c", steps["Build and test accelerator"]["run"]],
                           cwd=source, env=env, check=True)
            expected = [f"{[goal]} {source} {[cpus[0]] if hosted else cpus}" for goal in ("test", "vet", "build", "test-release")]
            assert log.read_text().splitlines() == expected
        cache = root / "hosted/tarballs"
        cache.mkdir(parents=True)
        subprocess.run(["bash", "-e", "-c", steps["Attach job-local native source cache"]["run"]],
                       cwd=source, env=dict(os.environ, KUASAR_TARBALL_CACHE=str(cache)), check=True)
        assert (source / "build/tarball").resolve() == cache


def check_source_workspace():
    """Exercise the actual release source resolver in the new sibling layout."""
    with tempfile.TemporaryDirectory() as directory:
        workspace = Path(directory)
        source = workspace / "src/accelerator"
        subprocess.run(["git", "clone", "--quiet", "--no-hardlinks", str(ROOT), str(source)], check=True)
        trusted = workspace / "trusted/platform"
        trusted.mkdir(parents=True)
        (trusted / "bootstrap-fixture").write_text("untracked trusted tooling outside source\n")
        sha = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
        script = ('set -euo pipefail\n'
                  'fail() { echo "$*" >&2; exit 1; }\n'
                  'source "$1/scripts/release-materials.sh"\n'
                  'release_materials_resolve_git_source "$2" "$3" accelerator\n')
        command = ["bash", "-c", script, "source-workspace", str(ROOT), str(source), sha]
        result = subprocess.run(command, text=True, capture_output=True)
        assert result.returncode == 0 and result.stdout.strip() == sha, result
        (source / "untracked-input").write_text("must fail cleanliness checks\n")
        result = subprocess.run(command, text=True, capture_output=True)
        assert result.returncode != 0 and "source worktree is dirty" in result.stderr, result


def check_abi():
    spec = importlib.util.spec_from_file_location("accelerator_abi", ROOT / "scripts/ci-check-abi.py")
    abi = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(abi)
    dynamic = "INTERP\n(NEEDED) Shared library: [libc.so.6]\nName: GLIBC_2.38"
    for output, static, valid in (
        ("There is no dynamic section in this file.", True, True),
        (dynamic, False, True),
        (dynamic.replace("2.38", "2.2.5"), False, True),
        (dynamic, True, False),
        ("(NEEDED) Shared library: [libc.so.6]", True, False),
        ("INTERP", True, False),
        ("There is no dynamic section in this file.", False, False),
        (dynamic.replace("2.38", "2.39"), False, False),
        (dynamic + "\nName: GLIBC_2.38.1", False, False),
        (dynamic + "\nName: GLIBC_PRIVATE", False, False),
        *((dynamic + f"\n(NEEDED) Shared library: [{library}]", False, False)
          for library in ("librocksdb.so.9", "libstdc++.so.6", "libgcc_s.so.1")),
    ):
        with patch.object(abi.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, output)):
            try:
                abi.validate(Path("fixture"), static=static)
                accepted = True
            except ValueError:
                accepted = False
        assert accepted == valid, (output, static)
    with patch.object(abi.subprocess, "run", side_effect=subprocess.CalledProcessError(1, "readelf")):
        try:
            abi.validate(Path("unreadable"))
        except subprocess.CalledProcessError:
            pass
        else:
            raise AssertionError("readelf failure must fail closed")
    print("accelerator ABI fixtures: static Go, normal CGO, static native libraries and glibc 2.38 ceiling PASS")


if __name__ == "__main__":
    check(Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else ROOT.parent / "kuasar-sandbox")
    check_abi()
