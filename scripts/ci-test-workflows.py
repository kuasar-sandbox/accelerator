#!/usr/bin/env python3
"""Offline portability contracts; no GitHub access, apt or real native build.

Usage: python3 scripts/ci-test-workflows.py ../kuasar-sandbox
Release builds use native Workbench images selected once for the run.
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
    shared["check"]()
    subprocess.run([sys.executable, str(platform / "ci/hosted/test-bootstrap.py")], check=True)
    workflows = {}
    for filename in ("release.yml", "delete-preview.yml", "reconcile-latest.yml"):
        document = yaml.safe_load((ROOT / ".github/workflows" / filename).read_text())
        workflows[filename] = document
        assert document["permissions"] == {"contents": "read"}
        for name, job in document["jobs"].items():
            assert job["runs-on"] == ("${{ matrix.runner }}" if filename == "release.yml" and name == "build"
                                      else "ubuntu-latest")
            for visibility in ("private", "internal", ""):
                context = {"github": {"repository": "kuasar-sandbox/accelerator", "event": {"repository": {
                    "visibility": visibility, "full_name": "kuasar-sandbox/accelerator"}}}}
                assert shared["expression"](job["if"], context) is False
            assert "github.event.repository.full_name == github.repository" in job["if"]
            permissions = job.get("permissions", document["permissions"])
            assert permissions == ({"actions": "read", "contents": "write"} if name == "publish" else
                                   {"actions": "write", "contents": "read"} if name == "cleanup" else
                                   {"contents": "write"} if name in ("delete", "reconcile") else {"contents": "read"})
            for step in job["steps"]:
                assert "create-github-app-token" not in step.get("uses", "")
                assert "actions/cache@" not in step.get("uses", "")
                if "actions/checkout@" in step.get("uses", ""):
                    assert step["with"]["persist-credentials"] is False
                command = step.get("run", step.get("with", {}).get("run"))
                if command:
                    subprocess.run(["bash", "-n"], input=command, text=True, check=True)

    release = workflows["release.yml"]
    jobs = release["jobs"]
    assert jobs["build"]["needs"] == "preflight"
    assert jobs["build"]["strategy"] == {"fail-fast": False, "matrix": {"include": [
        {"arch": "x86_64", "runner": "ubuntu-24.04"}, {"arch": "aarch64", "runner": "ubuntu-24.04-arm"}]}}
    preflight = {s["name"]: s for s in jobs["preflight"]["steps"]}
    assert preflight["Select immutable native Workbench images"]["env"] == {"GH_TOKEN": "${{ github.token }}"}
    select = preflight["Select immutable native Workbench images"]["run"]
    assert '--framework-sha "${{ steps.framework.outputs.sha }}" --output workbench.json' in select
    assert str(release).count("workbench.py select") == 1
    selection = preflight["Upload Workbench selection"]["with"]["name"]
    assert jobs["build"]["env"]["TARGET_ARCH"] == "${{ matrix.arch }}"
    assert jobs["publish"]["needs"] == ["preflight", "build"]
    assert jobs["cleanup"]["needs"] == ["preflight", "publish"]
    for filename in ("release.yml", "delete-preview.yml"):
        assert workflows[filename]["concurrency"] == {
            "group": "component-mutation-${{ github.repository }}-${{ inputs.version }}", "cancel-in-progress": False}
    build = {s["name"]: s for s in jobs["build"]["steps"]}
    names = list(build)
    assert build["Check out trusted build checks"]["with"]["ref"] == "${{ github.workflow_sha }}"
    assert build["Check out trusted platform tooling"]["with"]["ref"] == "${{ needs.preflight.outputs.framework_sha }}"
    for name in ("Check out exact component source", "Retry exact component source checkout"):
        assert build[name]["with"]["ref"] == "${{ needs.preflight.outputs.source_sha }}"
        assert build[name]["with"]["path"] == "src/accelerator"
    assert "Bootstrap native or cross build" not in build
    assert "Attach native source cache" not in build
    assert build["Download Workbench selection"]["with"]["name"] == selection
    step = build["Build, test and package accelerator"]
    assert sum(s.get("uses") == "./trusted/platform/.github/actions/workbench" for s in build.values()) == 1
    assert step["uses"] == "./trusted/platform/.github/actions/workbench"
    assert step["with"]["selection"] == "workbench-selection/workbench.json"
    assert step["with"]["sources"] == "src"
    assert step["with"]["arch"] == "${{ matrix.arch }}"
    assert "env" not in step and "GH_TOKEN" not in step["with"]["run"]
    assert "restore-or-build rocksdb" in step["with"]["run"]
    assert step["with"]["cache-coverage"] == "release-accelerator-build"
    assert step["with"]["outputs"].splitlines() == ["accelerator/bin", "accelerator/release-bundle"]
    assert "NO_ROCKSDB" not in str(jobs["build"])
    for name in ("Build, test and package accelerator", "Check the released accelerator ABI", "Check CI regression contracts"):
        assert "continue-on-error" not in build[name]
    assert "umask 022" in step["with"]["run"]
    assert "bin/$TARGET_ARCH" in build["Check the released accelerator ABI"]["run"]
    assert names.index("Build, test and package accelerator") < names.index("Check the released accelerator ABI") < names.index("Upload validated release bundle")
    package = step["with"]["run"]
    assert '[ "$(git rev-parse HEAD)" = "${{ needs.preflight.outputs.source_sha }}" ]' in package
    assert 'bash scripts/release.sh package "$VERSION" "$TARGET_ARCH" release-bundle' in package
    assert 'bash scripts/release.sh validate "$VERSION" "$TARGET_ARCH" release-bundle' in package
    upload = build["Upload validated release bundle"]["with"]
    assert "matrix.arch" in upload["name"] and upload["path"] == "src/accelerator/release-bundle"
    assert upload["retention-days"] == 1 and upload["if-no-files-found"] == "error"
    publish = {s["name"]: s for s in jobs["publish"]["steps"]}
    assert publish["Download Workbench selection"]["with"]["name"] == selection
    helper = publish["Compile trusted archive validator"]
    assert helper["uses"] == "./trusted/platform/.github/actions/workbench"
    assert helper["with"]["sources"] == "publisher-tools"
    assert helper["with"]["cache"] == "false"
    assert helper["with"]["outputs"] == "accelerator/release-archive-validator"
    assert helper["with"]["arch"] == "x86_64" and "env" not in helper
    assert 'go build -p "$KUASAR_BUILD_JOBS" -trimpath -o release-archive-validator release-archive-validator.go' in helper["with"]["run"]
    assert "cp scripts/release-archive-validator.go publisher-tools/accelerator/" in publish["Prepare trusted archive validator source"]["run"]
    for name in ("Assemble the two validated architecture archives", "Publish component release"):
        assert publish[name]["env"]["RELEASE_ARCHIVE_VALIDATOR"] == "${{ github.workspace }}/publisher-tools/accelerator/release-archive-validator"
    for arch in ("x86_64", "aarch64"):
        assert publish[f"Download validated {arch} bundle"]["with"]["name"] == upload["name"].replace("${{ matrix.arch }}", arch)
    assert 'publish-release.sh assemble' in publish["Assemble the two validated architecture archives"]["run"]
    assert publish["Publish component release"]["env"]["TARGET_ARCH"] == "all"
    for job, name in ((jobs["preflight"], "Validate release request"), (jobs["publish"], "Publish component release")):
        step = next(s for s in job["steps"] if s["name"] == name)
        assert 'bash scripts/validate-preview-line.sh' in step["run"]
        for variable in ("SOURCE_REF", "SOURCE_SHA", "VERSION"):
            binding = f"inputs.{variable.lower()}" if name == "Validate release request" else f"needs.preflight.outputs.{variable.lower()}"
            assert step["env"][variable] == "${{ " + binding + " }}"
    pr = yaml.safe_load((ROOT / ".github/workflows/integration-tests.yml").read_text())
    assert pr["jobs"]["ci"]["uses"] == "kuasar-sandbox/kuasar-sandbox/.github/workflows/ci-entry.yml@main"
    check_build_shell(build)
    check_source_workspace()
    print("accelerator workflows: public caller, isolated architectures, trust, ABI and original-byte publication PASS")


def check_build_shell(steps):
    """Execute the real command with private cache material and failing stages."""
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        source = root / "src/accelerator"
        source.mkdir(parents=True)
        tools = root / "tools"
        tools.mkdir()
        make = tools / "make"
        make.write_text(f"#!{sys.executable}\nimport os, sys\n"
                        "with open(os.environ['MAKE_LOG'], 'a') as log:\n"
                        "    print(sys.argv[1:], os.getcwd(), file=log)\n"
                        "sys.exit(72 if sys.argv[1] == os.environ.get('FAIL_GOAL') else 0)\n")
        make.chmod(0o755)
        git = tools / "git"
        git.write_text('#!/bin/sh\ncase "$*" in\n'
                       '"rev-parse HEAD") printf "%s\\n" "$GIT_SHA" ;;\n'
                       '"show -s --format=%ct "*) echo 12345 ;;\n*) exit 90 ;;\nesac\n')
        git.chmod(0o755)
        (source / "scripts").mkdir()
        (source / "scripts/release.sh").write_text(
            'set -eu\n[ "$SOURCE_DATE_EPOCH" = 12345 ]\n'
            '[ "$(cat "$CACHE_LOG.state")" = "$TARGET_ARCH" ]\n'
            'printf "%s\\n" "$1" >> "$RELEASE_LOG"\n'
            'if [ "$1" = "${FAIL_RELEASE:-}" ]; then\n'
            '  case "$1" in package) exit 74 ;; validate) exit 75 ;; esac\nfi\n')
        framework = root / "framework"
        recipe = framework / "ci/native-cache/native-cache.sh"
        recipe.parent.mkdir(parents=True)
        recipe.write_text('set -eu\n[ "$*" = "restore-or-build rocksdb" ]\n'
                          'printf "%s\\n" "$TARGET_ARCH" > "$CACHE_LOG"\n'
                          '[ "${CACHE_EXIT:-0}" = 0 ] || exit "$CACHE_EXIT"\n'
                          'printf "%s\\n" "$TARGET_ARCH" > "$CACHE_LOG.state"\n')
        command = steps["Build, test and package accelerator"]["with"]["run"].replace("/inputs/release", str(framework))
        command = command.replace("${{ needs.preflight.outputs.version }}", "v0.1.6-preview.20261009")
        command = command.replace("${{ needs.preflight.outputs.source_sha }}", "a" * 40)
        for arch in ("x86_64", "aarch64"):
            log = root / f"make-{arch}.log"
            cache_log = root / f"cache-{arch}.log"
            release_log = root / f"release-{arch}.log"
            env = dict(os.environ, PATH=f"{tools}:{os.environ['PATH']}", MAKE_LOG=str(log),
                       TARGET_ARCH=arch, CACHE_LOG=str(cache_log), RELEASE_LOG=str(release_log), GIT_SHA="a" * 40)
            subprocess.run(["bash", "-e", "-o", "pipefail", "-c", command],
                           cwd=source.parent, env=env, check=True)
            goals = ("test", "vet", "build", "test-release") if arch == "x86_64" else ("build",)
            assert log.read_text().splitlines() == [f"{[goal]} {source}" for goal in goals]
            assert cache_log.read_text().strip() == arch
            assert release_log.read_text().splitlines() == ["package", "validate"]
            log.unlink()
            release_log.unlink()
            failed = subprocess.run(["bash", "-e", "-c", command], cwd=source.parent,
                                    env=dict(env, CACHE_EXIT="71"))
            assert failed.returncode == 71 and not log.exists() and not release_log.exists()
            failed = subprocess.run(["bash", "-e", "-c", command], cwd=source.parent,
                                    env=dict(env, FAIL_GOAL=goals[0]))
            assert failed.returncode == 72
            assert log.read_text().splitlines() == [f"{[goals[0]]} {source}"]
            assert not release_log.exists()
            for stage, code, expected in (("package", 74, ["package"]),
                                          ("validate", 75, ["package", "validate"])):
                log.unlink()
                failed = subprocess.run(["bash", "-e", "-c", command], cwd=source.parent,
                                        env=dict(env, FAIL_RELEASE=stage))
                assert failed.returncode == code
                assert log.read_text().splitlines() == [f"{[goal]} {source}" for goal in goals]
                assert release_log.read_text().splitlines() == expected
                release_log.unlink()
            failed = subprocess.run(["bash", "-e", "-c", command], cwd=source.parent,
                                    env=dict(env, GIT_SHA="b" * 40))
            assert failed.returncode == 1 and not release_log.exists()


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
