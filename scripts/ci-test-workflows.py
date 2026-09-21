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
    shared["check"]()
    subprocess.run([sys.executable, str(platform / "ci/hosted/test-bootstrap.py")], check=True)
    workflows = {}
    for filename in ("release.yml", "delete-preview.yml", "reconcile-latest.yml"):
        document = yaml.safe_load((ROOT / ".github/workflows" / filename).read_text())
        workflows[filename] = document
        assert document["permissions"] == {"contents": "read"}
        for name, job in document["jobs"].items():
            assert job["runs-on"] == "ubuntu-latest"
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
                if "run" in step:
                    subprocess.run(["bash", "-n"], input=step["run"], text=True, check=True)

    release = workflows["release.yml"]
    jobs = release["jobs"]
    assert jobs["build"]["needs"] == "preflight"
    assert jobs["build"]["strategy"] == {"fail-fast": False, "matrix": {"arch": ["x86_64", "aarch64"]}}
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
    assert "artifact-cross" in build["Bootstrap native or cross build"]["run"]
    assert "NO_ROCKSDB" not in str(jobs["build"])
    for name in ("Build and test accelerator", "Check the released accelerator ABI", "Check CI regression contracts"):
        assert "continue-on-error" not in build[name]
    assert "umask 022" in build["Build and test accelerator"]["run"]
    assert "bin/$TARGET_ARCH" in build["Check the released accelerator ABI"]["run"]
    assert names.index("Build and test accelerator") < names.index("Check the released accelerator ABI") < names.index("Package the component release")
    package = build["Package the component release"]["run"]
    assert '[ "$(git rev-parse HEAD)" = "${{ needs.preflight.outputs.source_sha }}" ]' in package
    assert 'bash scripts/release.sh package "$VERSION" "$TARGET_ARCH" release-bundle' in package
    upload = build["Upload validated release bundle"]["with"]
    assert "matrix.arch" in upload["name"] and upload["path"] == "src/accelerator/release-bundle"
    assert upload["retention-days"] == 1 and upload["if-no-files-found"] == "error"
    publish = {s["name"]: s for s in jobs["publish"]["steps"]}
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
    """Execute both actual YAML build paths with a fake make and real taskset."""
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
        cpu = sorted(os.sched_getaffinity(0))[0]
        for arch in ("x86_64", "aarch64"):
            log = root / f"make-{arch}.log"
            env = dict(os.environ, PATH=f"{tools}:{os.environ['PATH']}", MAKE_LOG=str(log),
                       TARGET_ARCH=arch, KUASAR_BUILD_CPUS=str(cpu))
            subprocess.run(["bash", "-e", "-o", "pipefail", "-c", steps["Build and test accelerator"]["run"]],
                           cwd=source, env=env, check=True)
            goals = ("test", "vet", "build", "test-release") if arch == "x86_64" else ("build",)
            assert log.read_text().splitlines() == [f"{[goal]} {source} {[cpu]}" for goal in goals]
        cache = root / "hosted/tarballs"
        cache.mkdir(parents=True)
        subprocess.run(["bash", "-e", "-c", steps["Attach native source cache"]["run"]],
                       cwd=root, env=dict(os.environ, KUASAR_TARBALL_CACHE=str(cache)), check=True)
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
