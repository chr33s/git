"""Run inside a fresh Linux runtime container with no Node or source checkout.

Arguments: extracted distribution, disposable fixture directory, new report, source archive.
Only the fixture directory is mutated. Requires Git, SSH, and Python stdlib.
"""
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


require(len(sys.argv) == 5, "expected distribution, fixture, report, and archive paths")
install, fixture, report, archive = map(Path, sys.argv[1:])
install, fixture = install.resolve(), fixture.resolve()
require(platform.system() == "Linux", "requires Linux runtime container")
require(shutil.which("node") is None and shutil.which("npm") is None, "Node/npm must be absent")
require(not Path("/workspace").exists(), "build checkout must not be present")
require(not (install / "node_modules").exists(), "dependencies must not be installed")
require(not (install / "src").exists(), "source must not be installed")
require((install / "git-remote-git+id").is_symlink(), "helper must be installed")
require(os.readlink(install / "git-remote-git+id") == "git+", "helper target changed")
for line in (install / "CONTENTS.sha256").read_text().splitlines():
    digest, name = line.split("  ", 1)
    require(Path(name).name == name, "invalid checksum path")
    with (install / name).open("rb") as content:
        require(hashlib.file_digest(content, "sha256").hexdigest() == digest,
                "installed checksum mismatch")

identity = json.loads((fixture / "fixture.json").read_text())
env = dict(os.environ, PATH=str(install) + ":/usr/bin:/bin",
           HOME=str(fixture), XDG_CONFIG_HOME=str(fixture / "config"),
           GIT_CONFIG_GLOBAL="/dev/null", GIT_CONFIG_NOSYSTEM="1", GIT_TERMINAL_PROMPT="0",
           GIT_AUTHOR_NAME="Installation rehearsal", GIT_AUTHOR_EMAIL="test@example.invalid",
           GIT_COMMITTER_NAME="Installation rehearsal", GIT_COMMITTER_EMAIL="test@example.invalid",
           GIT_CONFIG_COUNT="2", GIT_CONFIG_KEY_0="credential.helper", GIT_CONFIG_VALUE_0="",
           GIT_CONFIG_KEY_1="credential.helper",
           GIT_CONFIG_VALUE_1='!f() { echo username=git; echo "password=$GIT_AUTH_TOKEN"; }; f',
           GIT_AUTH_TOKEN=identity["token"], GIT_HTTP_METRICS_INTERVAL_MS="0")

release = json.loads((install / "RELEASE.json").read_text())
require(release["schemaVersion"] == 1, "invalid release manifest")
require(release["kind"] in ("rehearsal", "release-candidate"), "invalid release kind")
with archive.open("rb") as content:
    archive_digest = hashlib.file_digest(content, "sha256").hexdigest()


def run(*args, cwd=fixture):
    result = subprocess.run(args, cwd=cwd, env=env, capture_output=True, timeout=30)
    require(result.returncode == 0, "installed command failed: " + args[0])
    return result.stdout.decode().strip()


def http(route):
    try:
        with urllib.request.urlopen("http://127.0.0.1:18080" + route, timeout=2) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()


build = json.loads(run(str(install / "git+"), "--build-info"))
require(build == json.loads((install / "build-info.json").read_text()), "build identity mismatch")
for field in ("version", "revision", "dirty"):
    require(release[field] == build[field], "release manifest differs from installed build")
if release["kind"] == "release-candidate":
    require(build["dirty"] is False, "release candidate cannot contain a dirty build")
run("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(fixture / "ssh-key"))
with (fixture / "server.log").open("wb") as log:
    server = subprocess.Popen([str(install / "git+"), "serve", "--root", str(fixture / "data"),
                               "--hostname", "127.0.0.1", "--port", "18080", "--ui"],
                              cwd=install, env=env, stdout=log, stderr=log)
    try:
        deadline = time.monotonic() + 15
        while True:
            require(server.poll() is None, "installed server exited during startup")
            try:
                status, body = http("/hub/")
                if status == 200:
                    break
            except (urllib.error.URLError, TimeoutError):
                pass
            require(time.monotonic() < deadline, "installed server startup timed out")
            time.sleep(0.05)
        require(b"<html" in body.lower(), "embedded UI document missing")
        for asset in ["/hub/bootstrap.js", "/hub/main.js", "/hub/main.css"]:
            status, body = http(asset)
            require(status == 200 and len(body) > 100, "embedded UI asset missing")
        require(http("/project/refs")[0] == 401, "anonymous private read accepted")
        run("git", "clone", "--quiet", identity["remote"], "first")
        first, second = fixture / "first", fixture / "second"
        run("git", "checkout", "-B", "main", cwd=first)
        (first / "content.txt").write_text("one\n")
        run("git", "add", ".", cwd=first)
        run("git", "commit", "-qm", "one", cwd=first)
        run("git", "push", "-q", "origin", "main", cwd=first)
        run("git", "clone", "-q", identity["remote"], "second")
        require((second / "content.txt").read_text() == "one\n", "clone content differs")
        (second / "content.txt").write_text("two\n")
        run("git", "commit", "-qam", "two", cwd=second)
        run("git", "push", "-q", "origin", "main", cwd=second)
        run("git", "fetch", "-q", "origin", cwd=first)
        require(run("git", "show", "origin/main:content.txt", cwd=first) == "two", "fetch differs")
        require(run("git", "rev-parse", "origin/main", cwd=first) ==
                run("git", "rev-parse", "HEAD", cwd=second), "remote refs differ")
        run("git", "fsck", "--strict", cwd=first)
        require("SHA256:" in (fixture / "config/chr33s-git/known_repos").read_text(),
                "repository identity was not pinned")
        with report.open("x") as output:
            json.dump(dict(schemaVersion=1, measuredAt=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                           platform=platform.platform(), arch=platform.machine(), build=build,
                           runtimeImage=os.environ.get("REHEARSAL_IMAGE"),
                           archiveSha256=archive_digest, release=release,
                           executableSha256=(install / "SHA256SUMS").read_text().split()[0],
                           git=run("git", "--version"), nodeAbsent=True, sourceCheckoutAbsent=True,
                           installedChecksumsVerified=True, embeddedUiServed=True,
                           privateAnonymousReadDenied=True, identityClonePushFetchVerified=True,
                           repositoryIdentityPinned=True, nativeGitFsckPassed=True,
                           limitsOfEvidence="Isolated Debian runtime only; not publisher authentication, platform signing, full OS qualification or browser rendering."),
                      output, indent=2)
            output.write("\n")
    finally:
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()
print("Installed standalone verification passed.")
