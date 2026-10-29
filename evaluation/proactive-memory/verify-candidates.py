#!/usr/bin/env python3
"""Run fixed historical suites against proposed edits, without executing model-chosen commands."""
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tarfile
import time

run_directory = pathlib.Path(sys.argv[1]).resolve()
repository = pathlib.Path(sys.argv[2]).resolve()
verification = pathlib.Path(sys.argv[3]).resolve()
verification.mkdir(mode=0o700, parents=True, exist_ok=True)
plan = json.loads((run_directory / 'plan.json').read_text())
commands = {
    'H01-color-environment': ['src/cli/GitCompat.inspection.interop.test.ts', 'src/cli/GitCompat.maintenance.interop.test.ts'],
    'H02-unknown-session': ['src/cli/session.test.ts'],
    'H03-editor-upgrade': ['src/ui/snapshot.interop.test.ts'],
    'H04-upload-reclaim': ['src/client/Upload.browser.interop.test.ts'],
    'H05-portable-gzip': ['src/server/Protocol.test.ts'],
}
# The browser is an existing pinned test dependency. Credentials and user configuration
# are absent from the candidate process environment.
browser_executable = pathlib.Path(subprocess.check_output(['node', '--input-type=module', '-e', "import {chromium} from 'playwright'; process.stdout.write(chromium.executablePath());"], cwd=repository, text=True).strip())
browser_path = str(next(parent.parent for parent in browser_executable.parents if parent.name.startswith('chromium-')))

for slot in plan['slots']:
    stem = run_directory / slot['id'][7:]
    retained = pathlib.Path(str(stem) + '.verification.json')
    if retained.exists():
        print('retained', slot['caseId'], slot['condition'], flush=True)
        continue
    input_path = pathlib.Path(str(stem) + '.input.json')
    answer_path = pathlib.Path(str(stem) + '.answer.txt')
    if not answer_path.exists():
        raise RuntimeError('model output not retained; reconcile before verification')
    packet = json.loads(input_path.read_text())
    result = json.loads(pathlib.Path(str(stem) + '.run.json').read_text())
    started = time.monotonic()
    artifact = {'version': 1, 'plan': plan['id'], 'slot': slot['id'], 'state': 'incomplete', 'testsPassed': False, 'patchApplied': False, 'portableImports': None}
    workspace = verification / slot['id'][7:]
    try:
        answer = json.loads(answer_path.read_text())
        files = {item['path']: item['text'] for item in packet['snapshot']['files']}
        revised = dict(files)
        edits = answer.get('edits')
        if not isinstance(edits, list) or not edits:
            raise ValueError('no concrete edits supplied')
        # Match all find strings against the original snapshot, then apply nonoverlapping edits.
        ranges = {}
        for edit in edits:
            name, find, replace = edit['path'], edit['find'], edit['replace']
            if name not in files or not isinstance(find, str) or not find or not isinstance(replace, str):
                raise ValueError('edit outside fixed selected source or invalid edit')
            if files[name].count(find) != 1:
                raise ValueError('find string is not unique in original source: ' + name)
            offset = files[name].index(find)
            ranges.setdefault(name, []).append((offset, offset + len(find), replace))
        for name, replacements in ranges.items():
            prior_end = -1
            for begin, end, _ in sorted(replacements):
                if begin < prior_end:
                    raise ValueError('overlapping edits: ' + name)
                prior_end = end
            for begin, end, replacement in sorted(replacements, reverse=True):
                revised[name] = revised[name][:begin] + replacement + revised[name][end:]
        workspace.mkdir(mode=0o700)
        process = subprocess.Popen(['git', 'archive', packet['snapshot']['base']], cwd=repository, stdout=subprocess.PIPE)
        with tarfile.open(fileobj=process.stdout, mode='r|') as archive:
            for member in archive:
                name = pathlib.PurePosixPath(member.name)
                if name.is_absolute() or '..' in name.parts or member.issym() or member.islnk():
                    raise ValueError('unsafe historical archive member')
                archive.extract(member, workspace)
        if process.wait() != 0:
            raise RuntimeError('historical archive failed')
        (workspace / 'node_modules').symlink_to(repository / 'node_modules', target_is_directory=True)
        for name, content in revised.items():
            target = workspace / name
            if target.read_text() != files[name]:
                raise ValueError('historical source differs from frozen input: ' + name)
            target.write_text(content)
        artifact['patchApplied'] = True
        if slot['caseId'] == 'H05-portable-gzip':
            artifact['portableImports'] = not any('node:' in line for line in revised['src/server/Protocol.ts'].splitlines() if line.startswith('import '))
        home = workspace / '.evaluation-home'
        home.mkdir(mode=0o700)
        environment = {
            'PATH': '/private/tmp/proactive-memory-git-2.55.0/bin:' + os.environ['PATH'],
            'HOME': str(home), 'LANG': 'C.UTF-8', 'LC_ALL': 'C',
            'GIT_CONFIG_GLOBAL': '/dev/null', 'GIT_CONFIG_SYSTEM': '/dev/null', 'GIT_CONFIG_NOSYSTEM': '1',
            'PLAYWRIGHT_BROWSERS_PATH': browser_path,
            'FORCE_COLOR': '1' if slot['caseId'] == 'H01-color-environment' else '0',
            'NO_COLOR': '1',
        }
        command = ['npx', '--no-install', 'vp', 'test', 'run'] + commands[slot['caseId']]
        remaining = max(1, (plan['configuration']['budget']['wallTimeMs'] - result['elapsedMs']) / 1000)
        artifact['command'] = command
        artifact['budgetRemainingSeconds'] = remaining
        test_started = time.monotonic()
        log_path = pathlib.Path(str(stem) + '.verification.log')
        with log_path.open('w') as log:
            tested = subprocess.run(command, cwd=workspace, env=environment, stdout=log, stderr=subprocess.STDOUT, timeout=remaining)
        artifact['runtimeMs'] = (time.monotonic() - test_started) * 1000
        artifact['exitStatus'] = tested.returncode
        artifact['testsPassed'] = tested.returncode == 0
        artifact['state'] = 'checked'
    except subprocess.TimeoutExpired:
        artifact['diagnostic'] = 'fixed per-task wall budget exhausted during verification'
    except (ValueError, RuntimeError, KeyError, OSError) as error:
        artifact['state'] = 'rejected'
        artifact['diagnostic'] = str(error)
    artifact['verificationWallMs'] = (time.monotonic() - started) * 1000
    retained.write_text(json.dumps(artifact))
    print(artifact['state'], slot['caseId'], slot['variant'], slot['condition'], 'tests=' + str(artifact['testsPassed']), artifact.get('diagnostic', ''), flush=True)
