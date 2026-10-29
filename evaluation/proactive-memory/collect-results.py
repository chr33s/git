#!/usr/bin/env python3
"""Join retained host/provider measurements and fixed task checks into typed samples."""
import hashlib
import json
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1]).resolve()
contexts_path = pathlib.Path(sys.argv[2]).resolve()
plan = json.loads((root / 'plan.json').read_text())
corpus = json.loads((root / 'corpus.json').read_text())
contexts = json.loads(contexts_path.read_text())['entries']
index = {(entry['caseId'], entry['variant'], entry['condition']): entry for entry in contexts}
maintainer_reviews = {entry['slot']: entry for entry in json.loads((root / 'maintainer-review.json').read_text())['entries']}
reviews = []
cost = 0.0

def measured(value, source, evidence):
    return {'state': 'measured', 'value': value, 'source': source, 'evidence': evidence}

def unavailable(reason):
    return {'state': 'unavailable', 'reason': reason}

for slot in plan['slots']:
    stem = root / slot['id'][7:]
    run_path = pathlib.Path(str(stem) + '.run.json')
    verify_path = pathlib.Path(str(stem) + '.verification.json')
    run = json.loads(run_path.read_text())
    verification = json.loads(verify_path.read_text())
    packet = json.loads(pathlib.Path(str(stem) + '.input.json').read_text())
    answer = json.loads(pathlib.Path(str(stem) + '.answer.txt').read_text())
    context = index[(slot['caseId'], slot['variant'], slot['condition'])]
    usage = run['usage']
    prompt_tokens = usage['input_tokens'] + usage['cache_read_input_tokens'] + usage['cache_creation_input_tokens']
    budget_ok = prompt_tokens <= plan['configuration']['budget']['inputTokens'] and usage['output_tokens'] <= plan['configuration']['budget']['outputTokens'] and run['elapsedMs'] + verification['verificationWallMs'] <= plan['configuration']['budget']['wallTimeMs']
    log = pathlib.Path(str(stem) + '.verification.log').read_text()
    tested = re.search(r'Tests\s+(\d+) passed', re.sub(r'\x1b\[[0-9;]*m', '', log))
    # Runtime tests establish the candidate's scoped behavior, not the model's self-report.
    accepted = run['state'] == 'completed' and verification['testsPassed'] and verification['patchApplied'] and tested is not None and budget_ok
    if slot['caseId'] == 'H05-portable-gzip':
        accepted = accepted and verification['portableImports'] is True
    if not isinstance(answer.get('diagnosis'), str) or not answer['diagnosis'].strip():
        accepted = False
    source_paths = {entry['path'] for entry in packet['snapshot']['files']}
    if any(item.get('path') not in source_paths for item in answer.get('evidence', [])):
        accepted = False
    maintainer_review = maintainer_reviews[slot['id']]
    accepted = accepted and maintainer_review['accepted']
    review = {
        'version': 1, 'plan': plan['id'], 'slot': slot['id'],
        'reviewer': 'benchmark preparer using retained source, preregistered criteria, and executed historical tests',
        'acceptedTaskResult': bool(accepted),
        'basis': {'caseId': slot['caseId'], 'testsPassed': verification['testsPassed'], 'passedTests': int(tested.group(1)) if tested else None, 'budgetSatisfied': budget_ok, 'maintainerAccepted': maintainer_review['accepted'], 'portableImports': verification['portableImports'], 'modelSelfReportUsedForAcceptance': False},
        'reviewerCorrections': 0,
        'maintainerReview': maintainer_review,
        'environmentCorrection': verification.get('environmentCorrection'),
        'claimUnit': 'one preregistered lesson supplied in startup context; does not count individual clauses as independent claims',
        'unsupportedClaimsSupplied': int(slot['variant'] in ['support-tombstoned-before-start', 'support-compromised-before-start'] and context['observed']['containsLesson']),
        'staleClaimsSupplied': int(slot['variant'] == 'support-tombstoned-before-start' and context['observed']['containsLesson']),
        'limitations': ['No model tools were enabled; exploration was fixed at zero by the design.', 'The supported-diagnosis time is an upper bound at the completed first response, not a streaming first-token measurement.', 'Curated knowledge was fixture-reviewed against known historical fixes; this does not evaluate autonomous semantic curation quality.'],
    }
    review_path = pathlib.Path(str(stem) + '.review.json')
    review_path.write_text(json.dumps(review))
    reviews.append(review)
    correction = context['timings']['correctionLatencyMs']
    if slot['variant'] == 'normal':
        correction_missing = 'No invalidation was introduced in this case.'
    elif slot['variant'] == 'dependency-changed-after-briefing' and slot['condition'] != 'recall-with-reviewed-curation':
        correction_missing = 'This condition has no authored Concept with that declared dependency.'
    elif context['observed']['containsLesson']:
        correction_missing = 'Affected support remained present at the next observed boundary; no correction latency was established.'
    else:
        correction_missing = 'No correction timing was retained.'

    metrics = {
        'repeatedExploratoryWork': measured(0, 'host', [run_path.name + '#toolsEnabled=false']),
        'timeToSupportedDiagnosisMs': measured(run['elapsedMs'], 'host', [run_path.name, review_path.name]) if maintainer_review['diagnosisSupported'] else unavailable('No supported first diagnosis established.'),
        'acceptedTaskResult': measured(int(accepted), 'reviewer', [review_path.name, verify_path.name]),
        'reviewerCorrections': measured(0, 'reviewer', [review_path.name]),
        'unsupportedClaimsSupplied': measured(review['unsupportedClaimsSupplied'], 'reviewer', [review_path.name, 'condition-contexts.json']),
        'staleClaimsSupplied': measured(review['staleClaimsSupplied'], 'reviewer', [review_path.name, 'condition-contexts.json']),
        'correctionLatencyMs': measured(correction, 'host', ['context-observations.json']) if correction is not None else unavailable(correction_missing),
        'inputTokens': measured(prompt_tokens, 'runner', [run_path.name + '#usage']),
        'outputTokens': measured(usage['output_tokens'], 'runner', [run_path.name + '#usage']),
        'cacheReadTokens': measured(usage['cache_read_input_tokens'], 'runner', [run_path.name + '#usage']),
        'cacheWriteTokens': measured(usage['cache_creation_input_tokens'], 'runner', [run_path.name + '#usage']),
        'startupLatencyMs': measured(context['timings']['startupLatencyMs'], 'host', ['context-observations.json']),
        'maintenanceRuntimeMs': measured(context['timings']['maintenanceRuntimeMs'], 'host', ['context-observations.json']),
        'conflicts': measured(context['observed']['conflicts'], 'host', ['context-observations.json']),
        'incompleteJobs': measured(context['observed']['incompleteJobs'], 'host', ['context-observations.json']),
    }
    assert set(metrics) == set(corpus['metrics'])
    sample = {'version': 1, 'plan': plan['id'], 'slot': slot['id'], 'attempt': run['attempt'], 'inputDigest': run['inputDigest'], 'outputDigest': run['outputDigest'], 'state': 'completed' if run['state'] == 'completed' else 'failed', 'metrics': metrics, 'diagnostic': 'Retrospective fixed-source diagnosis and proposed-edit pilot; read review and method limitations.'}
    pathlib.Path(str(stem) + '.sample.json').write_text(json.dumps(sample))
    cost += sum(value.get('costUSD', 0) for value in run['modelUsage'].values())
(root / 'context-observations.json').write_text(json.dumps({'version': 1, 'entries': contexts}))
(root / 'review.json').write_text(json.dumps({'version': 1, 'plan': plan['id'], 'reviews': reviews, 'cliReportedListPriceUSD': cost, 'historicalTaskScopes': len(corpus['cases']), 'historicalFixCommits': len({case['referenceFix'] for case in corpus['cases']}), 'controlledVariantCombinations': len(plan['slots']) // 3, 'completedCLIInvocations': len(reviews)}, indent=2))
print(json.dumps({'samples': len(reviews), 'accepted': sum(row['acceptedTaskResult'] for row in reviews), 'cliReportedListPriceUSD': cost}))
