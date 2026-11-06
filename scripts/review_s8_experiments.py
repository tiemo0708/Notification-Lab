"""Reconcile archived S8 measurements without running apps, Docker, or network calls."""
import argparse
from collections import defaultdict
from decimal import Decimal, getcontext
import hashlib
import json
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]
EXPERIMENTS = ROOT / 'docs/experiments'
getcontext().prec = 40
D = Decimal
STUDIES = [
    ('consumers', 's8-consumer-scaling-2026-11-04-group-2', 's8-consumer-scaling-2026-11-04-validation.json', [1, 2, 3, 3, 2, 1]),
    ('partitions', 's8-partition-scaling-2026-11-04-group-1', 's8-partition-scaling-2026-11-04-group-1-validation.json', [3, 6, 6, 3]),
    ('compression', 's8-compression-delivery-2026-11-05-group-1', 's8-compression-delivery-2026-11-05-group-1-validation.json', ['none', 'lz4', 'lz4', 'none']),
    ('window', 's8-relay-window-delivery-2026-11-06-group-1', 's8-relay-window-delivery-2026-11-06-group-1-validation.json', [1, 10, 10, 1]),
]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def resolve(reference):
    path = ROOT / reference if reference.startswith('docs/') else EXPERIMENTS / reference
    require(path.resolve().is_relative_to(EXPERIMENTS.resolve()), 'Evidence path leaves docs/experiments')
    return path


def load(path):
    require(path.stat().st_size <= 2_000_000, 'Evidence exceeds bounded input size')
    return json.loads(path.read_text(encoding='utf-8'), parse_float=D,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError('Nonfinite JSON: ' + value)))


def close(actual, expected, label):
    require(abs(D(str(actual)) - expected) <= D('1e-8') * max(D(1), abs(expected)), label)


def review():
    evidence = {}

    def retain(path, expected=None):
        digest = sha(path)
        require(expected is None or digest == expected, 'SHA mismatch: ' + path.name)
        evidence[str(path.relative_to(ROOT))] = digest
        return load(path)

    studies = []
    for kind, prefix, validation_name, sequence in STUDIES:
        summary = retain(EXPERIMENTS / (prefix + '-summary.json'))
        source = retain(EXPERIMENTS / (prefix + '-source.json'))
        validation = retain(EXPERIMENTS / validation_name)
        archived_assets = validation.get('artifactSha256', validation.get('experimentAssetsSha256'))
        require(archived_assets is not None, 'Missing historical asset hashes')
        for ref, digest in archived_assets.items():
            path = resolve(ref)
            # PNG provenance is checked as bytes, not parsed as JSON.
            if path.suffix == '.json':
                retain(path, digest)
            else:
                require(sha(path) == digest, 'Historical image SHA mismatch')
                evidence[str(path.relative_to(ROOT))] = digest
        manifest = source.get('sourceFiles', source.get('sourceSha256'))
        require(bool(manifest), 'Missing source inventory')
        reported_freeze = validation.get('sourceFilesUnchanged', validation.get('sourceAndBuildConfigurationHashesUnchanged'))
        require(reported_freeze is True, 'Historical source freeze not reported')
        source_order = source.get('sequence', source.get('protocol', {}).get('order'))
        require(source_order == sequence, 'Protocol sequence mismatch')
        rows, grouped = [], defaultdict(list)
        invariant = None
        for index, entry in enumerate(summary.get('rows', summary.get('runs')), 1):
            raw = retain(resolve(entry.get('rawFile', entry.get('raw'))), entry['rawSha256'])
            require(raw['result'] == 'PASS' and raw['targets'] == 10000, 'Invalid final experiment')
            factor = {
                'consumers': lambda: raw['consumerComparison']['campaignConsumers'],
                'partitions': lambda: raw['partitionComparison']['campaignRequestAndRetryPartitions'],
                'compression': lambda: raw['compressionComparison']['campaignCodec'],
                'window': lambda: raw['campaignRelayConfiguration']['submissionWindowSize'],
            }[kind]()
            require(factor == sequence[index - 1], 'Run order/factor mismatch')
            settings = dict(raw['settings'])
            settings.pop('campaignWorkerMaxActive', None)
            common = {k: raw[k] for k in ['java', 'os', 'jvmMaxHeapBytes', 'availableProcessors', 'infra', 'bounds',
                      'redisConnectionsPerLane', 'publishBatchSize', 'campaignRelayInstances', 'relayScheduling',
                      'fineTimingEnabled', 'transientQuotaRecoveryEnabled', 'offsetComparison', 'campaignOffsetMode']}
            common['settings'] = settings
            common['stageTimingVersion'] = raw['stageTimingVersion']
            if kind in ['compression', 'window']:
                stable = {lane: dict(raw[lane + 'ProducerConfiguration']) for lane in ['campaign', 'realtime']}
                if kind == 'compression':
                    stable['campaign'].pop('compression.type')
                common['fixedProducerConfiguration'] = stable
            require(invariant is None or common == invariant, 'Within-study fixed conditions changed')
            invariant = common
            require(raw['campaignOffsetMode'] == 'RECORD' and raw['publishBatchSize'] == 1, 'Unexpected delivery policy')
            for lane in ['campaign', 'realtime']:
                cfg = raw[lane + 'ConsumerConfiguration']
                n = factor if kind == 'consumers' and lane == 'campaign' else 3
                require(cfg['configuredConsumers'] == cfg['childContainers'] == cfg['workerMaxActive'] == n, 'Consumer bounds differ')
                require(cfg['syncCommits'] is True and cfg['ackMode'] == 'RECORD', 'Commit policy differs')
                partitions = factor if kind == 'partitions' and lane == 'campaign' else 3
                require(cfg['assignedPartitions'] == 2 * partitions, 'Partition assignment differs')
            audit, stats = raw['httpAudit'], raw['mockStats']
            rt = audit['uniqueRealtime']
            require(audit['uniqueCampaign'] == raw['verifiedTargetMappings'] == 10000, 'Target mapping count differs')
            require(audit['duplicates'] == audit['invalid'] == 0, 'Audit duplicate/invalid records')
            require(stats['requests'] == stats['accepted'] == 10000 + rt, 'HTTP totals differ')
            require(all(stats[k] == 0 for k in ['rateLimited', 'serverErrors', 'permanentFailures', 'busy', 'active']), 'HTTP errors/activity remain')
            require(raw['campaignAllowed'] == 10000 and raw['realtimeAllowed'] == rt, 'Quota acceptance totals differ')
            require(rt == raw['mixedProbes'] + raw['settings']['baselineProbes'], 'Realtime baseline denominator differs')
            require(raw['mixedApiLatencyMs']['count'] == raw['mixedProbes'], 'Mixed p95 population differs')
            for lane, count in [('campaign', 10000), ('realtime', rt)]:
                offsets = raw[lane + 'Offsets']
                parts = factor if kind == 'partitions' and lane == 'campaign' else 3
                require(len(offsets) == 2 * parts + 3, 'Missing offset rows')
                require(len({(o['topic'], o['partition']) for o in offsets}) == len(offsets), 'Duplicate offset rows')
                require(all(o['end'] == o['committed'] for o in offsets), 'Uncommitted records remain')
                require(sum(o['end'] for o in offsets) == count, 'Offset total differs')
                require(all(v == 0 for v in raw['quotaDiagnostics'][lane]['unavailableByReason'].values()), 'Quota lookup errors')
            if kind in ['compression', 'window']:
                for lane in ['campaign', 'realtime']:
                    cfg = raw[lane + 'ProducerConfiguration']
                    require(cfg['compression.type'] == (factor if kind == 'compression' and lane == 'campaign' else 'none'), 'Codec differs')
                    require(cfg['linger.ms'] == '0' and cfg['batch.size'] == '16384' and cfg['acks'] == 'all', 'Producer settings differ')
                    require(cfg['enable.idempotence'] == 'true' and cfg['max.in.flight.requests.per.connection'] == '1', 'Producer safety differs')
                    require(raw['producerLifetimeMetrics'][lane]['record-retry-total'] == raw['producerLifetimeMetrics'][lane]['record-error-total'] == 0, 'Producer errors/retries')
            if kind == 'window':
                require(raw['realtimeRelayConfiguration'] == {'submissionWindowSize': 1, 'publishBatchSize': 1}, 'Realtime relay changed')
                for lane, count in [('campaign', 10000), ('realtime', rt)]:
                    completion = raw['relayCompletion'][lane]
                    require(completion['published'] == completion['ackObservations'] == count, 'Relay completion differs')
                    require(completion['publishFailures'] == completion['unconfirmedAcks'] == 0, 'Uncertain publish outcome')
            seconds = raw['campaignDrainSeconds']
            rate = D(10000) / seconds
            close(raw['campaignPerSecond'], rate, 'Raw rate formula differs')
            close(entry.get('campaignPerSecond', entry.get('campaignPerSecondRecomputed')), rate, 'Summary rate differs')
            row = {'index': index, 'condition': str(factor), 'targets': 10000, 'drainSeconds': seconds,
                   'ratePerSecond': rate, 'mixedApiP95Ms': raw['mixedApiLatencyMs']['p95'],
                   'realtimeAcceptedIncludingBaseline': rt, 'httpAccepted': stats['accepted'],
                   'raw': str(resolve(entry.get('rawFile', entry.get('raw'))).relative_to(ROOT))}
            if 'producerLifetimeMetrics' in raw:
                row['campaignOutgoingBytesLifetime'] = raw['producerLifetimeMetrics']['campaign']['outgoing-byte-total']
                row['campaignRecordsPerRequestWindowAverage'] = raw['producerLifetimeMetrics']['campaign']['records-per-request-avg']
            rows.append(row); grouped[str(factor)].append(row)
        require(len(rows) == len(sequence), 'Missing runs')
        totals = {'campaign': 10000 * len(rows), 'realtimeIncludingBaselines': sum(r['realtimeAcceptedIncludingBaseline'] for r in rows),
                  'httpAccepted': sum(r['httpAccepted'] for r in rows)}
        if kind == 'window':
            require(summary['totals']['campaign'] == totals['campaign'] and summary['totals']['httpAccepted'] == totals['httpAccepted'], 'Summary totals differ')
        else:
            require(summary['totalCampaignAccepted'] == totals['campaign'] and summary['totalRealtimeAccepted'] == totals['realtimeIncludingBaselines'] and summary['totalHttpAccepted'] == totals['httpAccepted'], 'Summary totals differ')
        groups = []
        for condition, items in grouped.items():
            require(len(items) == 2, 'Expected two repeats per condition')
            rates = [r['ratePerSecond'] for r in items]
            group = {'condition': condition, 'runs': 2, 'targets': 20000,
                     'summedDrainSeconds': sum(r['drainSeconds'] for r in items),
                     'pooledRatePerSecond': D(20000) / sum(r['drainSeconds'] for r in items),
                     'perRunRateArithmeticMean': sum(rates) / 2, 'rateRange': [min(rates), max(rates)],
                     'mixedApiP95RangeMs': [min(r['mixedApiP95Ms'] for r in items), max(r['mixedApiP95Ms'] for r in items)]}
            if 'campaignOutgoingBytesLifetime' in items[0]:
                group['campaignOutgoingBytesLifetimeMean'] = sum(r['campaignOutgoingBytesLifetime'] for r in items) / 2
            if kind == 'window':
                previous = summary['byWindow'][condition]
                close(previous['pooledCampaignPerSecond'], group['pooledRatePerSecond'], 'Historical pooled rate differs')
                close(previous['perRunRateArithmeticMean'], group['perRunRateArithmeticMean'], 'Historical mean differs')
            else:
                key = {'consumers': 'concurrency', 'partitions': 'campaignPartitions', 'compression': 'campaignCodec'}[kind]
                previous = next(g for g in summary['summary'] if str(g[key]) == condition)
                close(previous['throughputMean'], group['perRunRateArithmeticMean'], 'Historical mean differs')
            groups.append(group)
        studies.append({'id': kind, 'sourceManifest': str((EXPERIMENTS / (prefix + '-source.json')).relative_to(ROOT)),
                        'baseCommit': source['baseCommit'], 'archivedSourceInventoryCount': len(manifest),
                        'historicalFreezeReported': True, 'fixedRuntimeConditions': invariant, 'sequence': sequence,
                        'runs': rows, 'byCondition': groups, 'totals': totals})

    producer_summary = retain(EXPERIMENTS / 's8-producer-batching-2026-11-04-summary.json')
    producer = retain(resolve(producer_summary['rawFile']), producer_summary['rawSha256'])
    psource = retain(EXPERIMENTS / 's8-producer-batching-2026-11-04-source.json')
    pvalidation = retain(EXPERIMENTS / 's8-producer-batching-2026-11-04-validation.json')
    for ref, digest in pvalidation['artifactSha256'].items():
        retain(resolve(ref), digest)
    require(producer['result'] == 'PASS' and producer['sequence'] == psource['sequence'], 'Producer sequence/result differs')
    require(pvalidation['sourceFilesUnchanged'] is True, 'Producer historical freeze not reported')
    require([row['mode'] for row in producer['rows']] == producer['sequence'], 'Producer row sequence differs')
    modes = {'SERIAL_NONE_L0': (1, 0, 'none'), 'SERIAL_NONE_L5': (1, 5, 'none'),
             'WINDOW10_NONE_L5': (10, 5, 'none'), 'WINDOW10_LZ4_L5': (10, 5, 'lz4')}
    pgroup = defaultdict(list)
    for row in producer['rows']:
        require((row['submissionWindow'], row['lingerMillis'], row['compression']) == modes[row['mode']], 'Producer mode settings differ')
        require(row['verifiedRecords'] == 1650 and row['warmupRecords'] == 150, 'Producer warmup differs')
        require(row['verifiedPartitionOffsets'] == [550, 550, 550] and row['committedEqualsEnd'] and row['verifiedContentAndPerPartitionOrder'], 'Producer correctness not verified')
        delta = row['measurementCounterDeltas']
        require(delta['record-send-total'] == 1500 and delta['record-error-total'] == delta['record-retry-total'] == 0, 'Measured producer counters differ')
        rate = D(1500) / row['sendToLastAckSeconds']
        close(row['recordsPerSecond'], rate, 'Producer rate formula differs')
        pgroup[row['mode']].append({'rate': rate, 'seconds': row['sendToLastAckSeconds'], 'measuredOutgoingBytes': delta['outgoing-byte-total'], 'warmupRetries': row['warmupRetries']})
    require(producer['verifiedRecords'] == 13200 and producer['measuredRecords'] == 12000, 'Producer totals differ')
    pby = []
    for mode, items in pgroup.items():
        require(len(items) == 2, 'Expected producer repeats')
        pby.append({'mode': mode, 'runs': 2, 'measuredRecords': 3000, 'pooledRecordsPerSecond': D(3000) / sum(r['seconds'] for r in items),
                    'perRunRateArithmeticMean': sum(r['rate'] for r in items) / 2,
                    'meanMeasuredOutgoingBytes': sum(r['measuredOutgoingBytes'] for r in items) / 2,
                    'warmupRetries': [r['warmupRetries'] for r in items]})
    for group in pby:
        previous = next(g for g in producer_summary['summary'] if g['mode'] == group['mode'])
        close(previous['throughputMean'], group['perRunRateArithmeticMean'], 'Historical producer mean differs')
        close(previous['meanOutgoingBytesMeasured'], group['meanMeasuredOutgoingBytes'], 'Historical producer bytes differ')
    yaml = (ROOT / 'src/main/resources/application.yml').read_text()
    worker = yaml.split('  worker:\n', 1)[1].split('\n  delivery:', 1)[0]
    require(all(re.search(r'^    ' + key + ': ' + str(value) + '$', worker, re.M) for key, value in [('consumers', 1), ('max-active', 1), ('queue-capacity', 2), ('queue-wait-ms', 1000)]), 'Application defaults changed; reassess decision')
    # Compare the unchanged historical configuration bytes, rather than inferring defaults from the load fixture.
    consumer_source = load(EXPERIMENTS / (STUDIES[0][1] + '-source.json'))
    historic = subprocess.check_output(['git', 'show', consumer_source['baseCommit'] + ':src/main/resources/application.yml'], cwd=ROOT)
    require(hashlib.sha256(historic).hexdigest() == consumer_source['sourceFiles']['src/main/resources/application.yml'], 'Historical configuration provenance differs')
    require(b'  worker:\n    consumers: 1\n    max-active: 1\n    queue-capacity: 2\n' in historic, 'Historical default differs')
    totals = {key: sum(s['totals'][key] for s in studies) for key in ['campaign', 'realtimeIncludingBaselines', 'httpAccepted']}
    require(totals == {'campaign': 180000, 'realtimeIncludingBaselines': 12065, 'httpAccepted': 192065}, 'S8 acceptance total differs')
    return {'reviewDateKst': '2026-11-06', 'assessment': 'Share with caveats',
            'methods': {'primaryRate': 'sum campaign acceptances / sum drain seconds within each study and condition',
                        'secondaryRate': 'arithmetic mean of two per-run rates; differs from pooled rate',
                        'percentiles': 'per-run p95/range only; never averaged or pooled from summaries',
                        'historicalFreeze': 'archived validations report unchanged code within studies; current review verifies archived file hashes, not a new execution',
                        'crossStudyComparison': 'no causal comparison across changed source/protocol/clock; no overall pooled throughput',
                        'scope': 'HTTP Mock acceptance, not physical delivery or production capacity'},
            'studies': studies, 'httpExperimentTotals': totals,
            'producerOnly': {'baseCommit': psource['baseCommit'], 'raw': producer_summary['rawFile'], 'runs': 8,
                             'verifiedRecords': 13200, 'warmupRecords': 1200, 'measuredRecords': 12000, 'byCondition': pby},
            'applicationWorkerDefaults': {'consumers': 1, 'maxActive': 1, 'queueCapacity': 2, 'queueWaitMs': 1000,
                                          'historicalConsumerStudyDefaultAlsoOne': True},
            'evidenceSha256': dict(sorted(evidence.items())),
            'currentDefaultConfigurationSha256': sha(ROOT / 'src/main/resources/application.yml'),
            'newLoadExecutions': 0, 'defaultSettingsChanged': False, 'remoteCiRun': False, 'pushed': False}


def encode(result):
    return json.dumps(result, ensure_ascii=False, indent=2, default=lambda value: str(value) if isinstance(value, D) else value, allow_nan=False) + '\n'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--output', type=Path, help='Create a new review file; refuse to overwrite')
    mode.add_argument('--check', type=Path, help='Recompute and compare an existing review')
    args = parser.parse_args()
    result = encode(review())
    if args.check:
        require(args.check.read_text(encoding='utf-8') == result, 'Review output differs from recomputed evidence')
        print('PASS: 18 HTTP runs / 8 producer runs / hashes, settings, counters, offsets, rates and defaults')
    else:
        with args.output.open('x', encoding='utf-8') as stream:
            stream.write(result)
        print(args.output)


if __name__ == '__main__':
    main()
