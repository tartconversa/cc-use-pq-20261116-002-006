"""Read-only staffing analysis built strictly from a PUBLISHED snapshot.

Pipeline:
  1. Rebuild the demo event from fixtures/community_event.json into a TEMPORARY
     sqlite database (the working-copy fitness.sqlite3 is never touched).
  2. Run the same publish flow as community_fitness.demo -> immutable snapshot v1.
  3. Read numbers ONLY through Publication.get(version=1) (the frozen report).
  4. Reconstruct hourly buckets from the encounter rows that were present at
     publish time and assert they reconcile to the snapshot totals, so every
     bucket is traceable to the published version rather than live collection.
Run with: python -I analysis/staffing_analysis.py <fixture_path>
"""
import json
import sys
import tempfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from community_fitness.competition import Competition  # noqa: E402
from community_fitness.publication import Publication  # noqa: E402
from community_fitness.scoring_sessions import ScoringSessions  # noqa: E402
from community_fitness.services import Services  # noqa: E402
from community_fitness.storage import Store  # noqa: E402
from community_fitness.validation import instant, zone  # noqa: E402


def build_published(fixture_path: Path, db_path: Path):
    fixture = json.loads(fixture_path.read_text(encoding='utf-8'))
    store = Store(db_path)
    competition, services, publication = Competition(store), Services(store), Publication(store)
    sessions = ScoringSessions(store)
    event = fixture['event']
    competition.create_event(event['id'], event['name'], event['starts_at'],
                             event['ends_at'], event['timezone'])
    for group in fixture['groups']:
        competition.add_group(event['id'], **group)
    for team in fixture['teams']:
        competition.register_team(event['id'], team['team_id'], team['group_code'],
                                  team['name'], team['district'], team['member_count'])
    for judge in fixture['judges']:
        competition.assign_judge(event['id'], judge['judge_id'], judge['name'],
                                 judge['group_code'])
    for service in fixture['zones']:
        services.add_zone(event['id'], **service)
    revision = store.inspect(event['id'])['revision']
    competition.change_state(event['id'], 'open', revision)
    for group in fixture['groups']:
        sessions.open(event['id'], group['code'], store.inspect(event['id'])['revision'])
    for score in fixture['scores']:
        competition.submit_score(event['id'], **score)
    ingest = services.ingest_batch(event['id'], 'demo-encounters', fixture['encounters'])
    for group in fixture['groups']:
        sessions.close(event['id'], group['code'], store.inspect(event['id'])['revision'])
    revision = store.inspect(event['id'])['revision']
    competition.change_state(event['id'], 'closed', revision)
    preview = publication.preview(event['id'])
    snapshot = publication.publish(event['id'], preview['head_version'],
                                   preview['event']['revision'])
    return store, publication, fixture, ingest, snapshot


def main() -> None:
    fixture_path = Path(sys.argv[1])
    out: dict = {}
    with tempfile.TemporaryDirectory() as directory:
        db_path = Path(directory) / 'published.sqlite3'
        store, publication, fixture, ingest, snap = build_published(fixture_path, db_path)
        event_id = snap['event']['id']
        local = zone(snap['event']['timezone'])

        # ---- frozen snapshot facts (the only "published" numbers) ----
        versions = publication.versions(event_id)
        out['snapshot'] = {
            'event_id': event_id,
            'version': snap['version'],
            'source_revision': versions[0]['source_revision'],
            'published_at': versions[0]['created_at'],
            'event_window_local': (snap['event']['starts_at'], snap['event']['ends_at']),
            'timezone': snap['event']['timezone'],
            'services_totals': {k: snap['services'][k] for k in
                                ('total_encounters', 'total_visits', 'unique_visitors')},
            'zones_in_snapshot': snap['services']['zones'],
            'days_in_snapshot': snap['services']['days'],
            'attendance_in_snapshot': {k: snap['attendance'][k] for k in
                                       ('registered_members', 'arrived_members', 'by_gate')},
        }

        # ---- zone service hours: config locked when event opened (draft-only edits) ----
        with store.connection() as db:
            zone_rows = [dict(r) for r in db.execute(
                'SELECT code,label,kind,opens_at,closes_at FROM service_zones '
                'WHERE event_id=? ORDER BY code', (event_id,))]
            # rows recorded no later than publication = what the frozen report saw
            published_at = versions[0]['created_at']
            enc_rows = [dict(r) for r in db.execute(
                'SELECT id,zone_code,occurred_at,visitor_token,count,recorded_at '
                'FROM encounters WHERE event_id=? AND recorded_at<=? ORDER BY occurred_at',
                (event_id, published_at))]
            live_count = db.execute(
                'SELECT COUNT(*) FROM encounters WHERE event_id=?', (event_id,)).fetchone()[0]
            live_after = [dict(r) for r in db.execute(
                'SELECT id,recorded_at FROM encounters WHERE event_id=? AND recorded_at>?',
                (event_id, published_at))]

        # ---- reconciliation: raw rows must reproduce the snapshot aggregates ----
        per_zone = defaultdict(lambda: {'encounters': 0, 'visits': 0, 'tokens': set()})
        per_day = defaultdict(lambda: {'encounters': 0, 'visits': 0, 'tokens': set()})
        hourly = defaultdict(lambda: {'encounters': 0, 'visits': 0})
        for r in enc_rows:
            local_dt = instant(r['occurred_at']).astimezone(local)
            hour = local_dt.strftime('%Y-%m-%dT%H:00')
            day = local_dt.date().isoformat()
            per_zone[r['zone_code']]['encounters'] += 1
            per_zone[r['zone_code']]['visits'] += r['count']
            per_zone[r['zone_code']]['tokens'].add(r['visitor_token'])
            per_day[day]['encounters'] += 1
            per_day[day]['visits'] += r['count']
            per_day[day]['tokens'].add(r['visitor_token'])
            hourly[f"{r['zone_code']}|{hour}"]['encounters'] += 1
            hourly[f"{r['zone_code']}|{hour}"]['visits'] += r['count']

        recon_zone = {z['code']: (
            per_zone[z['code']]['encounters'] == z['encounters']
            and per_zone[z['code']]['visits'] == z['visits']
            and len(per_zone[z['code']]['tokens']) == z['unique_visitors'])
            for z in snap['services']['zones']}
        recon_day = {d['date']: (
            per_day[d['date']]['encounters'] == d['encounters']
            and per_day[d['date']]['visits'] == d['visits']
            and len(per_day[d['date']]['tokens']) == d['unique_visitors'])
            for d in snap['services']['days']}
        total_visits_recon = sum(r['count'] for r in enc_rows)

        # ---- coverage: which zone-open hours actually carry a record ----
        coverage = []
        for z in zone_rows:
            opens = instant(z['opens_at']).astimezone(local)
            closes = instant(z['closes_at']).astimezone(local)
            open_hours = int((closes - opens).total_seconds() // 3600)
            recorded_hours = len({k.split('|', 1)[1] for k in hourly
                                  if k.startswith(z['code'] + '|')})
            coverage.append({'zone': z['code'], 'opens_local':
                             opens.strftime('%H:%M'), 'closes_local':
                             closes.strftime('%H:%M'), 'open_hours': open_hours,
                             'recorded_hours': recorded_hours,
                             'blank_zone_hours': open_hours - recorded_hours})

        out['zone_hours_locked_config'] = zone_rows
        out['reconciliation'] = {
            'ingest_accepted': len(ingest['accepted']),
            'ingest_rejected': len(ingest['rejected']),
            'zone_totals_match_snapshot': recon_zone,
            'day_totals_match_snapshot': recon_day,
            'visits_match_snapshot': total_visits_recon == snap['services']['total_visits'],
            'rows_at_publish': len(enc_rows),
            'rows_live_now': live_count,
            'late_rows_after_publish': live_after,
        }
        out['hourly_buckets_local'] = [
            {'zone': k.split('|', 1)[0], 'hour_local': k.split('|', 1)[1], **v}
            for k, v in sorted(hourly.items())]
        out['zone_hour_coverage'] = coverage
        out['sparsity'] = {
            'events_published': 1,
            'snapshot_versions': [v['version'] for v in versions],
            'distinct_encounter_days': len(per_day),
            'distinct_encounter_hours_overall': len({k.split('|', 1)[1] for k in hourly}),
            'total_open_zone_hours': sum(c['open_hours'] for c in coverage),
            'blank_open_zone_hours': sum(c['blank_zone_hours'] for c in coverage),
            'attendance_records': snap['attendance']['arrived_members'],
        }

    print(json.dumps(out, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
