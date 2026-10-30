import 'dart:convert';
import 'dart:ffi';
import 'dart:io';

import 'package:amobi_sync_flutter/amobi_upload_journal.dart';
import 'package:amobi_sync_flutter/powersync.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:sqlite3/open.dart' as sqlite_open;
import 'package:sqlite3/sqlite3.dart';
import 'package:sqlite_async/sqlite3_common.dart';
import 'package:sqlite_async/sqlite_async.dart';

const _schema = Schema([
  Table('history_table_ez_ai_app', [Column.text('history_name')]),
  Table('chat_table_ez_ai_app', [Column.text('history_id'), Column.text('content'), Column.text('file_data')]),
  Table('stored_files_table', [Column.text('stored_path'), Column.text('original_name')]),
]);

final String _extension =
    Platform.environment['POWERSYNC_EXTENSION'] ?? File('../dart/.native/libpowersync.dylib').absolute.path;

class _HostFactory extends PowerSyncOpenFactory {
  _HostFactory(String path) : super(path: path);
  @override
  CommonDatabase open(SqliteOpenOptions options) {
    sqlite_open.open.overrideFor(
        sqlite_open.OperatingSystem.macOS, () => DynamicLibrary.open('/opt/homebrew/opt/sqlite/lib/libsqlite3.dylib'));
    return super.open(options);
  }

  @override
  void enableExtension() => sqlite3
      .ensureExtensionLoaded(SqliteExtension.inLibrary(DynamicLibrary.open(_extension), 'sqlite3_powersync_init'));
}

Future<T> _withDb<T>(String file, Future<T> Function(PowerSyncDatabase) action) async {
  final db = PowerSyncDatabase.withFactory(_HostFactory(file), schema: _schema, maxReaders: 1);
  try {
    await db.initialize();
    return await action(db);
  } finally {
    await db.close();
  }
}

String _body(List<CrudEntry> entries) => jsonEncode({
      'batch': [
        for (final entry in entries)
          {
            'op': switch (entry.op) {
              UpdateType.put => 'PUT',
              UpdateType.patch => 'PATCH',
              UpdateType.delete => 'DELETE'
            },
            'table': entry.table,
            'data': {...?entry.opData, 'id': entry.id, 'user_id': 'owner-a'},
          }
      ]
    });

http.Response _ack(String key, int count, {int skipped = 0}) =>
    http.Response(jsonEncode({'ok': true, 'applied': count, 'skipped': skipped}), 200,
        headers: {'Idempotency-Key': key, 'X-Amobi-Upload-Protocol': amobiUploadProtocol});

Future<bool> _upload(AmobiUploadJournal journal, Future<http.Response> Function(String, String) transport) =>
    journal.upload(uid: 'owner-a', buildBody: _body, transport: transport, isCurrent: () => true);

Future<void> _seedLongDelete(PowerSyncDatabase db) async {
  await db.writeTransaction((tx) async {
    await tx.execute('INSERT INTO history_table_ez_ai_app(id,history_name) VALUES(?,?)', ['session', 'Existing']);
    for (var i = 0; i < 102; i++) {
      await tx.execute('INSERT INTO chat_table_ez_ai_app(id,history_id,content) VALUES(?,?,?)',
          ['message-$i', 'session', 'Text $i']);
    }
  });
  await (await db.getNextCrudTransaction())!.complete();
  await db.writeTransaction((tx) async {
    await tx.execute('DELETE FROM chat_table_ez_ai_app WHERE history_id=?', ['session']);
    await tx.execute('DELETE FROM history_table_ez_ai_app WHERE id=?', ['session']);
  });
  expect((await db.getNextCrudTransaction())!.crud, hasLength(103));
}

void main() {
  test('durable ACK accepts explicit local-only entries while applying chat writes', () async {
    final dir = await Directory.systemTemp.createTemp('ezai-local-skip-');
    try {
      await _withDb('${dir.path}/owner.db', (db) async {
        final journal = await AmobiUploadJournal.initialize(db);
        await db.writeTransaction((tx) async {
          await tx.execute('INSERT INTO stored_files_table(id,stored_path,original_name) VALUES(?,?,?)',
              ['file-1', '/private/device/path', 'receipt.pdf']);
          await tx.execute('INSERT INTO chat_table_ez_ai_app(id,history_id,content) VALUES(?,?,?)',
              ['message-1', 'session', 'hello']);
        });
        final sent = <String>[];
        expect(
            await journal.upload(
              uid: 'owner-a',
              buildBody: (entries) => jsonEncode({
                'batch': [
                  for (final entry in entries)
                    entry.table == 'stored_files_table'
                        ? {
                            'op': 'SKIP',
                            'table': entry.table,
                            'data': {'id': entry.id, 'user_id': 'owner-a'}
                          }
                        : {
                            'op': 'PUT',
                            'table': entry.table,
                            'data': {...?entry.opData, 'id': entry.id, 'user_id': 'owner-a'}
                          },
                ]
              }),
              transport: (key, body) async {
                sent.add(body);
                return _ack(key, 1, skipped: 1);
              },
              isCurrent: () => true,
            ),
            isTrue);
        expect(sent, hasLength(1));
        expect(sent.single, isNot(contains('/private/device/path')));
        expect(await db.getAll('SELECT id FROM ps_crud'), isEmpty);
      });
    } finally {
      await dir.delete(recursive: true);
    }
  });

  test('server cannot acknowledge a local-only row as an applied remote write', () async {
    final dir = await Directory.systemTemp.createTemp('ezai-local-skip-ack-mismatch-');
    try {
      await _withDb('${dir.path}/owner.db', (db) async {
        final journal = await AmobiUploadJournal.initialize(db);
        await db.execute('INSERT INTO stored_files_table(id,stored_path) VALUES(?,?)', ['file-1', '/private/path']);
        await expectLater(
          journal.upload(
            uid: 'owner-a',
            buildBody: (entries) => jsonEncode({
              'batch': [
                {
                  'op': 'SKIP',
                  'table': 'stored_files_table',
                  'data': {'id': entries.single.id, 'user_id': 'owner-a'}
                },
              ]
            }),
            transport: (key, body) async => _ack(key, 1),
            isCurrent: () => true,
          ),
          throwsA(isA<AmobiUploadHeldException>()),
        );
        expect(await db.getAll('SELECT id FROM ps_crud'), hasLength(1));
        expect((await db.get('SELECT outcome FROM amobi_ezai_upload_journal_pending'))['outcome'], 2);
      });
    } finally {
      await dir.delete(recursive: true);
    }
  });

  test('ACK persisted before stale owner fence advances to next chunk on reopen', () async {
    final dir = await Directory.systemTemp.createTemp('ezai-stale-chunk-');
    final file = '${dir.path}/owner.db';
    try {
      late String firstKey;
      late String firstBody;
      await _withDb(file, (db) async {
        final journal = await AmobiUploadJournal.initialize(db);
        await _seedLongDelete(db);
        var current = true;
        await expectLater(
            journal.upload(
                uid: 'owner-a',
                buildBody: _body,
                transport: (key, body) async {
                  firstKey = key;
                  firstBody = body;
                  current = false;
                  return _ack(key, 100);
                },
                isCurrent: () => current),
            throwsA(isA<AmobiUploadSessionChangedException>()));
        final pending =
            await db.get('SELECT upload_key,body,outcome,chunk_offset FROM amobi_ezai_upload_journal_pending');
        expect(pending['upload_key'], firstKey);
        expect(utf8.decode(pending['body'] as List<int>), firstBody);
        expect(pending['outcome'], 1);
        expect(pending['chunk_offset'], 0);
        expect(await db.getAll('SELECT id FROM ps_crud'), hasLength(103));
      });
      await _withDb(file, (db) async {
        final journal = await AmobiUploadJournal.initialize(db);
        final requests = <(String, String)>[];
        expect(
            await _upload(journal, (key, body) async {
              requests.add((key, body));
              return _ack(key, (jsonDecode(body)['batch'] as List).length);
            }),
            isTrue);
        expect(requests, hasLength(1));
        expect(requests.single.$1, isNot(firstKey));
        expect((jsonDecode(requests.single.$2)['batch'] as List), hasLength(3));
        expect(await db.getAll('SELECT id FROM ps_crud'), isEmpty);
      });
    } finally {
      await dir.delete(recursive: true);
    }
  });
  test('two large real SDK rows split by byte limit and each upload once', () async {
    final dir = await Directory.systemTemp.createTemp('ezai-byte-chunks-');
    try {
      await _withDb('${dir.path}/owner.db', (db) async {
        final journal = await AmobiUploadJournal.initialize(db);
        final payload = 'x' * (3 * 1024 * 1024 + 128 * 1024);
        await db.writeTransaction((tx) async {
          for (var i = 0; i < 2; i++) {
            await tx.execute('INSERT INTO chat_table_ez_ai_app(id,history_id,file_data) VALUES(?,?,?)',
                ['large-$i', 'session', payload]);
          }
        });
        expect((await db.getNextCrudTransaction())!.crud, hasLength(2));
        final requests = <(String, String)>[];
        expect(
            await _upload(journal, (key, body) async {
              requests.add((key, body));
              expect(utf8.encode(body).length, lessThanOrEqualTo(6 * 1024 * 1024));
              return _ack(key, (jsonDecode(body)['batch'] as List).length);
            }),
            isTrue);
        expect(requests, hasLength(2));
        expect(requests[0].$1, isNot(requests[1].$1));
        expect([
          for (final request in requests)
            for (final row in jsonDecode(request.$2)['batch'] as List) (row['data'] as Map)['id']
        ], [
          'large-0',
          'large-1'
        ]);
        expect(await db.getAll('SELECT id FROM ps_crud'), isEmpty);
      });
    } finally {
      await dir.delete(recursive: true);
    }
  });
  test('single oversized real SDK row holds queue before HTTP', () async {
    final dir = await Directory.systemTemp.createTemp('ezai-oversize-chunk-');
    try {
      await _withDb('${dir.path}/owner.db', (db) async {
        final journal = await AmobiUploadJournal.initialize(db);
        final payload = 'x' * (6 * 1024 * 1024);
        await db.execute('INSERT INTO chat_table_ez_ai_app(id,history_id,file_data) VALUES(?,?,?)',
            ['oversized', 'session', payload]);
        var calls = 0;
        await expectLater(
            _upload(journal, (key, body) async {
              calls++;
              return _ack(key, 1);
            }),
            throwsA(isA<AmobiUploadHeldException>()));
        expect(calls, 0);
        expect(await db.getAll('SELECT id FROM ps_crud'), hasLength(1));
        expect(await db.getAll('SELECT upload_key FROM amobi_ezai_upload_journal_pending'), isEmpty);
      });
    } finally {
      await dir.delete(recursive: true);
    }
  });
  test('oversized later row holds the whole SDK head before any HTTP', () async {
    final dir = await Directory.systemTemp.createTemp('ezai-later-oversize-');
    try {
      await _withDb('${dir.path}/owner.db', (db) async {
        final journal = await AmobiUploadJournal.initialize(db);
        await db.writeTransaction((tx) async {
          await tx.execute(
              'INSERT INTO chat_table_ez_ai_app(id,history_id,content) VALUES(?,?,?)', ['small', 'session', 'first']);
          await tx.execute('INSERT INTO chat_table_ez_ai_app(id,history_id,file_data) VALUES(?,?,?)',
              ['oversized', 'session', 'x' * (6 * 1024 * 1024)]);
        });
        expect((await db.getNextCrudTransaction())!.crud, hasLength(2));
        var calls = 0;
        await expectLater(
            _upload(journal, (key, body) async {
              calls++;
              return _ack(key, (jsonDecode(body)['batch'] as List).length);
            }),
            throwsA(isA<AmobiUploadHeldException>()));
        expect(calls, 0);
        expect(await db.getAll('SELECT id FROM ps_crud'), hasLength(2));
        expect(await db.getAll('SELECT upload_key FROM amobi_ezai_upload_journal_pending'), isEmpty);
      });
    } finally {
      await dir.delete(recursive: true);
    }
  });
  test('raw SDK head above 16 MiB is rejected before planning body bytes', () async {
    final dir = await Directory.systemTemp.createTemp('ezai-oversize-head-');
    try {
      await _withDb('${dir.path}/owner.db', (db) async {
        final journal = await AmobiUploadJournal.initialize(db);
        final payload = 'x' * (5 * 1024 * 1024 + 512 * 1024);
        await db.writeTransaction((tx) async {
          for (var i = 0; i < 3; i++) {
            await tx.execute('INSERT INTO chat_table_ez_ai_app(id,history_id,file_data) VALUES(?,?,?)',
                ['large-$i', 'session', payload]);
          }
        });
        var built = 0;
        var sent = 0;
        await expectLater(
            journal.upload(
              uid: 'owner-a',
              isCurrent: () => true,
              buildBody: (entries) {
                built++;
                return _body(entries);
              },
              transport: (key, body) async {
                sent++;
                return _ack(key, 1);
              },
            ),
            throwsA(isA<AmobiUploadHeldException>()));
        expect(built, 0);
        expect(sent, 0);
        expect(await db.getAll('SELECT id FROM ps_crud'), hasLength(3));
      });
    } finally {
      await dir.delete(recursive: true);
    }
  });
  test('existing frozen first chunk is unchanged when a later row cannot fit', () async {
    final dir = await Directory.systemTemp.createTemp('ezai-pending-later-oversize-');
    final file = '${dir.path}/owner.db';
    try {
      late String frozenKey;
      late String frozenBody;
      await _withDb(file, (db) async {
        await AmobiUploadJournal.initialize(db);
        await db.writeTransaction((tx) async {
          await tx.execute(
              'INSERT INTO chat_table_ez_ai_app(id,history_id,content) VALUES(?,?,?)', ['small', 'session', 'first']);
          await tx.execute('INSERT INTO chat_table_ez_ai_app(id,history_id,file_data) VALUES(?,?,?)',
              ['oversized', 'session', 'x' * (6 * 1024 * 1024)]);
        });
        final transaction = (await db.getNextCrudTransaction())!;
        final rows = await db.getAll('SELECT id,tx_id,data FROM ps_crud ORDER BY id');
        final rawHead = jsonEncode([
          for (final row in rows) {'id': row['id'], 'tx_id': row['tx_id'], 'data': row['data']}
        ]);
        frozenKey = '$amobiUploadProtocol-33333333-3333-4333-8333-333333333333';
        frozenBody = _body(transaction.crud.sublist(0, 1));
        await db.execute(
          'INSERT INTO amobi_ezai_upload_journal_pending(singleton,upload_key,uid,client_id,raw_head,body,entry_count,outcome,chunk_offset) VALUES(1,?,?,?,?,?,?,0,0)',
          [frozenKey, 'owner-a', await db.getClientId(), rawHead, utf8.encode(frozenBody), 1],
        );
      });
      await _withDb(file, (db) async {
        final journal = await AmobiUploadJournal.initialize(db);
        var calls = 0;
        await expectLater(
            _upload(journal, (key, body) async {
              calls++;
              return _ack(key, (jsonDecode(body)['batch'] as List).length);
            }),
            throwsA(isA<AmobiUploadHeldException>()));
        expect(calls, 0);
        final pending =
            await db.get('SELECT upload_key,body,outcome,chunk_offset FROM amobi_ezai_upload_journal_pending');
        expect(pending['upload_key'], frozenKey);
        expect(utf8.decode(pending['body'] as List<int>), frozenBody);
        expect(pending['outcome'], 0);
        expect(pending['chunk_offset'], 0);
        expect(await db.getAll('SELECT id FROM ps_crud'), hasLength(2));
      });
    } finally {
      await dir.delete(recursive: true);
    }
  });
  test('failed second chunk keeps its own frozen key and does not replay acknowledged first chunk', () async {
    final dir = await Directory.systemTemp.createTemp('ezai-later-chunk-failure-');
    final file = '${dir.path}/owner.db';
    try {
      late String secondKey;
      late String secondBody;
      await _withDb(file, (db) async {
        final journal = await AmobiUploadJournal.initialize(db);
        await _seedLongDelete(db);
        var calls = 0;
        await expectLater(
            _upload(journal, (key, payload) async {
              calls++;
              if (calls == 1) return _ack(key, 100);
              secondKey = key;
              secondBody = payload;
              return http.Response('unavailable', 503);
            }),
            throwsA(isA<AmobiUploadTransportException>()));
        expect(calls, 2);
        expect((jsonDecode(secondBody)['batch'] as List), hasLength(3));
        final pending = await db
            .get('SELECT upload_key,body,entry_count,chunk_offset,outcome FROM amobi_ezai_upload_journal_pending');
        expect(pending['upload_key'], secondKey);
        expect(utf8.decode(pending['body'] as List<int>), secondBody);
        expect(pending['chunk_offset'], 100);
        expect(pending['outcome'], 0);
        expect(await db.getAll('SELECT id FROM ps_crud'), hasLength(103));
      });
      await _withDb(file, (db) async {
        final journal = await AmobiUploadJournal.initialize(db);
        var calls = 0;
        expect(
            await _upload(journal, (key, payload) async {
              calls++;
              expect(key, secondKey);
              expect(payload, secondBody);
              return _ack(key, 3);
            }),
            isTrue);
        expect(calls, 1);
        expect(await db.getAll('SELECT id FROM ps_crud'), isEmpty);
      });
    } finally {
      await dir.delete(recursive: true);
    }
  });
  test('existing .1 frozen single-head key and bytes migrate without reminting', () async {
    final dir = await Directory.systemTemp.createTemp('ezai-one-head-migrate-');
    final file = '${dir.path}/owner.db';
    const key = '$amobiUploadProtocol-11111111-1111-4111-8111-111111111111';
    late String frozen;
    try {
      await _withDb(file, (db) async {
        await db
            .execute('INSERT INTO history_table_ez_ai_app(id,history_name) VALUES(?,?)', ['old-pending', 'Original']);
        final transaction = (await db.getNextCrudTransaction())!;
        final rows = await db.getAll('SELECT id,tx_id,data FROM ps_crud ORDER BY id');
        final rawHead = jsonEncode([
          for (final row in rows) {'id': row['id'], 'tx_id': row['tx_id'], 'data': row['data']}
        ]);
        frozen = _body(transaction.crud);
        await db.execute(
            'CREATE TABLE amobi_ezai_upload_journal_state(singleton INTEGER PRIMARY KEY,legacy_held INTEGER NOT NULL)');
        await db.execute('INSERT INTO amobi_ezai_upload_journal_state VALUES(1,0)');
        await db.execute(
            'CREATE TABLE amobi_ezai_upload_journal_pending(singleton INTEGER PRIMARY KEY,upload_key TEXT NOT NULL UNIQUE,uid TEXT NOT NULL,client_id TEXT NOT NULL,raw_head TEXT NOT NULL,body BLOB NOT NULL,entry_count INTEGER NOT NULL,outcome INTEGER NOT NULL)');
        await db.execute('INSERT INTO amobi_ezai_upload_journal_pending VALUES(1,?,?,?,?,?,?,0)',
            [key, 'owner-a', await db.getClientId(), rawHead, utf8.encode(frozen), 1]);
      });
      await _withDb(file, (db) async {
        final journal = await AmobiUploadJournal.initialize(db);
        final pending = await db.get('SELECT upload_key,body,chunk_offset FROM amobi_ezai_upload_journal_pending');
        expect(pending['upload_key'], key);
        expect(utf8.decode(pending['body'] as List<int>), frozen);
        expect(pending['chunk_offset'], 0);
        expect(
            await _upload(journal, (k, payload) async {
              expect(k, key);
              expect(payload, frozen);
              return _ack(k, 1);
            }),
            isTrue);
        expect(await db.getAll('SELECT id FROM ps_crud'), isEmpty);
      });
    } finally {
      await dir.delete(recursive: true);
    }
  });
  test('existing .1 oversized frozen head stays held with original key and bytes', () async {
    final dir = await Directory.systemTemp.createTemp('ezai-old-large-head-');
    final file = '${dir.path}/owner.db';
    const key = '$amobiUploadProtocol-22222222-2222-4222-8222-222222222222';
    late String frozen;
    try {
      await _withDb(file, (db) async {
        await _seedLongDelete(db);
        final transaction = (await db.getNextCrudTransaction())!;
        final rows = await db.getAll('SELECT id,tx_id,data FROM ps_crud ORDER BY id');
        final rawHead = jsonEncode([
          for (final row in rows) {'id': row['id'], 'tx_id': row['tx_id'], 'data': row['data']}
        ]);
        frozen = _body(transaction.crud);
        await db.execute(
            'CREATE TABLE amobi_ezai_upload_journal_state(singleton INTEGER PRIMARY KEY,legacy_held INTEGER NOT NULL)');
        await db.execute('INSERT INTO amobi_ezai_upload_journal_state VALUES(1,0)');
        await db.execute(
            'CREATE TABLE amobi_ezai_upload_journal_pending(singleton INTEGER PRIMARY KEY,upload_key TEXT NOT NULL UNIQUE,uid TEXT NOT NULL,client_id TEXT NOT NULL,raw_head TEXT NOT NULL,body BLOB NOT NULL,entry_count INTEGER NOT NULL,outcome INTEGER NOT NULL)');
        await db.execute('INSERT INTO amobi_ezai_upload_journal_pending VALUES(1,?,?,?,?,?,?,0)',
            [key, 'owner-a', await db.getClientId(), rawHead, utf8.encode(frozen), 103]);
      });
      await _withDb(file, (db) async {
        final journal = await AmobiUploadJournal.initialize(db);
        var calls = 0;
        await expectLater(
            _upload(journal, (k, body) async {
              calls++;
              return _ack(k, 103);
            }),
            throwsA(isA<AmobiUploadHeldException>()));
        expect(calls, 0);
        final pending =
            await db.get('SELECT upload_key,body,entry_count,chunk_offset FROM amobi_ezai_upload_journal_pending');
        expect(pending['upload_key'], key);
        expect(utf8.decode(pending['body'] as List<int>), frozen);
        expect(pending['entry_count'], 103);
        expect(pending['chunk_offset'], 0);
        expect(await db.getAll('SELECT id FROM ps_crud'), hasLength(103));
      });
    } finally {
      await dir.delete(recursive: true);
    }
  });
  test('103 real EzAI deletes in one SDK head resume first lost ACK and finish exact chunks', () async {
    final dir = await Directory.systemTemp.createTemp('ezai-chunk-journal-');
    final file = '${dir.path}/owner.db';
    try {
      late String firstKey;
      late String firstBody;
      await _withDb(file, (db) async {
        final journal = await AmobiUploadJournal.initialize(db);
        await _seedLongDelete(db);
        await expectLater(
            _upload(journal, (key, payload) async {
              firstKey = key;
              firstBody = payload;
              throw const SocketException('ACK lost');
            }),
            throwsA(isA<AmobiUploadTransportException>()));
        expect((jsonDecode(firstBody)['batch'] as List), hasLength(100));
        expect(await db.getAll('SELECT id FROM ps_crud'), hasLength(103));
      });
      await _withDb(file, (db) async {
        final journal = await AmobiUploadJournal.initialize(db);
        final requests = <(String, String)>[];
        expect(
            await _upload(journal, (key, payload) async {
              requests.add((key, payload));
              if (requests.length == 1) {
                expect(key, firstKey);
                expect(payload, firstBody);
              }
              return _ack(key, (jsonDecode(payload)['batch'] as List).length);
            }),
            isTrue);
        expect(requests.map((request) => (jsonDecode(request.$2)['batch'] as List).length), [100, 3]);
        expect(requests[0].$1, isNot(requests[1].$1));
        final ids = [
          for (final request in requests)
            for (final entry in jsonDecode(request.$2)['batch'] as List) (entry['data'] as Map)['id']
        ];
        expect(ids.toSet(), hasLength(103));
        expect(await db.getAll('SELECT id FROM ps_crud'), isEmpty);
      });
    } finally {
      await dir.delete(recursive: true);
    }
  });
}
