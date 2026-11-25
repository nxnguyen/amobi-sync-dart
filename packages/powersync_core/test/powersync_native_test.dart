@TestOn('!browser')
library;

import 'dart:async';

import 'package:amobi_sync_core/powersync_core.dart';
import 'package:sqlite3/common.dart';
import 'package:sqlite_async/sqlite_async.dart';
import 'package:test/test.dart';
import 'utils/abstract_test_utils.dart';
import 'utils/test_utils_impl.dart';

final testUtils = TestUtils();

void main() {
  group('Basic Tests', () {
    late String path;

    setUp(() async {
      path = testUtils.dbPath();
      await testUtils.cleanDb(path: path);
    });

    tearDown(() async {
      await testUtils.cleanDb(path: path);
    });

    test('Basic Setup', () async {
      final db = await testUtils.setupPowerSync(path: path);
      await db.execute(
          'INSERT INTO assets(id, make) VALUES(uuid(), ?)', ['Test Make']);
      final result = await db.get('SELECT make FROM assets');
      expect(result, equals({'make': 'Test Make'}));
      expect(
          await db.execute('PRAGMA journal_mode'),
          equals([
            {'journal_mode': 'wal'}
          ]));
      expect(
          await db.execute('PRAGMA locking_mode'),
          equals([
            {'locking_mode': 'normal'}
          ]));
    });

    test('PowerSync extension exposes UUID and diff functions', () async {
      final db = await testUtils.setupPowerSync(path: path);
      final row = await db.get(
        'SELECT uuid() AS id, powersync_diff(?, ?) AS diff',
        ['{"a":1}', '{"a":2,"b":3}'],
      );
      expect(row['id'], isA<String>());
      expect(row['diff'], '{"a":2,"b":3}');
      final bucketColumns = await db.getAll('PRAGMA table_info(ps_buckets)');
      expect(
          bucketColumns.map((column) => column['name']), contains('target_op'));
    });

    // Manual test
    test('Concurrency', () async {
      final db = PowerSyncDatabase.withFactory(
        (await testUtils.testFactory(
                path: path, options: const SqliteOptions(maxReaders: 3)))
            .sqliteFactory,
        schema: defaultSchema,
      );
      await db.initialize();

      final futures = [
        for (var i = 1; i <= 11; i++) db.get('SELECT ? as i', [i]),
      ];
      expect(await Future.wait(futures), hasLength(11));
    });

    test('read-only transactions', () async {
      final db = await testUtils.setupPowerSync(path: path);

      // Can read
      await db.getAll("WITH test AS (SELECT 1 AS one) SELECT * FROM test");

      // Cannot write
      await expectLater(() async {
        await db.getAll('INSERT INTO assets(id) VALUES(?)', ['test']);
      },
          throwsA((dynamic e) =>
              e is SqliteException &&
              e.message.contains('attempt to write a readonly database')));

      // Can use WITH ... SELECT
      await db.getAll("WITH test AS (SELECT 1 AS one) SELECT * FROM test");

      // Cannot use WITH .... INSERT
      await expectLater(() async {
        await db.getAll(
            "WITH test AS (SELECT 1 AS one) INSERT INTO assets(id) SELECT one FROM test");
      },
          throwsA((dynamic e) =>
              e is SqliteException &&
              e.message.contains('attempt to write a readonly database')));

      await db.writeTransaction((tx) async {
        // Within a write transaction, this is fine
        await tx
            .getAll('INSERT INTO assets(id) VALUES(?) RETURNING *', ['test']);
      });
    });

    test(
        'should allow read-only db calls within transaction callback in separate zone',
        () async {
      final db = await testUtils.setupPowerSync(path: path);

      // Get a reference to the parent zone (outside the transaction).
      final zone = Zone.current;

      // Each of these are fine, since it could use a separate connection.
      // Note: In highly concurrent cases, it could exhaust the connection pool and cause a deadlock.

      await db.writeTransaction((tx) async {
        // Use the parent zone to avoid the "recursive lock" error.
        await zone.fork().run(() async {
          await db.getAll('SELECT * FROM assets');
        });
      });

      await db.readTransaction((tx) async {
        await zone.fork().run(() async {
          await db.getAll('SELECT * FROM assets');
        });
      });

      await db.readTransaction((tx) async {
        await zone.fork().run(() async {
          await db.execute('SELECT * FROM assets');
        });
      });

      // Note: This would deadlock, since it shares a global write lock.
      // await db.writeTransaction((tx) async {
      //   await zone.fork().run(() async {
      //     await db.execute('SELECT * FROM test_data');
      //   });
      // });
    });
  });
}
