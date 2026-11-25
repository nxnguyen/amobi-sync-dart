import 'dart:async';
import 'dart:io';
import 'package:amobi_sync_core/native.dart';
import 'package:sqlite3/common.dart';
import 'package:sqlite_async/sqlite_async.dart';

import 'abstract_test_utils.dart';

base class TestOpenFactory extends NativePowerSyncOpenFactory
    with TestPowerSyncFactory {
  TestOpenFactory({required super.path, super.sqliteOptions});

  @override
  Future<CommonDatabase> openRawInMemoryDatabase() async {
    return TestOpenFactory(path: ':memory:').openNativeConnection(
      const SqliteOpenOptions(primaryConnection: true, readOnly: false),
    );
  }
}

class TestUtils extends AbstractTestUtils {
  @override
  String dbPath() {
    Directory("test-db").createSync(recursive: false);
    return super.dbPath();
  }

  @override
  Future<void> cleanDb({required String path}) async {
    try {
      await File(path).delete();
    } on PathNotFoundException {
      // Not an issue
    }
    try {
      await File("$path-shm").delete();
    } on PathNotFoundException {
      // Not an issue
    }
    try {
      await File("$path-wal").delete();
    } on PathNotFoundException {
      // Not an issue
    }
  }

  @override
  Future<TestOpenFactory> testFactory(
      {String? path,
      String sqlitePath = '',
      SqliteOptions options = const SqliteOptions()}) async {
    return TestOpenFactory(path: path ?? dbPath(), sqliteOptions: options);
  }
}
