import 'dart:ffi';
import 'dart:io' as io;
import 'dart:math';

import 'package:sqlite3/sqlite3.dart' as sqlite;
import 'package:sqlite_async/native.dart';
import 'package:sqlite_async/sqlite_async.dart';

import 'sqlite3_powersync_init.dart';
import '../powersync_sqlite_options.dart';

var _extensionLoaded = false;

SqliteOpenFactory powerSyncOpenFactory(String path, SqliteOptions options) =>
    NativePowerSyncOpenFactory(path: path, sqliteOptions: options);

/// Native PowerSync database factory with the PowerSync SQLite extension.
base class NativePowerSyncOpenFactory extends NativeSqliteOpenFactory {
  NativePowerSyncOpenFactory({
    required super.path,
    super.sqliteOptions = powerSyncDefaultSqliteOptions,
  });

  void enableExtension() {
    if (_extensionLoaded) return;
    final entrypoint = Native.addressOf<NativeFunction<ExtensionEntrypoint>>(
      sqlite3_powersync_init,
    );
    sqlite.sqlite3.ensureExtensionLoaded(
      sqlite.SqliteExtension(entrypoint.cast()),
    );
    _extensionLoaded = true;
  }

  @override
  List<String> pragmaStatements(SqliteOpenOptions options) => [
        ...super.pragmaStatements(options),
        'PRAGMA recursive_triggers = TRUE',
      ];

  @override
  sqlite.Database openNativeConnection(SqliteOpenOptions options) {
    enableExtension();

    final stopwatch = Stopwatch()..start();
    var retryDelay = 2;
    while (stopwatch.elapsedMilliseconds < 500) {
      try {
        return super.openNativeConnection(options);
      } on sqlite.SqliteException catch (e) {
        if (e.resultCode != 5) rethrow;
        io.sleep(Duration(milliseconds: retryDelay));
        retryDelay = min(retryDelay * 2, 16);
      }
    }
    throw StateError('Timed out opening PowerSync SQLite connection');
  }
}
