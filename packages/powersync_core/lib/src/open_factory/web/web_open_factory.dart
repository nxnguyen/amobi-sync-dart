import 'dart:async';

import 'package:sqlite3_web/sqlite3_web.dart';
import 'package:sqlite_async/sqlite_async.dart';
import 'package:sqlite_async/web.dart';

import '../../web/worker_utils.dart';
import '../powersync_sqlite_options.dart';

SqliteOpenFactory powerSyncOpenFactory(String path, SqliteOptions options) =>
    WebPowerSyncOpenFactory(path: path, sqliteOptions: options);

/// Web PowerSync database factory.
base class WebPowerSyncOpenFactory extends WebSqliteOpenFactory {
  WebPowerSyncOpenFactory({
    required super.path,
    super.sqliteOptions = powerSyncDefaultSqliteOptions,
  });

  @override
  Future<WebSqlite> openWebSqlite(WebSqliteOptions options) async {
    return WebSqlite.open(
      wasmModule: sqliteOptions.webSqliteOptions.wasmUri,
      workers: WorkerConnector.defaultWorkers(
          sqliteOptions.webSqliteOptions.workerUri),
      controller: PowerSyncAsyncSqliteController(),
      handleCustomRequest: handleCustomRequest,
    );
  }

  @override
  Future<ConnectToRecommendedResult> connectToWorker(
      WebSqlite sqlite, String name) {
    return sqlite.connectToRecommended(
      name,
      preparedStatementCacheSize:
          sqliteOptions.preparedStatementCacheSize,
    );
  }
}
