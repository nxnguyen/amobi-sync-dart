import 'package:sqlite_async/sqlite_async.dart';

const powerSyncDefaultSqliteOptions = SqliteOptions(
  webSqliteOptions: WebSqliteOptions(
    wasmUri: 'sqlite3.wasm',
    workerUri: 'powersync_db.worker.js',
  ),
);
