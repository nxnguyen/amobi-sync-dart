import 'dart:async';

import 'package:logging/logging.dart';
import 'package:meta/meta.dart';
import 'package:amobi_sync_core/sqlite_async.dart';
import 'package:amobi_sync_core/src/abort_controller.dart';
import '../sync/options.dart';
import '../sync/streaming_sync.dart';
import 'powersync_database.dart';

import '../connector.dart';
import '../schema.dart';

base class PowerSyncDatabaseImpl extends PowerSyncDatabase {
  @override
  Future<void> close() {
    throw UnimplementedError();
  }

  @override
  bool get closed => throw UnimplementedError();

  @override
  Schema get schema => throw UnimplementedError();

  @override
  set schema(Schema s) => throw UnimplementedError();

  @override
  SqliteDatabase get database => throw UnimplementedError();

  @override
  Future<void> get isInitialized => throw UnimplementedError();

  /// Open a [PowerSyncDatabase].
  ///
  /// Only a single [PowerSyncDatabase] per [path] should be opened at a time.
  ///
  /// The specified [schema] is used for the database.
  ///
  /// A connection pool is used by default, allowing multiple concurrent read
  /// transactions, and a single concurrent write transaction. Write transactions
  /// do not block read transactions, and read transactions will see the state
  /// from the last committed write transaction.
  ///
  /// The maximum number of concurrent readers is configured with
  /// [SqliteOptions.maxReaders].
  ///
  /// [logger] defaults to [autoLogger], which logs to the console in debug builds.
  factory PowerSyncDatabaseImpl(
      {required Schema schema,
      required String path,
      Logger? logger,
      SqliteOptions sqliteOptions = const SqliteOptions()}) {
    throw UnimplementedError();
  }

  /// Open a [PowerSyncDatabase] with a [SqliteOpenFactory].
  ///
  /// The factory determines which database file is opened, as well as any
  /// additional logic to run inside the database isolate before or after opening.
  ///
  /// Subclass the platform-specific [SqliteOpenFactory] to add custom logic.
  ///
  /// [logger] defaults to [autoLogger], which logs to the console in debug builds.s
  factory PowerSyncDatabaseImpl.withFactory(
    SqliteOpenFactory openFactory, {
    required Schema schema,
    Logger? logger,
  }) {
    throw UnimplementedError();
  }

  /// Open a PowerSyncDatabase on an existing [SqliteDatabase].
  ///
  /// Migrations are run on the database when this constructor is called.
  ///
  /// [logger] defaults to [autoLogger], which logs to the console in debug builds.s
  factory PowerSyncDatabaseImpl.withDatabase({
    required Schema schema,
    required SqliteDatabase database,
    Logger? logger,
  }) {
    throw UnimplementedError();
  }

  @override
  Future<T> readLock<T>(Future<T> Function(SqliteReadContext tx) callback,
      {String? debugContext, Duration? lockTimeout}) {
    throw UnimplementedError();
  }

  @override
  Future<T> writeLock<T>(Future<T> Function(SqliteWriteContext tx) callback,
      {String? debugContext, Duration? lockTimeout}) {
    throw UnimplementedError();
  }

  @override
  Future<bool> getAutoCommit() {
    throw UnimplementedError();
  }

  @override
  Logger get logger => throw UnimplementedError();

  @override
  @internal
  Future<void> connectInternal({
    required PowerSyncBackendConnector connector,
    required AbortController abort,
    required List<SubscribedStream> initiallyActiveStreams,
    required Stream<List<SubscribedStream>> activeStreams,
    required Zone asyncWorkZone,
    required ResolvedSyncOptions options,
  }) {
    throw UnimplementedError();
  }

  @override
  Future<void> refreshSchema() {
    throw UnimplementedError();
  }
}
