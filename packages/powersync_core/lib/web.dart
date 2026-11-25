/// Internal options used to customize how PowerSync opens databases on the web.
library;

export 'src/web/worker_utils.dart' show PowerSyncAdditionalOpenOptions;
export 'package:sqlite3_web/sqlite3_web.dart';
export 'package:sqlite_async/web.dart';

import 'package:sqlite_async/web.dart';
import 'src/open_factory/web/web_open_factory.dart';

/// The default [SqliteOpenFactory] implementation for the web. Unlike
/// the cross-platform interface, this is guaranteed to implement
/// [WebSqliteOpenFactory].
///
/// This typedef is mostly used internally, e.g. in the web implementation of
/// `powersync_sqlcipher` which relies on the fact that web-specific factory
/// methods are available.
typedef PowerSyncWebOpenFactory = WebPowerSyncOpenFactory;
