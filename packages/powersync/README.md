# Amobi Sync Flutter client

This package is the Amobi-maintained Dart client fork used by Money. It is based
on the pinned PowerSync Flutter SDK 1.16.2 and core 1.7.0. The initial fork keeps
the existing API and sync implementation; changes are limited to package names,
internal imports and package metadata.

Money continues using its existing SQLite database, schema, CRUD queue, sync
protocol, Firebase auth connector, and migration path. The app dependency changes
from `powersync` to this local package. The native SQLite extension remains the
exact `powersync_flutter_libs` 0.4.15+1 dependency so existing database and queue
behavior stays compatible.

Source versions, archive checksums, licenses, and fork boundaries are recorded in
[`UPSTREAM.md`](UPSTREAM.md). Preserve the Apache license and NOTICE files when
editing or redistributing this source.
