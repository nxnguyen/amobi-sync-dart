# Flutter client fork provenance

The Dart sources were copied from the exact package releases resolved by Money's
`pubspec.lock` before this fork was created:

| Package | Upstream version | pub.dev archive SHA-256 | Fork path |
| --- | --- | --- | --- |
| `powersync` | `1.16.2` | `6856132dcb41a7fa35721d616f4256a9b923be2041ddf186f7c8eea11518cada` | package root (`.`) |
| `powersync_core` | `1.7.0` | `20ff37895eff15c8f9791c76b90364c4a1cb18c55dc50d96024e44507d435e2e` | `core/` |
| `powersync_flutter_libs` | `0.4.15+1` | `3836fd9b4f351aae3229eed1fb0346a2dffa9151792c90f83682e6508ff4df0b` | retained as pinned native plugin dependency; not forked |

Upstream repository: <https://github.com/powersync-ja/powersync.dart>

The first Amobi fork revision changes only the Dart package names and internal
package imports, plus Amobi package metadata and documentation. Sync protocol,
database schema/format, upload queue, checkpoint handling, and migration logic
remain byte-for-byte equivalent at the Dart source level apart from those
namespace replacements. The native extension is still the upstream pinned
PowerSync Flutter plugin; this fork does not claim a replacement native engine.

Upstream `LICENSE` and `NOTICE` files are retained in both package roots. The
copied source is under Apache License 2.0 and carries Journey Mobile copyright
notices. Review the retained license and NOTICE before distributing changes.

The Flutter package also retains upstream `bin/setup_web.dart` with its core
package import renamed for this fork, plus the byte-identical upstream
`analysis_options.yaml`. The copied core Web setup selects
`amobi_sync_flutter` from package resolution and strips the fork build metadata
when requesting the pinned upstream asset release. Its original `powersync`
package and no-package fallbacks remain available. `--help` exits before any
asset download.
