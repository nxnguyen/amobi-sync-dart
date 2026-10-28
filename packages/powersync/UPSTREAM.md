# EzAI Flutter client fork provenance

Sources were copied from the exact pub.dev packages resolved by EzAI new\'s `pubspec.lock` on 2026-10-28. The package roots are permanent maintained source, not generated cache output.

| Package | Version | pub.dev archive SHA-256 | Fork path |
| --- | --- | --- | --- |
| `powersync` | `1.18.0` | `2c952a0d933b23504092f9ee8f8eb4f651cf196e86afa69521de84e301cb0002` | `.` |
| `powersync_core` | `1.8.0` | `74422b5c66cdd749155380df9bd994591a23a5373ae3163516f47123963da94d` | `core/` |
| `powersync_flutter_libs` | `0.4.15+1` | `3836fd9b4f351aae3229eed1fb0346a2dffa9151792c90f83682e6508ff4df0b` | pinned hosted native plugin |

Upstream repository: https://github.com/powersync-ja/powersync.dart

Maintained Amobi repository:
`https://github.com/nxnguyen/amobi-sync-dart`,
package paths `packages/powersync` and `packages/powersync_core`.
Package repository metadata points here; upstream attribution stays above.

Dart runtime/test sources differ only in package namespace references. `bin/setup_web.dart` retains the upstream CLI, with core asset selection recognizing `amobi_sync_flutter` and stripping fork build metadata to select the upstream 1.18.0 assets. The native plugin stays at 0.4.15+1. Protocol, schema, queue, checkpoint, and migration code are unchanged. Upstream LICENSE and NOTICE are retained in both package roots.

`lib/amobi_upload_journal.dart` and its two path helpers are an additional
EzAI upload extension. They persist the frozen request and SDK queue head in
raw SQLite metadata before HTTP, and require a durable echoed ACK before SDK
queue completion. These files were adapted from the Money journal, but use
EzAI-specific metadata table and protocol names, a 6 MiB body bound, a 16 MiB
raw-head bound, and an exact all-applied ACK. Money's fork and app are unchanged.

The same-file test proves an upstream 1.18.0 client can reopen a file containing
the fork's pending metadata without deleting it. It does not authorize upstream
uploads after rollback. An older client must remain offline while pending
outcomes are reconciled; new writes made by an upstream client after the fork's
state marker exists cannot be distinguished from fork writes by this journal.
