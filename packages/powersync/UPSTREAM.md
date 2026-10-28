# EzAI Flutter client fork provenance

Sources were copied from the exact pub.dev packages resolved by EzAI new\'s `pubspec.lock` on 2026-10-28. The package roots are permanent maintained source, not generated cache output.

| Package | Version | pub.dev archive SHA-256 | Fork path |
| --- | --- | --- | --- |
| `powersync` | `1.18.0` | `2c952a0d933b23504092f9ee8f8eb4f651cf196e86afa69521de84e301cb0002` | `.` |
| `powersync_core` | `1.8.0` | `74422b5c66cdd749155380df9bd994591a23a5373ae3163516f47123963da94d` | `core/` |
| `powersync_flutter_libs` | `0.4.15+1` | `3836fd9b4f351aae3229eed1fb0346a2dffa9151792c90f83682e6508ff4df0b` | pinned hosted native plugin |

Upstream repository: https://github.com/powersync-ja/powersync.dart

Maintained Amobi repository:
`https://github.com/nxnguyen/amobi-sync-dart`, tag
`amobi-1.18.0-core1.8.0.3`. The exact commit is recorded in
`clients/GITHUB_FORKS.json` in the maintained server repository.
Git package paths are `packages/powersync` and `packages/powersync_core`;
the maintained local mirrors remain `clients/flutter_1_18` and its `core/`.
Package repository metadata points here; upstream attribution stays above.

Dart runtime/test sources differ only in package namespace references. `bin/setup_web.dart` retains the upstream CLI, with core asset selection recognizing `amobi_sync_flutter` and stripping fork build metadata to select the upstream 1.18.0 assets. The native plugin stays at 0.4.15+1. Protocol, schema, queue, checkpoint, and migration code are unchanged. Upstream LICENSE and NOTICE are retained in both package roots.

`lib/amobi_upload_journal.dart` and its two path helpers are an additional
EzAI upload extension. They persist the frozen request and SDK queue head in
raw SQLite metadata before HTTP, and require a durable echoed ACK before SDK
queue completion. These files were adapted from the Money journal, but use
EzAI-specific metadata table and protocol names, a 6 MiB body bound, a 16 MiB
raw-head bound, and an exact all-applied ACK. Money's fork and app are unchanged.

The `.2` extension splits one frozen SDK head into requests of at most 100 CRUD
entries and 6 MiB. Each chunk has its own durable UUID/body and advances only
after an exact ACK; the SDK head retires after the last chunk. Server atomicity
is per HTTP request. Existing `.1` frozen requests retain their original key and
bytes; an old oversized frozen request stays held for reconciliation. The old
`.1` journal refuses a new partial-head record before HTTP. The `.2` Git candidate passed real Rust HTTP acceptance, including a 103-entry
SDK transaction, a lost ACK, exact replay after reopen, and a fresh reader.
A separate `.1`-pinned package verified that both pending chunk states remain
held with unchanged key/body/queue and zero HTTP after downgrade.

The same-file test proves an upstream 1.18.0 client can reopen a file containing
the fork's pending metadata without deleting it. It does not authorize upstream
uploads after rollback. An older client must remain offline while pending
outcomes are reconciled; new writes made by an upstream client after the fork's
state marker exists cannot be distinguished from fork writes by this journal.

`test/amobi_upload_journal_chunk_test.dart` uses a minimal SDK schema with the
EzAI chat/history table names and only the columns exercised by its SQLite
transactions. The separate EzAI New repository test uses the actual app schema
and confirms that deleting 102 messages plus their session creates 103 SDK CRUD
entries. The package test needs PowerSync SQLite core 0.4.11 on macOS ARM64
(SHA-256 `43d6cc2d7df407e1936b95610e54f2a59aaf0663184eec8691c074b16f53d895`).
Set `POWERSYNC_EXTENSION` to that dylib when running from a standalone checkout;
the maintained Work checkout defaults to `../dart/.native/libpowersync.dylib`.
The app's pinned Flutter native plugin remains `0.4.15+1`.

The `.3` journal validates the complete bounded SDK head and all remaining
chunk bodies before sending or retrying HTTP. A later single row above 6 MiB
therefore holds the queue before any new partial remote write. Planned bodies
are capped at 24 MiB, and the raw head is checked before planning. An existing
frozen pending key/body remains unchanged if a later row cannot fit. Already
partially accepted `.2` heads with an oversized row still require reconciliation;
this change does not infer a rollback of remote writes. Ten real SDK SQLite
regressions, including both oversize paths and the raw-head bound, pass.
