import "dart:async";
import "dart:convert";

import "package:amobi_sync_flutter/powersync.dart";
import "package:amobi_sync_flutter/sqlite_async.dart" show SqliteWriteContext;
import "package:http/http.dart" as http;
import "package:uuid/uuid.dart";

// This source is imported directly by the standalone upstream/fork acceptance harness.
// ignore: always_use_package_imports
import "amobi_upload_path_web.dart" if (dart.library.io) "amobi_upload_path_io.dart" as upload_path;

const amobiUploadProtocol = "amobi-ezai-upload-v1";
const _stateTable = "amobi_ezai_upload_journal_state";
const _pendingTable = "amobi_ezai_upload_journal_pending";
const _maxEntries = 5000;
const _maxBatchEntries = 100;
const int _maxBodyBytes = 6 * 1024 * 1024;
const int _maxHeadBytes = 16 * 1024 * 1024;
// At most 16 MiB raw SDK head plus 8 MiB for per-entry owner/envelope fields.
const int _maxPlannedBytes = _maxHeadBytes + 8 * 1024 * 1024;
final _uploadKeyPattern = RegExp(
  r"^amobi-ezai-upload-v1-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
);

enum AmobiUploadHoldReason {
  legacyQueue,
  headChanged,
  identityChanged,
  ambiguousAck,
  capacity,
  invalidState,
  destructiveClear,
}

class AmobiUploadHeldException implements Exception {
  const AmobiUploadHeldException(this.reason);
  final AmobiUploadHoldReason reason;

  @override
  String toString() => "Amobi upload held: ${reason.name}";
}

class AmobiUploadTransportException implements Exception {
  const AmobiUploadTransportException([this.statusCode]);
  final int? statusCode;

  @override
  String toString() => "Amobi upload not acknowledged${statusCode == null ? "" : ": $statusCode"}";
}

class AmobiUploadSessionChangedException implements Exception {
  const AmobiUploadSessionChangedException();
}

/// A single durable outbox head for a PowerSync database file. Initialize before
/// any app or migration writes. Existing unmarked queues are permanently held.
final class AmobiUploadJournal {
  AmobiUploadJournal._(this.database, this._path, this.legacyHeld);

  final PowerSyncDatabase database;
  final String _path;
  final bool legacyHeld;
  static final Map<String, Future<void>> _owners = {};

  static Future<AmobiUploadJournal> initialize(PowerSyncDatabase database) async {
    await database.initialize();
    final path = await upload_path.canonicalUploadPath(database.database.openFactory.path);
    await database.writeTransaction((tx) async {
      await tx.execute(
        "CREATE TABLE IF NOT EXISTS $_stateTable("
        "singleton INTEGER PRIMARY KEY CHECK(singleton=1), legacy_held INTEGER NOT NULL CHECK(legacy_held IN (0,1)))",
      );
      await tx.execute(
        "CREATE TABLE IF NOT EXISTS $_pendingTable("
        "singleton INTEGER PRIMARY KEY CHECK(singleton=1), upload_key TEXT NOT NULL UNIQUE,"
        "uid TEXT NOT NULL, client_id TEXT NOT NULL, raw_head TEXT NOT NULL,"
        "body BLOB NOT NULL, entry_count INTEGER NOT NULL, outcome INTEGER NOT NULL CHECK(outcome IN (0,1,2)),"
        "chunk_offset INTEGER NOT NULL DEFAULT 0)",
      );
      final columns = await tx.execute("PRAGMA table_info($_pendingTable)");
      if (!columns.any((column) => column["name"] == "chunk_offset")) {
        await tx.execute("ALTER TABLE $_pendingTable ADD COLUMN chunk_offset INTEGER NOT NULL DEFAULT 0");
      }
      final initialized = await tx.execute("SELECT legacy_held FROM $_stateTable WHERE singleton=1");
      if (initialized.isEmpty) {
        final queue = await tx.execute("SELECT id FROM ps_crud LIMIT 1");
        await tx.execute("INSERT INTO $_stateTable(singleton,legacy_held) VALUES(1,?)", [queue.isEmpty ? 0 : 1]);
      }
    });
    final marker = await database.get(
      "SELECT legacy_held,typeof(legacy_held) AS value_type FROM $_stateTable WHERE singleton=1",
    );
    if (marker["value_type"] != "integer" || marker["legacy_held"] != 0 && marker["legacy_held"] != 1) {
      throw const AmobiUploadHeldException(AmobiUploadHoldReason.invalidState);
    }
    return AmobiUploadJournal._(database, path, marker["legacy_held"] == 1);
  }

  Future<bool> hasPendingOrHeld() => _serialize(_hasPendingOrHeld);

  Future<bool> _hasPendingOrHeld() async {
    if (legacyHeld) return true;
    final row = await database.get(
      "SELECT EXISTS(SELECT 1 FROM $_pendingTable) AS pending, "
      "EXISTS(SELECT 1 FROM ps_crud) AS queued",
    );
    return row["pending"] == 1 || row["queued"] == 1;
  }

  Future<void> guardDestructiveClear() => _serialize(() async {
        if (await _hasPendingOrHeld()) throw const AmobiUploadHeldException(AmobiUploadHoldReason.destructiveClear);
      });

  /// Call only after an explicitly acknowledged account deletion and a
  /// successful SDK disconnectAndClear. The caller must initialize a new
  /// journal before any further local write.
  Future<void> eraseAfterAccountDeletion() async {
    await _serialize(() async {
      final queue = await database.getAll("SELECT id FROM ps_crud LIMIT 1");
      if (queue.isNotEmpty) throw const AmobiUploadHeldException(AmobiUploadHoldReason.destructiveClear);
      await database.writeTransaction((tx) async {
        await tx.execute("DELETE FROM $_pendingTable");
        await tx.execute("DELETE FROM $_stateTable");
      });
    });
  }

  /// Returns false for an empty queue, true after a durable ACK and SDK completion.
  /// The transport must send the supplied body byte-for-byte as UTF-8 with its key in the
  /// Idempotency-Key header. Its response is checked here before queue removal.
  Future<bool> upload({
    required String uid,
    required String Function(List<CrudEntry>) buildBody,
    required Future<http.Response> Function(String key, String body) transport,
    required bool Function() isCurrent,
  }) =>
      _serialize(
        () => _upload(
          uid: uid,
          buildBody: buildBody,
          transport: transport,
          isCurrent: isCurrent,
        ),
      );

  Future<T> _serialize<T>(Future<T> Function() operation) async {
    final previous = _owners[_path];
    final done = Completer<void>();
    _owners[_path] = done.future;
    try {
      if (previous != null) await previous;
      return await operation();
    } finally {
      done.complete();
      if (identical(_owners[_path], done.future)) _owners.remove(_path);
    }
  }

  Future<bool> _upload({
    required String uid,
    required String Function(List<CrudEntry>) buildBody,
    required Future<http.Response> Function(String key, String body) transport,
    required bool Function() isCurrent,
  }) async {
    void current() {
      if (!isCurrent()) throw const AmobiUploadSessionChangedException();
    }

    current();
    if (legacyHeld) throw const AmobiUploadHeldException(AmobiUploadHoldReason.legacyQueue);
    final clientId = await database.getClientId();
    if (uid.isEmpty || clientId.isEmpty || utf8.encode(uid).length > 512 || utf8.encode(clientId).length > 128) {
      throw const AmobiUploadHeldException(AmobiUploadHoldReason.identityChanged);
    }
    current();
    var pending = await _pending();
    var transaction = await database.getNextCrudTransaction();
    List<({List<int> bytes, int count})>? plannedRemaining;
    current();
    if (pending == null && transaction == null) return false;

    if (pending == null) {
      if (transaction == null) throw const AmobiUploadHeldException(AmobiUploadHoldReason.invalidState);
      if (transaction.crud.isEmpty || transaction.crud.length > _maxEntries) {
        throw const AmobiUploadHeldException(AmobiUploadHoldReason.capacity);
      }
      final head = await _rawHead(transaction);
      if (head == null) throw const AmobiUploadHeldException(AmobiUploadHoldReason.headChanged);
      final planned = _planChunks(transaction.crud, 0, buildBody);
      final firstChunk = planned.first;
      plannedRemaining = planned.sublist(1);
      current();
      final key = "$amobiUploadProtocol-${const Uuid().v4()}";
      await database.writeTransaction((tx) async {
        current();
        final stillHead = await _rawHead(transaction!, tx: tx);
        if (stillHead != head) throw const AmobiUploadHeldException(AmobiUploadHoldReason.headChanged);
        await tx.execute(
          "INSERT INTO $_pendingTable(singleton,upload_key,uid,client_id,raw_head,body,entry_count,outcome,chunk_offset) "
          "VALUES(1,?,?,?,?,?,?,0,0)",
          [key, uid, clientId, head, firstChunk.bytes, firstChunk.count],
        );
      });
      pending = await _pending();
    }

    if (pending == null || pending.uid != uid || pending.clientId != clientId) {
      throw const AmobiUploadHeldException(AmobiUploadHoldReason.identityChanged);
    }
    if (pending.ambiguous) throw const AmobiUploadHeldException(AmobiUploadHoldReason.ambiguousAck);
    current();
    transaction = await database.getNextCrudTransaction();
    final currentHead = transaction == null ? null : await _rawHead(transaction);
    current();
    if (currentHead != pending.rawHead) {
      if (pending.acknowledged &&
          pending.chunkOffset + pending.entryCount == _capturedCount(pending.rawHead) &&
          await _allCapturedGone(pending.rawHead)) {
        await database.execute("DELETE FROM $_pendingTable WHERE singleton=1 AND upload_key=?", [pending.key]);
        return true;
      }
      throw const AmobiUploadHeldException(AmobiUploadHoldReason.headChanged);
    }
    if (transaction == null ||
        pending.entryCount > _maxBatchEntries ||
        pending.chunkOffset + pending.entryCount > transaction.crud.length) {
      throw const AmobiUploadHeldException(AmobiUploadHoldReason.headChanged);
    }
    plannedRemaining ??= _planChunks(transaction.crud, pending.chunkOffset + pending.entryCount, buildBody);
    var plannedPosition = 0;
    current();
    var active = pending;
    for (var chunk = 0; chunk < _maxEntries; chunk++) {
      if (!active.acknowledged) {
        http.Response response;
        try {
          response = await transport(active.key, utf8.decode(active.body));
        } catch (_) {
          current();
          throw const AmobiUploadTransportException();
        }
        if (!_validAcknowledgement(response, active.key, active.entryCount, active.body)) {
          if (response.statusCode == 200) {
            await database.execute(
              "UPDATE $_pendingTable SET outcome=2 WHERE singleton=1 AND upload_key=? AND outcome=0",
              [active.key],
            );
            current();
            throw const AmobiUploadHeldException(AmobiUploadHoldReason.ambiguousAck);
          }
          current();
          throw AmobiUploadTransportException(response.statusCode);
        }
        await database.execute(
          "UPDATE $_pendingTable SET outcome=1 WHERE singleton=1 AND upload_key=? AND outcome=0",
          [active.key],
        );
        current();
      }
      if (await _rawHead(transaction) != active.rawHead) {
        throw const AmobiUploadHeldException(AmobiUploadHoldReason.headChanged);
      }
      current();
      final nextOffset = active.chunkOffset + active.entryCount;
      if (nextOffset == transaction.crud.length) {
        await transaction.complete();
        current();
        await database.execute("DELETE FROM $_pendingTable WHERE singleton=1 AND upload_key=?", [active.key]);
        return true;
      }
      final nextChunk = plannedRemaining[plannedPosition++];
      final nextKey = "$amobiUploadProtocol-${const Uuid().v4()}";
      await database.writeTransaction((tx) async {
        current();
        if (await _rawHead(transaction!, tx: tx) != active.rawHead) {
          throw const AmobiUploadHeldException(AmobiUploadHoldReason.headChanged);
        }
        await tx.execute(
          "UPDATE $_pendingTable SET upload_key=?,body=?,entry_count=?,chunk_offset=?,outcome=0 "
          "WHERE singleton=1 AND upload_key=? AND outcome=1 AND chunk_offset=?",
          [nextKey, nextChunk.bytes, nextChunk.count, nextOffset, active.key, active.chunkOffset],
        );
        final changed = await tx.execute("SELECT changes() AS changed");
        if (changed.single["changed"] != 1) throw const AmobiUploadHeldException(AmobiUploadHoldReason.invalidState);
      });
      current();
      final nextPending = await _pending();
      if (nextPending == null || nextPending.key != nextKey || nextPending.chunkOffset != nextOffset) {
        throw const AmobiUploadHeldException(AmobiUploadHoldReason.invalidState);
      }
      active = nextPending;
    }
    throw const AmobiUploadHeldException(AmobiUploadHoldReason.capacity);
  }

  Future<_Pending?> _pending() async {
    final sizes = await database.getAll(
      "SELECT typeof(body) AS body_type,typeof(raw_head) AS head_type,"
      "typeof(uid) AS uid_type,typeof(client_id) AS client_type,typeof(upload_key) AS key_type,"
      "typeof(entry_count) AS count_type,typeof(outcome) AS outcome_type,"
      "typeof(chunk_offset) AS offset_type,"
      "length(body) AS body_bytes,length(CAST(raw_head AS BLOB)) AS head_bytes,"
      "length(CAST(uid AS BLOB)) AS uid_bytes,length(CAST(client_id AS BLOB)) AS client_bytes,"
      "length(CAST(upload_key AS BLOB)) AS key_bytes,entry_count,outcome,chunk_offset FROM $_pendingTable LIMIT 2",
    );
    if (sizes.length > 1) throw const AmobiUploadHeldException(AmobiUploadHoldReason.invalidState);
    if (sizes.isEmpty) return null;
    final size = sizes.single;
    if (size["body_type"] != "blob" ||
        size["head_type"] != "text" ||
        size["uid_type"] != "text" ||
        size["client_type"] != "text" ||
        size["key_type"] != "text" ||
        size["count_type"] != "integer" ||
        size["outcome_type"] != "integer" ||
        size["offset_type"] != "integer" ||
        size["body_bytes"] is! int ||
        (size["body_bytes"] as int) < 1 ||
        (size["body_bytes"] as int) > _maxBodyBytes ||
        size["head_bytes"] is! int ||
        (size["head_bytes"] as int) < 1 ||
        (size["head_bytes"] as int) > _maxHeadBytes ||
        size["uid_bytes"] is! int ||
        (size["uid_bytes"] as int) < 1 ||
        (size["uid_bytes"] as int) > 512 ||
        size["client_bytes"] is! int ||
        (size["client_bytes"] as int) < 1 ||
        (size["client_bytes"] as int) > 128 ||
        size["key_bytes"] is! int ||
        (size["key_bytes"] as int) > 100 ||
        size["entry_count"] is! int ||
        (size["entry_count"] as int) < 1 ||
        (size["entry_count"] as int) > _maxEntries ||
        size["chunk_offset"] is! int ||
        (size["chunk_offset"] as int) < 0 ||
        (size["chunk_offset"] as int) >= _maxEntries ||
        !const [0, 1, 2].contains(size["outcome"])) {
      throw const AmobiUploadHeldException(AmobiUploadHoldReason.invalidState);
    }
    final rows = await database.getAll("SELECT * FROM $_pendingTable LIMIT 2");
    if (rows.length > 1) throw const AmobiUploadHeldException(AmobiUploadHoldReason.invalidState);
    if (rows.isEmpty) return null;
    final row = rows.single;
    final body = row["body"];
    final rawHead = row["raw_head"];
    final count = row["entry_count"];
    final offset = row["chunk_offset"];
    if (body is! List<int> ||
        rawHead is! String ||
        count is! int ||
        count < 1 ||
        count > _maxEntries ||
        offset is! int ||
        offset < 0 ||
        offset >= _maxEntries ||
        body.length > _maxBodyBytes ||
        utf8.encode(rawHead).length > _maxHeadBytes ||
        row["upload_key"] is! String ||
        !_uploadKeyPattern.hasMatch(row["upload_key"] as String) ||
        row["uid"] is! String ||
        row["client_id"] is! String ||
        !const [0, 1, 2].contains(row["outcome"])) {
      throw const AmobiUploadHeldException(AmobiUploadHoldReason.invalidState);
    }
    try {
      utf8.decode(body);
    } on FormatException {
      throw const AmobiUploadHeldException(AmobiUploadHoldReason.invalidState);
    }
    return _Pending(
      row["upload_key"] as String,
      row["uid"] as String,
      row["client_id"] as String,
      rawHead,
      body,
      count,
      offset,
      row["outcome"] == 1,
      row["outcome"] == 2,
    );
  }

  ({List<int> bytes, int count}) _freezeChunk(
    List<CrudEntry> entries,
    int offset,
    String Function(List<CrudEntry>) buildBody,
  ) {
    final limit = (entries.length - offset).clamp(0, _maxBatchEntries);
    List<int> encode(int count) => utf8.encode(buildBody(entries.sublist(offset, offset + count)));
    final first = encode(1);
    if (first.isEmpty || first.length > _maxBodyBytes) {
      throw const AmobiUploadHeldException(AmobiUploadHoldReason.capacity);
    }
    if (limit == 1) return (bytes: first, count: 1);
    final full = encode(limit);
    if (full.isNotEmpty && full.length <= _maxBodyBytes) return (bytes: full, count: limit);
    var low = 1;
    var high = limit;
    var best = first;
    while (high - low > 1) {
      final middle = (low + high) ~/ 2;
      final bytes = encode(middle);
      if (bytes.isNotEmpty && bytes.length <= _maxBodyBytes) {
        low = middle;
        best = bytes;
      } else {
        high = middle;
      }
    }
    return (bytes: best, count: low);
  }

  List<({List<int> bytes, int count})> _planChunks(
    List<CrudEntry> entries,
    int offset,
    String Function(List<CrudEntry>) buildBody,
  ) {
    final chunks = <({List<int> bytes, int count})>[];
    var totalBytes = 0;
    while (offset < entries.length) {
      final chunk = _freezeChunk(entries, offset, buildBody);
      totalBytes += chunk.bytes.length;
      if (totalBytes > _maxPlannedBytes) throw const AmobiUploadHeldException(AmobiUploadHoldReason.capacity);
      chunks.add(chunk);
      offset += chunk.count;
    }
    return chunks;
  }

  Future<String?> _rawHead(CrudTransaction transaction, {SqliteWriteContext? tx}) async {
    final entries = transaction.crud;
    if (entries.isEmpty || entries.length > _maxEntries) return null;
    final first = entries.first.clientId;
    final last = entries.last.clientId;
    final List<Map<String, Object?>> rows;
    if (tx == null) {
      rows = [
        for (final row in await database.getAll(
          "SELECT id,tx_id,data FROM ps_crud WHERE id BETWEEN ? AND ? ORDER BY id",
          [first, last],
        ))
          Map<String, Object?>.from(row),
      ];
    } else {
      rows = [
        for (final row in await tx.execute("SELECT id,tx_id,data FROM ps_crud WHERE id BETWEEN ? AND ? ORDER BY id", [
          first,
          last,
        ]))
          Map<String, Object?>.from(row),
      ];
    }
    if (rows.length != entries.length) return null;
    for (var i = 0; i < entries.length; i++) {
      if (rows[i]["id"] != entries[i].clientId || rows[i]["tx_id"] != entries[i].transactionId) return null;
    }
    final List<Map<String, Object?>> earliest;
    if (tx == null) {
      earliest = [
        for (final row in await database.getAll("SELECT id FROM ps_crud ORDER BY id LIMIT 1"))
          Map<String, Object?>.from(row),
      ];
    } else {
      earliest = [
        for (final row in await tx.execute("SELECT id FROM ps_crud ORDER BY id LIMIT 1"))
          Map<String, Object?>.from(row),
      ];
    }
    if (earliest.isEmpty || earliest.first["id"] != first) return null;
    final raw = jsonEncode([
      for (final row in rows) {"id": row["id"], "tx_id": row["tx_id"], "data": row["data"]},
    ]);
    return utf8.encode(raw).length <= _maxHeadBytes ? raw : null;
  }

  Future<bool> _allCapturedGone(String rawHead) async {
    try {
      final decoded = jsonDecode(rawHead);
      if (decoded is! List || decoded.isEmpty) return false;
      if (decoded.first is! Map || decoded.last is! Map) return false;
      final first = (decoded.first as Map)["id"];
      final last = (decoded.last as Map)["id"];
      if (first is! int || last is! int || first > last) return false;
      final rows = await database.getAll("SELECT id FROM ps_crud WHERE id BETWEEN ? AND ? LIMIT 1", [first, last]);
      return rows.isEmpty;
    } on FormatException {
      return false;
    }
  }

  int _capturedCount(String rawHead) {
    try {
      final decoded = jsonDecode(rawHead);
      return decoded is List ? decoded.length : -1;
    } catch (_) {
      return -1;
    }
  }

  bool _validAcknowledgement(http.Response response, String key, int entryCount, List<int> requestBody) {
    if (response.statusCode != 200 ||
        response.bodyBytes.length > 64 * 1024 ||
        _header(response, "idempotency-key") != key ||
        _header(response, "x-amobi-upload-protocol") != amobiUploadProtocol) {
      return false;
    }
    try {
      final body = jsonDecode(response.body);
      if (body is! Map<String, dynamic> || body.length != 3 || body["ok"] != true) return false;
      final applied = body["applied"];
      final skipped = body["skipped"];
      final expectedSkipped = _expectedLocalSkips(requestBody, entryCount);
      return applied is int &&
          skipped is int &&
          expectedSkipped != null &&
          applied >= 0 &&
          skipped >= 0 &&
          skipped == expectedSkipped &&
          applied == entryCount - expectedSkipped;
    } catch (_) {
      return false;
    }
  }

  int? _expectedLocalSkips(List<int> requestBody, int entryCount) {
    try {
      final decoded = jsonDecode(utf8.decode(requestBody));
      if (decoded is! Map<String, dynamic> || decoded["batch"] is! List) return null;
      final batch = decoded["batch"] as List;
      if (batch.length != entryCount) return null;
      var skipped = 0;
      for (final entry in batch) {
        if (entry is! Map<String, dynamic>) return null;
        final op = entry["op"];
        if (op == "SKIP") {
          final data = entry["data"];
          if (entry["table"] != "stored_files_table" ||
              data is! Map<String, dynamic> ||
              data.length != 2 ||
              data["id"] is! String ||
              data["user_id"] is! String) {
            return null;
          }
          skipped++;
        } else if (!const {"PUT", "PATCH", "DELETE"}.contains(op)) {
          return null;
        }
      }
      return skipped;
    } catch (_) {
      return null;
    }
  }

  String? _header(http.Response response, String name) {
    final matching = response.headers.entries.where((entry) => entry.key.toLowerCase() == name).toList();
    return matching.length == 1 ? matching.single.value : null;
  }
}

final class _Pending {
  const _Pending(
    this.key,
    this.uid,
    this.clientId,
    this.rawHead,
    this.body,
    this.entryCount,
    this.chunkOffset,
    this.acknowledged,
    this.ambiguous,
  );
  final String key;
  final String uid;
  final String clientId;
  final String rawHead;
  final List<int> body;
  final int entryCount;
  final int chunkOffset;
  final bool acknowledged;
  final bool ambiguous;
}
