import 'dart:async';
import 'dart:isolate';

import 'package:sqlite_async/sqlite_async.dart';

/// Relays sync and CRUD locks from the database isolate to the sync isolate.
final class MutexServer {
  final Map<String, Mutex> _mutexes;
  final Map<String, Completer<void>> _held = {};
  final Map<String, Completer<void>> _waiting = {};
  bool _didExit = false;

  MutexServer(this._mutexes);

  void acquireRequest(SendPort reply, String name, String key, int requestId) {
    final abort = Completer<void>();
    _waiting[key] = abort;
    unawaited(_mutexes[name]!.lock(() async {
      _waiting.remove(key);
      if (_didExit) return;
      final release = Completer<void>.sync();
      _held[key] = release;
      reply.send(['mutex:granted', requestId]);
      await release.future;
      _held.remove(key);
    }, abortTrigger: abort.future).catchError((Object _) {
      _waiting.remove(key);
      if (!_didExit) reply.send(['mutex:aborted', requestId]);
    }));
  }

  void releaseRequest(String key) {
    _held.remove(key)?.complete();
  }

  void cancelRequest(String key) {
    final abort = _waiting.remove(key);
    if (abort != null && !abort.isCompleted) abort.complete();
    releaseRequest(key);
  }

  void handleChildIsolateExit() {
    _didExit = true;
    for (final abort in _waiting.values.toList()) {
      if (!abort.isCompleted) abort.complete();
    }
    _waiting.clear();
    for (final release in _held.values.toList()) {
      if (!release.isCompleted) release.complete();
    }
    _held.clear();
  }
}

final class RemoteMutexes {
  final SendPort sendPort;
  int _nextRequestId = 0;
  final Map<int, Completer<void>> _inflightRequests = {};
  final String _clientId = '${Isolate.current.hashCode}';
  final ReceivePort _receivePort = ReceivePort();

  RemoteMutexes(this.sendPort) {
    _receivePort.listen((message) {
      if (message is List && message.length >= 2) {
        final requestId = message[1] as int;
        if (message[0] == 'mutex:granted') {
          markGranted(requestId);
        } else if (message[0] == 'mutex:aborted') {
          markAborted(requestId);
        }
      }
    });
  }

  Mutex mutex(String name) => _RemoteMutex(this, name);

  Future<void> acquire(String name, int requestId) async {
    final completer = _inflightRequests[requestId] = Completer<void>.sync();
    sendPort.send([
      'mutex:acquire',
      name,
      '$_clientId:$requestId',
      requestId,
      _receivePort.sendPort,
    ]);
    await completer.future;
  }

  int nextRequestId() => _nextRequestId++;

  String keyFor(int requestId) => '$_clientId:$requestId';

  void close() => _receivePort.close();

  void markGranted(int requestId) {
    _inflightRequests.remove(requestId)?.complete();
  }

  void markAborted(int requestId) {
    _inflightRequests
        .remove(requestId)
        ?.completeError(AbortException('sync mutex'));
  }
}

final class _RemoteMutex implements Mutex {
  final RemoteMutexes _mutexes;
  final String name;

  _RemoteMutex(this._mutexes, this.name);

  @override
  Future<T> lock<T>(Future<T> Function() callback,
      {Future<void>? abortTrigger}) async {
    final id = _mutexes.nextRequestId();
    var acquired = false;
    var cancelled = false;
    if (abortTrigger != null) {
      abortTrigger.then((_) {
        if (acquired) return;
        cancelled = true;
        _mutexes.sendPort.send(['mutex:cancel', _mutexes.keyFor(id)]);
        _mutexes.markAborted(id);
      });
    }

    try {
      await _mutexes.acquire(name, id);
    } on AbortException {
      rethrow;
    }

    if (cancelled) {
      _mutexes.sendPort.send(['mutex:release', _mutexes.keyFor(id)]);
      throw AbortException('sync mutex');
    }
    acquired = true;
    try {
      return await callback();
    } finally {
      _mutexes.sendPort.send(['mutex:release', _mutexes.keyFor(id)]);
    }
  }
}
