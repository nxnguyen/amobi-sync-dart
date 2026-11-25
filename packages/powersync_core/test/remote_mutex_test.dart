import 'dart:async';
import 'dart:isolate';

import 'package:amobi_sync_core/src/database/native/remote_mutex.dart';
import 'package:sqlite_async/sqlite_async.dart';
import 'package:test/test.dart';

void _routeMutexMessage(Object? message, MutexServer server) {
  if (message is! List || message.isEmpty) return;
  switch (message[0]) {
    case 'mutex:acquire':
      server.acquireRequest(message[4] as SendPort, message[1] as String,
          message[2] as String, message[3] as int);
    case 'mutex:release':
      server.releaseRequest(message[1] as String);
    case 'mutex:cancel':
      server.cancelRequest(message[1] as String);
  }
}

Future<void> _holdMutexInChild(List<Object?> args) async {
  final mutexes = RemoteMutexes(args[0] as SendPort);
  await mutexes.mutex(args[1] as String).lock(() async {
    (args[2] as SendPort).send('held');
    await Completer<void>().future;
  });
}

void main() {
  test('sync lock requests serialize through the parent mutex server',
      () async {
    final serverPort = ReceivePort();
    final server = MutexServer({'sync': Mutex.simple()});
    serverPort.listen((message) => _routeMutexMessage(message, server));
    final remote = RemoteMutexes(serverPort.sendPort);
    addTearDown(() {
      remote.close();
      serverPort.close();
    });

    final entered = <int>[];
    final firstEntered = Completer<void>();
    final releaseFirst = Completer<void>();
    final first = remote.mutex('sync').lock(() async {
      entered.add(1);
      firstEntered.complete();
      await releaseFirst.future;
    });
    await firstEntered.future;

    final second = remote.mutex('sync').lock(() async {
      entered.add(2);
    });
    await Future<void>.delayed(Duration.zero);
    expect(entered, [1]);

    releaseFirst.complete();
    await Future.wait([first, second]);
    expect(entered, [1, 2]);
  });

  test('canceling a queued request lets later requests acquire the lock',
      () async {
    final serverPort = ReceivePort();
    final server = MutexServer({'sync': Mutex.simple()});
    serverPort.listen((message) => _routeMutexMessage(message, server));
    final remote = RemoteMutexes(serverPort.sendPort);
    addTearDown(() {
      remote.close();
      serverPort.close();
    });

    final firstEntered = Completer<void>();
    final releaseFirst = Completer<void>();
    final first = remote.mutex('sync').lock(() async {
      firstEntered.complete();
      await releaseFirst.future;
    });
    await firstEntered.future;

    final abortWaiting = Completer<void>();
    final canceled = remote.mutex('sync').lock(() async {
      fail('The canceled request must not acquire the lock.');
    }, abortTrigger: abortWaiting.future);
    await Future<void>.delayed(Duration.zero);
    abortWaiting.complete();
    await expectLater(canceled, throwsA(isA<AbortException>()));

    final laterEntered = Completer<void>();
    final later = remote.mutex('sync').lock(() async {
      laterEntered.complete();
    });
    releaseFirst.complete();
    await first;
    await laterEntered.future.timeout(const Duration(seconds: 1));
    await later;
  });

  test('a child isolate exit releases its held sync mutex', () async {
    final serverPort = ReceivePort();
    final readyPort = ReceivePort();
    final exitPort = ReceivePort();
    final syncMutex = Mutex.simple();
    final server = MutexServer({'sync': syncMutex});
    serverPort.listen((message) => _routeMutexMessage(message, server));
    final child = await Isolate.spawn(
      _holdMutexInChild,
      [serverPort.sendPort, 'sync', readyPort.sendPort],
      onExit: exitPort.sendPort,
    );
    addTearDown(() {
      serverPort.close();
      readyPort.close();
      exitPort.close();
    });

    await expectLater(readyPort.first, completion('held'));
    child.kill(priority: Isolate.immediate);
    await exitPort.first;
    server.handleChildIsolateExit();

    var acquired = false;
    await syncMutex.lock(() async {
      acquired = true;
    }).timeout(const Duration(seconds: 1));
    expect(acquired, isTrue);
  });

  test('CRUD and sync use separate parent-owned mutexes', () async {
    final serverPort = ReceivePort();
    final syncMutex = Mutex.simple();
    final crudMutex = Mutex.simple();
    final server = MutexServer({'sync': syncMutex, 'crud': crudMutex});
    serverPort.listen((message) => _routeMutexMessage(message, server));
    final remote = RemoteMutexes(serverPort.sendPort);
    addTearDown(() {
      remote.close();
      serverPort.close();
    });

    final syncEntered = Completer<void>();
    final releaseSync = Completer<void>();
    final sync = remote.mutex('sync').lock(() async {
      syncEntered.complete();
      await releaseSync.future;
    });
    await syncEntered.future;

    final crudEntered = Completer<void>();
    final crud = remote.mutex('crud').lock(() async {
      crudEntered.complete();
    });
    await crudEntered.future.timeout(const Duration(seconds: 1));

    releaseSync.complete();
    await Future.wait([sync, crud]);
  });
}
