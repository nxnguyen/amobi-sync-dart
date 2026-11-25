import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:code_assets/code_assets.dart';
import 'package:crypto/crypto.dart';
import 'package:hooks/hooks.dart';
import 'package:http/io_client.dart';
import 'package:amobi_sync_core/src/setup/native.dart';

void main(List<String> args) async {
  await build(args, (input, output) async {
    if (!input.config.buildCodeAssets) return;

    final fileName = _fileNameForBuild(input.config.code);
    final digest = assetNameToSha256Hash[fileName]!;
    final target = input.outputDirectoryShared.resolve(
      'download-${digest.substring(0, 8)}/',
    );
    Directory(target.toFilePath()).createSync(recursive: true);
    final file = File(
      target
          .resolve(input.config.code.targetOS.libraryFileName(
            'powersync_core',
            DynamicLoadingBundled(),
          ))
          .toFilePath(),
    );

    if (file.existsSync()) {
      final actualDigest = await file.openRead().transform(sha256).first;
      if (actualDigest.toString() == digest) {
        output.assets.code.add(_codeAsset(file));
        return;
      }
    }

    final bytes = await _downloadCoreExtension(fileName, digest);
    final temporary = File('${file.path}.tmp');
    await temporary.writeAsBytes(bytes);
    await temporary.rename(file.path);
    output.assets.code.add(_codeAsset(file));
  });
}

CodeAsset _codeAsset(File file) => CodeAsset(
      package: 'amobi_sync_core',
      name: 'src/open_factory/native/sqlite3_powersync_init.dart',
      linkMode: DynamicLoadingBundled(),
      file: file.uri,
    );

Future<Uint8List> _downloadCoreExtension(String fileName, String expectedDigest) async {
  final client = IOClient(HttpClient()..findProxy = HttpClient.findProxyFromEnvironment);
  final uri = Uri.https(
    'github.com',
    'powersync-ja/powersync-sqlite-core/releases/download/$releaseVersion/$fileName',
  );
  try {
    final response = await client.get(uri);
    if (response.statusCode != HttpStatus.ok) {
      throw HttpException(
        'Could not download $uri: HTTP ${response.statusCode}',
        uri: uri,
      );
    }
    final actualDigest = sha256.convert(response.bodyBytes).toString();
    if (actualDigest != expectedDigest) {
      throw StateError(
        'Unexpected digest for $uri: expected $expectedDigest, got $actualDigest',
      );
    }
    return response.bodyBytes;
  } finally {
    client.close();
  }
}

String _fileNameForBuild(CodeConfig config) {
  Never unsupported() => throw UnsupportedError(
        'PowerSync SQLite core does not support ${config.targetArchitecture.name} '
        'on ${config.targetOS.name}.',
      );

  String architecture({bool armv7 = false, bool x86 = false, bool riscv64 = false}) =>
      switch (config.targetArchitecture) {
        Architecture.arm64 => 'aarch64',
        Architecture.x64 => 'x64',
        Architecture.arm when armv7 => 'armv7',
        Architecture.ia32 when x86 => 'x86',
        Architecture.riscv64 when riscv64 => 'riscv64gc',
        _ => unsupported(),
      };

  return switch (config.targetOS) {
    OS.android => 'libpowersync_${architecture(armv7: true, x86: true)}.android.so',
    OS.iOS => config.iOS.targetSdk == IOSSdk.iPhoneOS
        ? config.targetArchitecture == Architecture.arm64
            ? 'libpowersync_aarch64.ios.dylib'
            : unsupported()
        : 'libpowersync_${architecture()}.ios-sim.dylib',
    OS.linux =>
      'libpowersync_${architecture(armv7: true, x86: true, riscv64: true)}.linux.so',
    OS.macOS => 'libpowersync_${architecture()}.macos.dylib',
    OS.windows => 'powersync_${architecture(x86: true)}.dll',
    _ => throw UnsupportedError('PowerSync SQLite core does not support ${config.targetOS.name}.'),
  };
}
