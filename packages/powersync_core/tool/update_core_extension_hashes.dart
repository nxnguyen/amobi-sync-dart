import 'dart:convert';
import 'dart:io';

import 'package:crypto/crypto.dart';
import 'package:amobi_sync_core/src/setup/native.dart';

Future<void> main() async {
  final client = HttpClient();
  try {
    final releaseUri = Uri.https(
      'api.github.com',
      '/repos/powersync-ja/powersync-sqlite-core/releases/tags/$releaseVersion',
    );
    final release = await _getJson(client, releaseUri);
    final assets = (release['assets'] as List)
        .cast<Map<String, dynamic>>()
        .where((asset) => _isNativeAsset(asset['name'] as String))
        .toList()
      ..sort((a, b) => (a['name'] as String).compareTo(b['name'] as String));

    final hashes = <String, String>{};
    for (final asset in assets) {
      final name = asset['name'] as String;
      final uri = Uri.parse(asset['browser_download_url'] as String);
      final request = await client.getUrl(uri);
      request.headers
          .set(HttpHeaders.userAgentHeader, 'amobi-sync-hash-update');
      final response = await request.close();
      if (response.statusCode != HttpStatus.ok) {
        throw HttpException(
            'Download $uri returned HTTP ${response.statusCode}');
      }
      final bytes = await response.fold<List<int>>(<int>[], (out, chunk) {
        out.addAll(chunk);
        return out;
      });
      hashes[name] = sha256.convert(bytes).toString();
    }

    if (hashes.isEmpty) throw StateError('No native release assets found');
    final file = File('lib/src/setup/native.dart');
    final source = await file.readAsString();
    const start = '  // start of generated hashes\n';
    const end = '  // end of generated hashes';
    final startIndex = source.indexOf(start);
    final endIndex = source.indexOf(end, startIndex);
    if (startIndex < 0 || endIndex < 0) {
      throw StateError('Could not find generated hash markers in ${file.path}');
    }
    final entries = hashes.entries
        .map((entry) => "  '${entry.key}':\n      '${entry.value}',\n")
        .join();
    final updated = source.replaceRange(
      startIndex,
      endIndex,
      '$start$entries',
    );
    await file.writeAsString(updated);
    stdout.writeln('Updated ${hashes.length} release asset hashes.');
  } finally {
    client.close();
  }
}

bool _isNativeAsset(String name) =>
    name.startsWith('libpowersync_') || name.startsWith('powersync_');

Future<Map<String, dynamic>> _getJson(HttpClient client, Uri uri) async {
  final request = await client.getUrl(uri);
  request.headers
    ..set(HttpHeaders.userAgentHeader, 'amobi-sync-hash-update')
    ..set(HttpHeaders.acceptHeader, 'application/vnd.github+json');
  final response = await request.close();
  if (response.statusCode != HttpStatus.ok) {
    throw HttpException('Request $uri returned HTTP ${response.statusCode}');
  }
  return jsonDecode(await response.transform(utf8.decoder).join())
      as Map<String, dynamic>;
}
