import 'dart:io';

import 'package:amobi_sync_core/src/setup_web.dart';
import 'package:test/test.dart';

void main() {
  late Directory fixture;
  late File configFile;
  late HttpClient client;

  setUp(() {
    fixture = Directory.systemTemp.createTempSync('amobi-sync-web-');
    configFile = File('${fixture.path}/.dart_tool/package_config.json');
    configFile.parent.createSync();
    client = HttpClient();
  });

  tearDown(() {
    client.close(force: true);
    fixture.deleteSync(recursive: true);
  });

  Map<String, Object> package(String name, String version) {
    final root = Directory('${fixture.path}/$name')..createSync();
    File('${root.path}/pubspec.yaml').writeAsStringSync('name: $name\nversion: $version\n');
    return {'name': name, 'rootUri': '../$name'};
  }

  test('fork version resolves to its pinned upstream asset tag', () async {
    final result = await powerSyncVersionOrLatest(client, {
      'packages': <dynamic>[package('amobi_sync_flutter', '1.16.2+amobi.1')],
    }, configFile);

    expect(result.tag, 'powersync-v1.16.2');
    expect(result.version.toString(), '1.16.2');
  });

  test('fork takes precedence when both packages are present', () async {
    final result = await powerSyncVersionOrLatest(client, {
      'packages': <dynamic>[
        package('powersync', '1.15.0'),
        package('amobi_sync_flutter', '1.16.2+amobi.1'),
      ],
    }, configFile);

    expect(result.tag, 'powersync-v1.16.2');
  });

  test('original package keeps its explicit pinned version', () async {
    final result = await powerSyncVersionOrLatest(client, {
      'packages': <dynamic>[package('powersync', '1.15.0')],
    }, configFile);

    expect(result.tag, 'powersync-v1.15.0');
  });

  test('help exits before reading package configuration or downloading assets', () async {
    await downloadWebAssets(['--help']);
  });
}
