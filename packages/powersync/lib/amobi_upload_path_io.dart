import "dart:io";

Future<String> canonicalUploadPath(String path) => File(path).resolveSymbolicLinks();
