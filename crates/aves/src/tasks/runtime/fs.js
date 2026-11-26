import * as std from "qjs:std";
import * as os from "qjs:os";

function requireString(operation, argument, value) {
  if (typeof value !== "string") {
    throw new TypeError(`${operation}: ${argument} must be a string`);
  }
}

function filesystemError(operation, path, detail) {
  return new Error(`${operation}: ${path}${detail === undefined ? "" : `: ${detail}`}`);
}

export function readTextFile(path) {
  requireString("readTextFile", "path", path);
  const contents = std.loadFile(path);
  if (contents === null) {
    throw filesystemError("readTextFile", path);
  }
  return contents;
}

export function writeTextFile(path, contents) {
  requireString("writeTextFile", "path", path);
  requireString("writeTextFile", "contents", contents);
  if (contents === "") {
    const errorInfo = {};
    let file;
    try {
      file = std.open(path, "w", errorInfo);
    } catch (error) {
      throw filesystemError("writeTextFile", path, error.message);
    }
    if (file === null) {
      throw filesystemError("writeTextFile", path, `errno ${errorInfo.errno}`);
    }
    let result;
    try {
      result = file.close();
    } catch (error) {
      throw filesystemError("writeTextFile", path, error.message);
    }
    if (result !== 0) {
      throw filesystemError("writeTextFile", path, `close result ${result}`);
    }
    return;
  }
  try {
    std.writeFile(path, contents);
  } catch (error) {
    throw filesystemError("writeTextFile", path, error.message);
  }
}

export function listDir(path) {
  requireString("listDir", "path", path);
  const [entries, errno] = os.readdir(path);
  if (errno !== 0) {
    throw filesystemError("listDir", path, `errno ${errno}`);
  }
  return entries.filter(entry => entry !== "." && entry !== "..");
}

export function makeDir(path) {
  requireString("makeDir", "path", path);
  const errno = os.mkdir(path);
  if (errno !== 0) {
    throw filesystemError("makeDir", path, `errno ${errno}`);
  }
}
