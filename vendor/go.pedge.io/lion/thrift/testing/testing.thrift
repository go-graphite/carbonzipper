namespace go thriftliontesting

struct Foo {
  1: optional string one,
  2: optional i32 two,
  3: optional string string_field,
  4: optional i32 int32_field,
  5: optional Bar bar,
}

struct Bar {
  1: optional string one,
  2: optional string two,
  3: optional string string_field,
  4: optional i32 int32_field,
}

struct Ban {
  1: optional string string_field,
  2: optional i32 int32_field,
}

struct Bat {
  1: optional Ban ban,
}

struct Baz {
  1: optional Bat bat,
}

struct Empty {}

struct NoStdJson {
  1: optional map<i64, string> one,
}
