namespace go thriftlion

// Level is a logging level.
enum Level {
  DEBUG = 0,
  INFO = 1,
  WARN = 2,
  ERROR = 3,
  FATAL = 4,
  PANIC = 5,
  NONE = 6,
}

// EntryMessage represents a serialized protobuf message.
// The name is the name registered with lion.
struct EntryMessage {
  1: optional string encoding,
  2: optional string name,
  3: optional binary value,
}

// Entry is the object serialized for logging.
struct Entry {
  // id may not be set depending on logger options
  // it is up to the user to determine if id is required
  1: optional string id,  
  2: optional Level level,
  3: optional i64 time_unix_nanos,
  // both context and fields may be set
  4: optional list<EntryMessage> context,
  5: optional map<string, string> fields,
  // one of event, message, writer_output will be set
  6: optional EntryMessage event,
  7: optional string message,
  8: optional binary writer_output,
}
