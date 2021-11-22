use "promises"

trait tag RedisStringApi

  fun tag _exec[A: Any val, E: RedisValueExtractor[A] val](command: Array[RedisValue val] val): Promise[A]

  fun tag get(key: String): Promise[(String | None)] =>
    """
    Get the value of a key
    """
    _exec[(String | None), _BlobStringOrNoneExtractor]([RedisValue.blob_string("GET"); RedisValue.blob_string(key)])

  fun tag set(key: String, value: String): Promise[Bool] =>
    """
    Set key to hold the string value. If key already holds a value,
    It is overwritten, regardless of its type. Any pervious time to
    live associated with the key is discarded on successful SET
    operation.
    """
    _exec[Bool, _OkayOrNoneExtractor]([RedisValue.blob_string("SET"); RedisValue.blob_string(key); RedisValue.blob_string(value)])
