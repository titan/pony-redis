use "collections"
use "promises"

primitive StreamTrimmingStrategyMaxLen

primitive StreamTrimmingStrategyMinId

type StreamTrimmingStrategy is (StreamTrimmingStrategyMaxLen | StreamTrimmingStrategyMinId)

trait tag RedisStreamApi

  fun tag _exec[A: Any val, E: RedisValueExtractor[A] val](command: Array[RedisValue val] val): Promise[A]

  fun tag xack(key: String, group: String, ids: Array[String]): Promise[I64] =>
    """
    The XACK command removes one or multiple messages from the
    Pending Entries List(PEL) of a stream consumer group. A message
    is pending, and as such stored inside the PEL, when it was
    delivered to some consumer, normally as a side effect of calling
    XREADGROUP, or when a consumer took ownership of a message
    calling XCLAIM.
    """
    let cmd = recover Array[RedisValue val](ids.size() + 3) end
    cmd.push(RedisValue.blob_string("XACK"))
    cmd.push(RedisValue.blob_string(key))
    cmd.push(RedisValue.blob_string(group))
    for id in ids.values() do
      cmd.push(RedisValue.blob_string(id))
    end
    _exec[I64, _NumberExtractor](consume cmd)

  fun tag xadd(key: String, pairs: Array[(String, String)], strategy: (StreamTrimmingStrategy | None) = None, threshold: (String | None) = None, limit: USize = 0, id: String = "*", nomkstream: Bool = false): Promise[(String | None)] =>
    """
    Appends the specified stream entry to the stream at the specified
    key. If the key does not exist, as a side effect of running this
    command the key is created with a stream value. The creation of
    stream's key can be disabled with the NOMKSTREAM option.
    """
    let cmd = recover Array[RedisValue val]((pairs.size() * 2) + 8) end
    cmd.push(RedisValue.blob_string("XADD"))
    cmd.push(RedisValue.blob_string(key))
    match strategy
    | StreamTrimmingStrategyMaxLen =>
      match threshold
      | let threshold' : String =>
        cmd.push(RedisValue.blob_string("MAXLEN"))
        cmd.push(RedisValue.blob_string("~"))
        cmd.push(RedisValue.blob_string(threshold'))
        if limit > 0 then
          cmd.push(RedisValue.blob_string(limit.string()))
        end
      end
    | StreamTrimmingStrategyMinId =>
      match threshold
      | let threshold' : String =>
        cmd.push(RedisValue.blob_string("MINID"))
        cmd.push(RedisValue.blob_string("~"))
        cmd.push(RedisValue.blob_string(threshold'))
        if limit > 0 then
          cmd.push(RedisValue.blob_string(limit.string()))
        end
      end
    end
    cmd.push(RedisValue.blob_string(id))
    for (k, v) in pairs.values() do
      cmd.push(RedisValue.blob_string(k))
      cmd.push(RedisValue.blob_string(v))
    end
    _exec[(String | None), _BlobStringOrNoneExtractor](consume cmd)

  fun tag xdel(key: String, ids: Array[String]): Promise[I64] =>
    """
    Removes the specified entries from a stream, and returns the
    number of entries deleted. This number may be less than the
    number of IDs passed to the command in the case where some of the
    specified IDs do not exist in the stream.
    """
    let cmd = recover Array[RedisValue val](ids.size() + 2) end
    cmd.push(RedisValue.blob_string("XDEL"))
    cmd.push(RedisValue.blob_string(key))
    for id in ids.values() do
      cmd.push(RedisValue.blob_string(id))
    end
    _exec[I64, _NumberExtractor](consume cmd)

  fun tag xlen(key: String): Promise[I64] =>
    """
    Returns the number of entries inside a stream. If the specified
    key does not exist the command returns zero, as if the stream was
    empty. However note that unlike other Redis types, zero-length
    streams are possible, so you should call TYPE or EXISTS in order
    to check if a key exists or not.
    """
    _exec[I64, _NumberExtractor]([RedisValue.blob_string("XLEN"); RedisValue.blob_string(key)])

  fun tag xrange(key: String, start: String = "-", stop: String = "+", count: USize = 0): Promise[Array[(String, Map[String, String] val)] val] =>
    """
    The command returns the stream entries matching a given range of
    IDs. The range is specified by a minimum and maximum ID. All the
    entries having an ID between the two specified or exactly one of
    the two IDs specified (closed interval) are returned.
    """
    let cmd = recover Array[RedisValue val](5) end
    cmd.push(RedisValue.blob_string("XRANGE"))
    cmd.push(RedisValue.blob_string(key))
    cmd.push(RedisValue.blob_string(start))
    cmd.push(RedisValue.blob_string(stop))
    if count > 0 then
      cmd.push(RedisValue.blob_string(count.string()))
    end
    _exec[Array[(String, Map[String, String] val)] val, _SingleStreamItemArrayExtractor](consume cmd)

  fun tag xread(keyids: Array[(String, String)], count: USize = 0, timeout: U64 = 0): Promise[(None | Map[String, Array[(String, Map[String, String] val)] val] val)] =>
    """
    Read data from one or multiple streams, only returning entries
    with an ID greater than the last received ID reported by the
    caller. This command has an option to block if items are not
    available, in a similar fashion to BRPOP or BZPOPMIN and others.
    """
    let cmd = recover Array[RedisValue val](6 + (keyids.size() * 2)) end
    cmd.push(RedisValue.blob_string("XREAD"))
    if count > 0 then
      cmd.push(RedisValue.blob_string("COUNT"))
      cmd.push(RedisValue.blob_string(count.string()))
    end
    if timeout > 0 then
      cmd.push(RedisValue.blob_string("BLOCK"))
      cmd.push(RedisValue.blob_string(timeout.string()))
    end
    cmd.push(RedisValue.blob_string("STREAMS"))
    for (key, id) in keyids.values() do
      cmd.push(RedisValue.blob_string(key))
    end
    for (key, id) in keyids.values() do
      cmd.push(RedisValue.blob_string(id))
    end
    _exec[(None | Map[String, Array[(String, Map[String, String] val)] val] val), _MultiplayStreamItemArrayExtractor](consume cmd)

  fun tag xreadgroup(group: String, consumer: String, keyids: Array[(String, String)], count: USize = 0, timeout: U64 = 0, noack: Bool = false): Promise[(None | Map[String, Array[(String, Map[String, String] val)] val] val)] =>
    """
    The XREADGROUP command is a special version of the XREAD command
    with support for consumer groups. Probably you will have to
    understand the XREAD command before reading this page will makes
    sense.
    """
    let cmd = recover Array[RedisValue val](10 + (keyids.size() * 2)) end
    cmd.push(RedisValue.blob_string("XREADGROUP"))
    cmd.push(RedisValue.blob_string("GROUP"))
    cmd.push(RedisValue.blob_string(group))
    cmd.push(RedisValue.blob_string(consumer))
    if count > 0 then
      cmd.push(RedisValue.blob_string("COUNT"))
      cmd.push(RedisValue.blob_string(count.string()))
    end
    if timeout > 0 then
      cmd.push(RedisValue.blob_string("BLOCK"))
      cmd.push(RedisValue.blob_string(timeout.string()))
    end
    if noack then
      cmd.push(RedisValue.blob_string("NOACK"))
    end
    cmd.push(RedisValue.blob_string("STREAMS"))
    for (key, id) in keyids.values() do
      cmd.push(RedisValue.blob_string(key))
    end
    for (key, id) in keyids.values() do
      cmd.push(RedisValue.blob_string(id))
    end
    _exec[(None | Map[String, Array[(String, Map[String, String] val)] val] val), _MultiplayStreamItemArrayExtractor](consume cmd)

  fun tag xtrim(key: String, strategies: StreamTrimmingStrategy = StreamTrimmingStrategyMaxLen, threshold: String, limit: USize = 0): Promise[I64] =>
    """
    XTRIM trims the stream by evicting older entries (entries with
    lower IDs) if needed.

    Trimming the stream can be done using one of these strategies:

    MAXLEN: Evicts entries as long as the stream's length exceeds the
            specified threshold, where threshold is a positive
            integer.
    MINID:  Evicts entries with IDs lower than threshold, where
            threshold is a stream ID.
    """
    let cmd = recover Array[RedisValue val](7) end
    cmd.push(RedisValue.blob_string("XTRIM"))
    cmd.push(RedisValue.blob_string(key))
    match strategies
    | StreamTrimmingStrategyMaxLen =>
      cmd.push(RedisValue.blob_string("MAXLEN"))
      cmd.push(RedisValue.blob_string("~"))
      cmd.push(RedisValue.blob_string(threshold.string()))
    | StreamTrimmingStrategyMinId =>
      cmd.push(RedisValue.blob_string("MINID"))
      cmd.push(RedisValue.blob_string("~"))
      cmd.push(RedisValue.blob_string(threshold.string()))
    end
    if limit > 0 then
      cmd.push(RedisValue.blob_string("LIMIT"))
      cmd.push(RedisValue.blob_string(limit.string()))
    end
    _exec[I64, _NumberExtractor](consume cmd)
