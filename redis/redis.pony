use "buffered"
use "collections"
use "promises"
use "net"

use "ponytest"
use "format"

actor Redis is (RedisStringApi & RedisStreamApi)
  let _auth: TCPConnectionAuth
  var _conn: (TCPConnection | None) = None

  embed outgoing: Array[String] = []
  embed promises: List[Promise[RedisValue val]] = List[Promise[RedisValue val]]

  new create(auth': TCPConnectionAuth) =>
    _auth = auth'

  be dispose() =>
    match _conn
    | let conn': TCPConnection => conn'.dispose()
    end

  be connect(host': String, port': String) =>
    TCPConnection(_auth, _TCPNotify(this), host', port')

  be _connected(conn': TCPConnection) =>
    for o in outgoing.values() do
      conn'.write(o)
    end
    outgoing.clear()
    _conn = conn'

  be _received(v: RedisValue val) =>
    try promises.shift()?(v) end

  be _received_error(e: String) =>
    try promises.shift()?.reject() end

  be _send(d: (String | None), p: Promise[RedisValue val]) =>
    match d
    | let d': String =>
      match _conn
      | let conn': TCPConnection => conn'.write(d')
      | None => outgoing.push(d')
      end
      promises.push(p)
    | None => p.reject()
    end

  fun tag _exec[A: Any val, E: RedisValueExtractor[A] val](command: Array[RedisValue val] val): Promise[A] =>
    let p = Promise[RedisValue val]
    let cmd = RedisValue.array(command)
    let p' = p.next[A](E~apply(this))
    try
      let d' = RespEncoder(consume cmd)?
      _send(d', p)
    else
      _send(None, p)
    end
    p'

class iso _TCPNotify is TCPConnectionNotify
  let _redis: Redis
  var _ctx: (None | _DecodeContext ref)

  new iso create(redis': Redis) =>
    _redis = redis'
    _ctx = None

  fun ref connected(conn: TCPConnection ref) =>
    _redis._connected(conn)

  fun ref received(conn: TCPConnection ref, data: Array[U8] iso, times: USize) : Bool =>
    var data': Array[U8] ref = consume data
    while true do
      let result = match (_ctx)
                   | let ctx': _DecodeContext ref => RespDecoder.cont(ctx', data')
                   else
                     RespDecoder(data')
                   end
      match result
      | (_DecodeDone, (let v: RedisValue val, let r: Array[U8])) =>
        _redis._received(v)
        data' = r
        _ctx = None
      | (_DecodeContinueWithCr, let ctx: _DecodeContext) =>
        _ctx = ctx
        break
      | (_DecodeContinue, let ctx: _DecodeContext) =>
        _ctx = ctx
        break
      | (_DecodeError, let e: String) =>
        _redis._received_error(e)
        break
      end
    end
    true

  fun ref connect_failed(conn: TCPConnection ref) =>
    None
