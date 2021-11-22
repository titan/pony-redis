use "buffered"
use "collections"

primitive _DecodeDone

primitive _DecodeContinue

primitive _DecodeContinueWithCr

primitive _DecodeError

type _DecodeResultType is ((_DecodeDone, (RedisValue val, Array[U8] ref)) | (_DecodeContinue, _DecodeContext) | (_DecodeContinueWithCr, _DecodeContext) | (_DecodeError, String))

type _InnerDecodeResultType[T: Any val] is ((_DecodeDone, (T, Array[U8] ref)) | (_DecodeContinueWithCr, Array[U8] iso) | (_DecodeContinue, Array[U8] iso))

class _DecodeContextFrame
  let target: (None | RedisKind)
  let acc: (None | Array[U8] iso^)
  let items: Array[RedisValue val] iso^
  let expect_len: USize
  let rest_len: USize
  let cr: Bool
  new create(target': (None | RedisKind), acc': (None | Array[U8] iso^), items': Array[RedisValue val] iso^, expect_len': USize, rest_len': USize, cr': Bool) =>
    target = target'
    acc = acc'
    items = items'
    expect_len = expect_len'
    rest_len = rest_len'
    cr = cr'

class _DecodeContext
  var target: (None | RedisKind)
  var acc: (None | Array[U8] iso^)
  var items: Array[RedisValue val] iso^
  var expect_len: USize
  var rest_len: USize
  var cr: Bool
  let stack: Array[_DecodeContextFrame]

  new create(target': (None | RedisKind) = None, acc': (None | Array[U8] iso^) = None, cr': Bool = false) =>
    target = target'
    acc = acc'
    items = recover [] end
    expect_len = 0
    rest_len = 0
    cr = cr'
    stack = []

  fun ref push_item(item: RedisValue val) =>
    items.push(item)

  fun ref push_frame() =>
    stack.push(_DecodeContextFrame(target, acc, items, expect_len, rest_len, cr))
    target = None
    acc = None
    items = []
    expect_len = 0
    rest_len = 0
    cr = false

  fun ref pop_frame() =>
    try
      let frame: _DecodeContextFrame = stack.pop()?
      target = frame.target
      acc = frame.acc
      items = frame.items
      expect_len = frame.expect_len
      rest_len = frame.rest_len
      cr = frame.cr
    end

  fun ref stack_empty(): Bool val =>
    stack.size() == 0

primitive RespDecoder
  fun apply(data: Array[U8] ref): _DecodeResultType =>
    cont(_DecodeContext(), data)

  fun cont(ctx: _DecodeContext, data: Array[U8] ref): _DecodeResultType =>
    if ctx.cr then
      data.unshift('\r')
    end
    match ctx.target
    | RedisBlobString =>
      if ctx.expect_len == 0 then
        try
          let acc': Array[U8] iso^ = if ctx.acc is None then recover Array[U8] end else ctx.acc as Array[U8] iso^ end
          match _until_crlf[USize val, _FromBytesToUSize](data, acc')?
          | (_DecodeDone, (let len: USize val, let rest''': Array[U8] ref)) =>
            try
              match _decode_fixed_size_string(rest''', len, recover Array[U8](len) end)?
              | (_DecodeDone, (let result': String val, let rest: Array[U8] ref)) =>
                if ctx.stack_empty() then
                  (_DecodeDone, (RedisValue.blob_string(result'), rest))
                else
                  ctx.pop_frame()
                  ctx.push_item(RedisValue.blob_string(result'))
                  ctx.rest_len = ctx.rest_len - 1
                  if ctx.rest_len == 0 then
                    (_DecodeDone, (RedisValue.array(ctx.items), rest))
                  else
                    ctx.push_frame()
                    cont(ctx, rest)
                  end
                end
              | (_DecodeContinueWithCr, let acc: Array[U8] iso^) =>
                ctx.expect_len = len
                ctx.rest_len = len - acc.size()
                ctx.acc = acc
                ctx.cr = true
                (_DecodeContinueWithCr, ctx)
              | (_DecodeContinue, let acc: Array[U8] iso^) =>
                ctx.expect_len = len
                ctx.rest_len = len - acc.size()
                ctx.acc = acc
                ctx.cr = false
                (_DecodeContinue, ctx)
              else
                (_DecodeError, "Unknown error")
              end
            else
              (_DecodeError, "Decoding blob-string(" + len.string() + ") error")
            end
          | (_DecodeContinueWithCr, let acc: Array[U8] iso^) =>
            ctx.acc = acc
            ctx.cr = true
            (_DecodeContinueWithCr, ctx)
          | (_DecodeContinue, let acc: Array[U8] iso^) =>
            ctx.acc = acc
            ctx.cr = false
            (_DecodeContinue, ctx)
          else
            (_DecodeError, "Unknown error")
          end
        else
          (_DecodeError, "Decoding length of blob-string error")
        end
      else
        try
          let acc': Array[U8] iso^ = if ctx.acc is None then recover iso Array[U8](ctx.expect_len) end else ctx.acc as Array[U8] iso^ end
          match _decode_fixed_size_string(data, ctx.rest_len, acc')?
          | (_DecodeDone, (let result': String val, let rest: Array[U8] ref)) =>
            if ctx.stack_empty() then
              (_DecodeDone, (RedisValue.blob_string(result'), rest))
            else
              ctx.pop_frame()
              ctx.push_item(RedisValue.blob_string(result'))
              ctx.rest_len = ctx.rest_len - 1
              if ctx.rest_len == 0 then
                (_DecodeDone, (RedisValue.array(ctx.items), rest))
              else
                ctx.push_frame()
                cont(ctx, rest)
              end
            end
          | (_DecodeContinueWithCr, let acc: Array[U8] iso^) =>
            ctx.rest_len = ctx.rest_len - acc.size()
            ctx.acc = acc
            ctx.cr = true
            (_DecodeContinueWithCr, ctx)
          | (_DecodeContinue, let acc: Array[U8] iso^) =>
            ctx.rest_len = ctx.rest_len - acc.size()
            ctx.acc = acc
            ctx.cr = false
            (_DecodeContinue, ctx)
          else
            (_DecodeError, "Unknown error")
          end
        else
          (_DecodeError, "Decoding blob-string error")
        end
      end
    | RedisSimpleString =>
      try
        let acc': Array[U8] iso^ = if ctx.acc is None then recover iso Array[U8] end else ctx.acc as Array[U8] iso^ end
        match _until_crlf[String val, _FromBytesToString](data, acc')?
        | (_DecodeDone, (let result': String val, let rest: Array[U8] ref)) =>
          if ctx.stack_empty() then
            (_DecodeDone, (RedisValue.simple_string(result'), rest))
          else
            ctx.pop_frame()
            ctx.push_item(RedisValue.simple_string(result'))
            ctx.rest_len = ctx.rest_len - 1
            if ctx.rest_len == 0 then
              (_DecodeDone, (RedisValue.array(ctx.items), rest))
            else
              ctx.push_frame()
              cont(ctx, rest)
            end
          end
        | (_DecodeContinueWithCr, let acc: Array[U8] iso^) =>
          ctx.acc = acc
          ctx.cr = true
          (_DecodeContinueWithCr, ctx)
        | (_DecodeContinue, let acc: Array[U8] iso^) =>
          ctx.acc = acc
          ctx.cr = false
          (_DecodeContinue, ctx)
        else
          (_DecodeError, "Unknown error")
        end
      else
        (_DecodeError, "Decoding simple-string error")
      end
    | RedisSimpleError =>
      try
        let acc': Array[U8] iso^ = if ctx.acc is None then recover iso Array[U8] end else ctx.acc as Array[U8] iso^ end
        match _until_crlf[String val, _FromBytesToString](data, acc')?
        | (_DecodeDone, (let result': String val, let rest: Array[U8] ref)) =>
          if ctx.stack_empty() then
            (_DecodeDone, (RedisValue.simple_error(result'), rest))
          else
            ctx.pop_frame()
            ctx.push_item(RedisValue.simple_error(result'))
            ctx.rest_len = ctx.rest_len - 1
            if ctx.rest_len == 0 then
              (_DecodeDone, (RedisValue.array(ctx.items), rest))
            else
              ctx.push_frame()
              cont(ctx, rest)
            end
          end
        | (_DecodeContinueWithCr, let acc: Array[U8] iso^) =>
          ctx.acc = acc
          ctx.cr = true
          (_DecodeContinueWithCr, ctx)
        | (_DecodeContinue, let acc: Array[U8] iso^) =>
          ctx.acc = acc
          ctx.cr = false
          (_DecodeContinue, ctx)
        else
          (_DecodeError, "Unknown error")
        end
      else
        (_DecodeError, "Decoding simple-error error")
      end
    | RedisNumber =>
      try
        let acc': Array[U8] iso^ = if ctx.acc is None then recover iso Array[U8] end else ctx.acc as Array[U8] iso^ end
        match _until_crlf[I64 val, _FromBytesToInt](data, acc')?
        | (_DecodeDone, (let result': I64 val, let rest: Array[U8] ref)) =>
          if ctx.stack_empty() then
            (_DecodeDone, (RedisValue.number(result'), rest))
          else
            ctx.pop_frame()
            ctx.push_item(RedisValue.number(result'))
            ctx.rest_len = ctx.rest_len - 1
            if ctx.rest_len == 0 then
              (_DecodeDone, (RedisValue.array(ctx.items), rest))
            else
              ctx.push_frame()
              cont(ctx, rest)
            end
          end
        | (_DecodeContinueWithCr, let acc: Array[U8] iso^) =>
          ctx.acc = acc
          ctx.cr = true
          (_DecodeContinueWithCr, ctx)
        | (_DecodeContinue, let acc: Array[U8] iso^) =>
          ctx.acc = acc
          ctx.cr = false
          (_DecodeContinue, ctx)
        else
          (_DecodeError, "Unknown error")
        end
      else
        (_DecodeError, "Decoding number error")
      end
    | RedisArray =>
      if ctx.expect_len == 0 then
        try
          let acc': Array[U8] iso^ = if ctx.acc is None then recover Array[U8] end else ctx.acc as Array[U8] iso^ end
          match _until_crlf[USize val, _FromBytesToUSize](data, acc')?
          | (_DecodeDone, (let len: USize val, let rest': Array[U8] ref)) =>
            ctx.expect_len = len
            ctx.rest_len = len
            ctx.push_frame()
            match cont(ctx, rest')
            | (_DecodeDone, (let result': RedisValue val, let rest: Array[U8] ref)) =>
              if ctx.stack_empty() then
                (_DecodeDone, (result', rest))
              else
                ctx.pop_frame()
                ctx.push_item(result')
                ctx.rest_len = ctx.rest_len - 1
                if ctx.rest_len == 0 then
                  (_DecodeDone, (RedisValue.array(ctx.items), rest))
                else
                  ctx.push_frame()
                  cont(ctx, rest)
                end
              end
            | (_DecodeContinue, _) => (_DecodeContinue, ctx)
            | (_DecodeContinueWithCr, _) => (_DecodeContinueWithCr, ctx)
            | (_DecodeError, let e: String) => (_DecodeError, e)
            end
          | (_DecodeContinueWithCr, let acc: Array[U8] iso^) =>
            ctx.acc = acc
            ctx.cr = true
            (_DecodeContinueWithCr, ctx)
          | (_DecodeContinue, let acc: Array[U8] iso^) =>
            ctx.acc = acc
            ctx.cr = false
            (_DecodeContinue, ctx)
          else
            (_DecodeError, "Unknown error")
          end
        else
          (_DecodeError, "Decoding length of array error")
        end
      else
        var rest': Array[U8] ref = data
        match cont(ctx, rest')
        | (_DecodeDone, (let result': RedisValue val, let rest: Array[U8] ref)) =>
          if ctx.stack_empty() then
            (_DecodeDone, (result', rest))
          else
            ctx.pop_frame()
            ctx.push_item(result')
            ctx.rest_len = ctx.rest_len - 1
            if ctx.rest_len == 0 then
              (_DecodeDone, (RedisValue.array(ctx.items), rest))
            else
              ctx.push_frame()
              cont(ctx, rest)
            end
          end
        | (_DecodeContinue, _) => (_DecodeContinue, ctx)
        | (_DecodeContinueWithCr, _) => (_DecodeContinueWithCr, ctx)
        | (_DecodeError, let e: String) => (_DecodeError, e)
        end
      end
    else
      var head: U8 = 0
      try
        head = data.shift()?
      else
        return (_DecodeContinue, ctx)
      end
      match head
      | '$' =>
        ctx.target = RedisBlobString
        cont(ctx, data)
      | '+' =>
        ctx.target = RedisSimpleString
        cont(ctx, data)
      | '-' =>
        ctx.target = RedisSimpleError
        cont(ctx, data)
      | ':' =>
        ctx.target = RedisNumber
        cont(ctx, data)
      | '*' =>
        ctx.target = RedisArray
        cont(ctx, data)
      else
        (_DecodeError, "Unsupported data type which starts with " + String.from_array([head]))
      end
    end

  fun _until_crlf[T: Any val, C: _FromBytesTo[T]](data: Array[U8] ref, acc: Array[U8] iso^): _InnerDecodeResultType[T] ? =>
    let first: (U8 | None) = try data.shift()? else None end
    let second: (U8 | None) = try data.shift()? else None end
    match (first, second, data.size())
    | ('\r', '\n', _) => (_DecodeDone, (C(acc)?, data))
    | ('\r', None, 0) => (_DecodeContinueWithCr, acc)
    | (None, None, _) => (_DecodeContinue, acc)
    | (let f: U8, None, 0) => acc.push_u8(f); (_DecodeContinue, acc)
    | (let f: U8, '\r', _) => acc.push_u8(f); data.unshift('\r'); _until_crlf[T, C](data, acc)?
    | (let f: U8, let s: U8, _) => acc.push_u8(f); acc.push_u8(s); _until_crlf[T, C](data, acc)?
    else
      error
    end

  fun _decode_fixed_size_string(data: Array[U8] ref, len: USize, acc: Array[U8] iso^): _InnerDecodeResultType[String] ? =>
    let first: (U8 | None) = try data.shift()? else None end
    let second: (U8 | None) = try data.shift()? else None end
    match (first, second, data.size(), len)
    | ('\r', '\n', _, 0) => (_DecodeDone, (_FromBytesToString(acc), data))
    | ('\r', None, _, 0) => (_DecodeContinueWithCr, acc)
    | (None, None, _, _) => (_DecodeContinue, acc)
    | (let f: U8, None, 0, _) => acc.push_u8(f); (_DecodeContinue, acc)
    | (let f: U8, '\r', _, _) => acc.push_u8(f); data.unshift('\r'); _decode_fixed_size_string(data, len - 1, acc)?
    | (let f: U8, let s: U8, _, _) => acc.push_u8(f); acc.push_u8(s); _decode_fixed_size_string(data, len - 2, acc)?
    else
      error
    end

trait val _FromBytesTo[A: Any val]
  new val create()
  fun val apply(bytes: Array[U8] val): A ?

primitive _FromBytesToString is _FromBytesTo[String val]
  fun val apply(bytes: Array[U8] val): String val =>
    String.from_array(bytes)

primitive _FromBytesToUSize is _FromBytesTo[USize val]
  fun val apply(bytes: Array[U8] val): USize val ? =>
    String.from_array(bytes).usize()?

primitive _FromBytesToInt is _FromBytesTo[I64 val]
  fun val apply(bytes: Array[U8] val): I64 val ? =>
    String.from_array(bytes).i64()?
