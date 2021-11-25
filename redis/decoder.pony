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
  let acc_expect_len: USize
  let acc_rest_len: USize
  let items: Array[RedisValue val] iso^
  let items_expect_len: USize
  let items_rest_len: USize
  let cr: Bool
  new create(target': (None | RedisKind), acc': (None | Array[U8] iso^), acc_expect_len': USize, acc_rest_len': USize, items': Array[RedisValue val] iso^, items_expect_len': USize, items_rest_len': USize, cr': Bool) =>
    target = target'
    acc = acc'
    acc_expect_len = acc_expect_len'
    acc_rest_len = acc_rest_len'
    items = items'
    items_expect_len = items_expect_len'
    items_rest_len = items_rest_len'
    cr = cr'

class _DecodeContext
  var target: (None | RedisKind)
  var acc: (None | Array[U8] iso^)
  var acc_expect_len: USize
  var acc_rest_len: USize
  var items: Array[RedisValue val] iso^
  var items_expect_len: USize
  var items_rest_len: USize
  var cr: Bool
  let stack: Array[_DecodeContextFrame]

  new create(target': (None | RedisKind) = None, acc': (None | Array[U8] iso^) = None, cr': Bool = false) =>
    target = target'
    acc = acc'
    acc_expect_len = 0
    acc_rest_len = 0
    items = recover [] end
    items_expect_len = 0
    items_rest_len = 0
    cr = cr'
    stack = []

  fun ref push_item(item: RedisValue val) =>
    items.push(item)

  fun ref push_frame() =>
    stack.push(_DecodeContextFrame(target, acc, acc_expect_len, acc_rest_len, items, items_expect_len, items_rest_len, cr))
    target = None
    acc = None
    acc_expect_len = 0
    acc_rest_len = 0
    items = []
    items_expect_len = 0
    items_rest_len = 0
    cr = false

  fun ref pop_frame() =>
    try
      let frame: _DecodeContextFrame = stack.pop()?
      target = frame.target
      acc = frame.acc
      acc_expect_len = frame.acc_expect_len
      acc_rest_len = frame.acc_rest_len
      items = frame.items
      items_expect_len = frame.items_expect_len
      items_rest_len = frame.items_rest_len
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
      if ctx.acc_expect_len == 0 then
        try
          let acc': Array[U8] iso^ = if ctx.acc is None then recover Array[U8] end else ctx.acc as Array[U8] iso^ end
          match _until_crlf[I64 val, _FromBytesToInt](data, acc')?
          | (_DecodeDone, (let len: I64 val, let rest: Array[U8] ref)) =>
            if len == -1 then
              (_DecodeDone, (RedisValue.null(), rest))
            else
              try
                match _decode_fixed_size_string(rest, len.usize(), recover Array[U8](len.usize()) end)?
                | (_DecodeDone, (let result': String val, let rest': Array[U8] ref)) =>
                  _push_to_array_or_just_return(ctx, RedisValue.blob_string(result'), rest')
                | (_DecodeContinueWithCr, let acc: Array[U8] iso^) =>
                  ctx.acc_expect_len = len.usize()
                  ctx.acc_rest_len = len.usize() - acc.size()
                  ctx.acc = acc
                  ctx.cr = true
                  (_DecodeContinueWithCr, ctx)
                | (_DecodeContinue, let acc: Array[U8] iso^) =>
                  ctx.acc_expect_len = len.usize()
                  ctx.acc_rest_len = len.usize() - acc.size()
                  ctx.acc = acc
                  ctx.cr = false
                  (_DecodeContinue, ctx)
                else
                  (_DecodeError, "Unknown error")
                end
              else
                (_DecodeError, "Decoding blob-string(" + len.string() + ") error")
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
          (_DecodeError, "Decoding length of blob-string error")
        end
      else
        try
          let acc': Array[U8] iso^ = if ctx.acc is None then recover iso Array[U8](ctx.acc_expect_len) end else ctx.acc as Array[U8] iso^ end
          match _decode_fixed_size_string(data, ctx.acc_rest_len, acc')?
          | (_DecodeDone, (let result': String val, let rest: Array[U8] ref)) =>
            _push_to_array_or_just_return(ctx, RedisValue.blob_string(result'), rest)
          | (_DecodeContinueWithCr, let acc: Array[U8] iso^) =>
            ctx.acc_rest_len = ctx.acc_rest_len - acc.size()
            ctx.acc = acc
            ctx.cr = true
            (_DecodeContinueWithCr, ctx)
          | (_DecodeContinue, let acc: Array[U8] iso^) =>
            ctx.acc_rest_len = ctx.acc_rest_len - acc.size()
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
          _push_to_array_or_just_return(ctx, RedisValue.simple_string(result'), rest)
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
          _push_to_array_or_just_return(ctx, RedisValue.simple_error(result'), rest)
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
          _push_to_array_or_just_return(ctx, RedisValue.number(result'), rest)
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
      if ctx.items_expect_len == 0 then
        try
          let acc': Array[U8] iso^ = if ctx.acc is None then recover Array[U8] end else ctx.acc as Array[U8] iso^ end
          match _until_crlf[I64 val, _FromBytesToInt](data, acc')?
          | (_DecodeDone, (let len: I64 val, let rest: Array[U8] ref)) =>
            if len == -1 then
              _push_to_array_or_just_return(ctx, RedisValue.null(), rest)
            else
              ctx.items_expect_len = len.usize()
              ctx.items_rest_len = len.usize()
              ctx.push_frame()
              match cont(ctx, rest)
              | (_DecodeDone, (let result': RedisValue val, let rest': Array[U8] ref)) =>
                _push_to_array_or_just_return(ctx, result', rest')
              | (_DecodeContinue, _) => (_DecodeContinue, ctx)
              | (_DecodeContinueWithCr, _) => (_DecodeContinueWithCr, ctx)
              | (_DecodeError, let e: String) => (_DecodeError, e)
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
          (_DecodeError, "Decoding length of array error")
        end
      else
        var rest: Array[U8] ref = data
        ctx.push_frame()
        match cont(ctx, rest)
        | (_DecodeDone, (let result': RedisValue val, let rest': Array[U8] ref)) =>
          _push_to_array_or_just_return(ctx, result', rest')
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

  fun _push_to_array_or_just_return(ctx: _DecodeContext, value: RedisValue val, rest: Array[U8] ref): _DecodeResultType =>
    if ctx.stack_empty() then
      (_DecodeDone, (value, rest))
    else
      ctx.pop_frame()
      ctx.items.push(value)
      ctx.items_rest_len = ctx.items_rest_len - 1
      if ctx.items_rest_len == 0 then
        _push_to_array_or_just_return(ctx, RedisValue.array(ctx.items), rest)
      else
        ctx.push_frame()
        cont(ctx, rest)
      end
    end

trait val _FromBytesTo[A: Any val]
  new val create()
  fun val apply(bytes: Array[U8] val): A ?

primitive _FromBytesToString is _FromBytesTo[String val]
  fun val apply(bytes: Array[U8] val): String val =>
    String.from_array(bytes)

primitive _FromBytesToInt is _FromBytesTo[I64 val]
  fun val apply(bytes: Array[U8] val): I64 val ? =>
    String.from_array(bytes).i64()?
