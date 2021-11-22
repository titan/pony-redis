use "ponytest"
use "collections"

class iso _TestBlobString is UnitTest
  fun name(): String => "blob-string"

  fun apply(h: TestHelper) =>
    _test_encode_then_decode(h)
    _test_decode_partially1(h)
    _test_decode_partially2(h)
    _test_decode_partially3(h)
    _test_decode_partially4(h)

  fun _test_encode_then_decode(h: TestHelper) =>
    h.expect_action("blob-string-encode-then-decode")
    let entity = RedisValue.blob_string("hello\r\nworld")
    let msg = "Decoded result is not a blog-string"
    try
      let bytes: Array[U8] val = RespEncoder(consume entity)?.array()
      let bytes': Array[U8] ref = recover ref Array[U8](bytes.size()) end
      bytes'.copy_from(bytes, 0, 0, bytes.size())
      match RespDecoder(bytes')
      | (_DecodeDone, (let v: RedisValue val, let r: Array[U8] ref)) =>
        try
          let c = v.get_string()?
          h.assert_eq[String](c, "hello\r\nworld", msg)
          h.complete_action("blob-string-encode-then-decode")
        else
          h.fail(msg)
        end
      | (_DecodeContinue, let ctx: _DecodeContext) =>
        match ctx.acc
        | let acc': Array[U8] iso^ =>
          h.fail("Decoding is not completed, acc: " + String.from_iso_array(acc'))
        else
          h.fail("Decoding is not completed")
        end
      | (_DecodeContinueWithCr, _) => h.fail("Decoding is not completed, missing LF")
      | (_DecodeError, let e: String) => h.fail(e)
      end
    else
      h.fail()
    end

  fun _test_decode_partially1(h: TestHelper) =>
    h.expect_action("blob-string-decode-partially-1")
    let msg = "Decoded result is not a blob-string"
    let bytes1: Array[U8] ref = recover ref Array[U8](5) end
    bytes1.append("$12\r\n".array())
    let bytes2: Array[U8] ref = recover ref Array[U8](14) end
    bytes2.append("hello\r\nworld\r\n".array())
    match RespDecoder(bytes1)
    | (_DecodeContinue, let ctx1: _DecodeContext) =>
      if ctx1.expect_len > 0 then
        h.assert_eq[USize](ctx1.expect_len, 12, "Length of blob-string is not 12")
        match RespDecoder.cont(ctx1, bytes2)
        | (_DecodeDone, (let v: RedisValue val, let r: Array[U8] ref)) =>
          try
            let c = v.get_string()?
            h.assert_eq[String](c, "hello\r\nworld", msg)
            h.complete_action("blob-string-decode-partially-1")
          else
            h.fail(msg)
          end
        | (_DecodeContinue, let ctx2: _DecodeContext) =>
          match ctx2.acc
          | let acc2: Array[U8] iso^ =>
            h.fail("Decoding is not completed, acc: " + String.from_iso_array(acc2))
          else
            h.fail("Decoding is not completed")
          end
        | (_DecodeContinueWithCr, _) => h.fail("Decoding is not completed, missing LF")
        | (_DecodeError, let e: String) => h.fail(e)
        end
      else
        h.fail("Decoding length of blob-string error")
      end
    else
      h.fail()
    end

  fun _test_decode_partially2(h: TestHelper) =>
    h.expect_action("blob-string-decode-partially-2")
    let msg = "Decoded result is not a blob-string"
    let bytes1: Array[U8] ref = recover ref Array[U8](17) end
    bytes1.append("$12\r\nhello\r\nworld".array())
    let bytes2: Array[U8] ref = recover ref Array[U8](2) end
    bytes2.append("\r\n".array())
    match RespDecoder(bytes1)
    | (_DecodeContinue, let ctx1: _DecodeContext) =>
      if ctx1.expect_len > 0 then
        h.assert_eq[USize](ctx1.expect_len, 12, "Length of blob-string is not 12")
        match RespDecoder.cont(ctx1, bytes2)
        | (_DecodeDone, (let v: RedisValue val, let r: Array[U8] ref)) =>
          try
            let c = v.get_string()?
            h.assert_eq[String](c, "hello\r\nworld", msg)
            h.complete_action("blob-string-decode-partially-2")
          else
            h.fail(msg)
          end
        | (_DecodeContinue, let ctx2: _DecodeContext) =>
          match ctx2.acc
          | let acc2: Array[U8] iso^ =>
            h.fail("Decoding is not completed, acc: " + String.from_iso_array(acc2))
          else
            h.fail("Decoding is not completed")
          end
        | (_DecodeContinueWithCr, _) => h.fail("Decoding is not completed, missing LF")
        | (_DecodeError, let e: String) => h.fail(e)
        end
      else
        h.fail("Decoding length of blob-string error")
      end
    else
      h.fail()
    end

  fun _test_decode_partially3(h: TestHelper) =>
    h.expect_action("blob-string-decode-partially-3")
    let msg = "Decoded result is not a blob-string"
    let bytes1: Array[U8] ref = recover ref Array[U8](2) end
    bytes1.append("$1".array())
    let bytes2: Array[U8] ref = recover ref Array[U8](17) end
    bytes2.append("2\r\nhello\r\nworld\r\n".array())
    match RespDecoder(bytes1)
    | (_DecodeContinue, let ctx1: _DecodeContext) =>
      if ctx1.expect_len == 0 then
        match RespDecoder.cont(ctx1, bytes2)
        | (_DecodeDone, (let v: RedisValue val, let r: Array[U8] ref)) =>
          try
            let c = v.get_string()?
            h.assert_eq[String](c, "hello\r\nworld", msg)
            h.complete_action("blob-string-decode-partially-3")
          else
            h.fail(msg)
          end
        | (_DecodeContinue, let ctx2: _DecodeContext) =>
          match ctx2.acc
          | let acc2: Array[U8] iso^ =>
            h.fail("Decoding is not completed, acc: " + String.from_iso_array(acc2))
          else
            h.fail("Decoding is not completed")
          end
        | (_DecodeContinueWithCr, _) => h.fail("Decoding is not completed, missing LF")
        | (_DecodeError, let e: String) => h.fail(e)
        end
      else
        h.fail("Decoding length of blob-string error")
      end
    else
      h.fail()
    end

  fun _test_decode_partially4(h: TestHelper) =>
    h.expect_action("blob-string-decode-partially-4")
    let msg = "Decoded result is not a blob-string"
    let bytes1: Array[U8] ref = recover ref Array[U8](18) end
    bytes1.append("$12\r\nhello\r\nworld\r".array())
    let bytes2: Array[U8] ref = recover ref Array[U8](1) end
    bytes2.append("\n".array())
    match RespDecoder(bytes1)
    | (_DecodeContinueWithCr, let ctx1: _DecodeContext) =>
      if ctx1.expect_len > 0 then
        h.assert_eq[USize](ctx1.expect_len, 12, "Length of blob-string is not 12")
        match RespDecoder.cont(ctx1, bytes2)
        | (_DecodeDone, (let v: RedisValue val, let r: Array[U8] ref)) =>
          try
            let c = v.get_string()?
            h.assert_eq[String](c, "hello\r\nworld", msg)
            h.complete_action("blob-string-decode-partially-4")
          else
            h.fail(msg)
          end
        | (_DecodeContinue, let ctx2: _DecodeContext) =>
          match ctx2.acc
          | let acc2: Array[U8] iso^ =>
            h.fail("Decoding is not completed, acc: " + String.from_iso_array(acc2))
          else
            h.fail("Decoding is not completed")
          end
        | (_DecodeContinueWithCr, _) => h.fail("Decoding is not completed, missing LF")
        | (_DecodeError, let e: String) => h.fail(e)
        end
      else
        h.fail("Decoding length of blob-string error")
      end
    else
      h.fail()
    end

class iso _TestSimpleString is UnitTest
  fun name(): String => "simple-string"

  fun apply(h: TestHelper) =>
    _test_encode_then_decode(h)
    _test_decode_partially(h)

  fun _test_encode_then_decode(h: TestHelper) =>
    h.expect_action("simple-string-encode-then-decode")
    let entity = RedisValue.simple_string("hello world")
    let msg = "Decoded result is not a simple-string"
    try
      let bytes: Array[U8] val = RespEncoder(consume entity)?.array()
      let bytes': Array[U8] ref = recover ref Array[U8](bytes.size()) end
      bytes'.copy_from(bytes, 0, 0, bytes.size())
      match RespDecoder(bytes')
      | (_DecodeDone, (let v: RedisValue val, let r: Array[U8] ref)) =>
        try
          let c = v.get_string()?
          h.assert_eq[String](c, "hello world", msg)
          h.complete_action("simple-string-encode-then-decode")
        else
          h.fail(msg)
        end
      | (_DecodeContinue, let ctx: _DecodeContext) =>
        match ctx.acc
        | let acc': Array[U8] iso^ =>
          h.fail("Decoding is not completed, acc: " + String.from_iso_array(acc'))
        else
          h.fail("Decoding is not completed")
        end
      | (_DecodeContinueWithCr, _) => h.fail("Decoding is not completed, missing LF")
      | (_DecodeError, let e: String) => h.fail(e)
      end
    else
      h.fail()
    end

  fun _test_decode_partially(h: TestHelper) =>
    h.expect_action("simple-string-decode-partially")
    let msg = "Decoded result is not a simple-string"
    let bytes1: Array[U8] ref = recover ref Array[U8](12) end
    bytes1.append("+hello world".array())
    let bytes2: Array[U8] ref = recover ref Array[U8](2) end
    bytes2.append("\r\n".array())
    match RespDecoder(bytes1)
    | (_DecodeContinue, let ctx1: _DecodeContext) =>
      match ctx1.acc
      | let acc': Array[U8] iso^ =>
        h.assert_eq[String](String.from_iso_array(acc'), "hello world", "Acc of first time is not 'hello world'")
        match RespDecoder.cont(ctx1, bytes2)
        | (_DecodeDone, (let v: RedisValue val, let r: Array[U8] ref)) =>
          try
            let c = v.get_string()?
            h.assert_eq[String](c, "hello world", msg)
            h.complete_action("simple-string-decode-partially")
          else
            h.fail(msg)
          end
        | (_DecodeContinue, let ctx2: _DecodeContext) =>
          match ctx2.acc
          | let acc'': Array[U8] iso^ =>
            h.fail("Decoding is not completed, acc: " + String.from_iso_array(acc''))
          else
            h.fail("Decoding is not completed")
          end
        | (_DecodeContinueWithCr, _) => h.fail("Decoding is not completed, missing LF")
        | (_DecodeError, let e: String) => h.fail(e)
        end
      else
        h.fail()
      end
    else
      h.fail()
    end

class iso _TestSimpleError is UnitTest
  fun name(): String => "simple-error"

  fun apply(h: TestHelper) =>
    _test_encode_then_decode(h)

  fun _test_encode_then_decode(h: TestHelper) =>
    h.expect_action("simple-error-encode-then-decode")
    let entity = RedisValue.simple_error("hello world")
    let msg = "Decoded result is not a simple-error"
    try
      let bytes: Array[U8] val = RespEncoder(consume entity)?.array()
      let bytes': Array[U8] ref = recover ref Array[U8](bytes.size()) end
      bytes'.copy_from(bytes, 0, 0, bytes.size())
      match RespDecoder(bytes')
      | (_DecodeDone, (let v: RedisValue val, let r: Array[U8] ref)) =>
        try
          let c = v.get_string()?
          h.assert_eq[String](c, "hello world", msg)
          h.complete_action("simple-error-encode-then-decode")
        else
          h.fail(msg)
        end
      | (_DecodeContinue, let ctx: _DecodeContext) =>
        match ctx.acc
        | let acc': Array[U8] iso^ =>
          h.fail("Decoding is not completed, acc: " + String.from_iso_array(acc'))
        else
          h.fail("Decoding is not completed")
        end
      | (_DecodeContinueWithCr, _) => h.fail("Decoding is not completed, missing LF")
      | (_DecodeError, let e: String) => h.fail(e)
      end
    else
      h.fail()
    end

class iso _TestNumber is UnitTest
  fun name(): String => "number"

  fun apply(h: TestHelper) =>
    _test_encode_then_decode(h)

  fun _test_encode_then_decode(h: TestHelper) =>
    h.expect_action("number-encode-then-decode")
    let entity = RedisValue.number(-1)
    let msg = "Decoded result is not a number"
    try
      let bytes: Array[U8] val = RespEncoder(consume entity)?.array()
      let bytes': Array[U8] ref = recover ref Array[U8](bytes.size()) end
      bytes'.copy_from(bytes, 0, 0, bytes.size())
      match RespDecoder(bytes')
      | (_DecodeDone, (let v: RedisValue val, let r: Array[U8] ref)) =>
        try
          let n = v.get_number()?
          h.assert_eq[I64](n, -1, msg)
          h.complete_action("number-encode-then-decode")
        else
          h.fail(msg)
        end
      | (_DecodeContinue, let ctx: _DecodeContext) =>
        match ctx.acc
        | let acc': Array[U8] iso^ =>
          h.fail("Decoding is not completed, acc: " + String.from_iso_array(acc'))
        else
          h.fail("Decoding is not completed")
        end
      | (_DecodeContinueWithCr, _) => h.fail("Decoding is not completed, missing LF")
      | (_DecodeError, let e: String) => h.fail(e)
      end
    else
      h.fail()
    end

class iso _TestArray is UnitTest
  fun name(): String => "array"

  fun apply(h: TestHelper) =>
    _test_encode_then_decode1(h)
    _test_encode_then_decode2(h)
    _test_decode_partially1(h)

  fun _test_encode_then_decode1(h: TestHelper) =>
    h.expect_action("array-encode-then-decode-1")
    let entity = RedisValue.array([RedisValue.blob_string("hello world"); RedisValue.simple_string("hello world")])
    try
      let bytes: Array[U8] val = RespEncoder(consume entity)?.array()
      let bytes': Array[U8] ref = recover ref Array[U8](bytes.size()) end
      bytes'.copy_from(bytes, 0, 0, bytes.size())
      let msg = "Decoded result is not an array"
      match RespDecoder(bytes')
      | (_DecodeDone, (let v: RedisValue val, let r: Array[U8] ref)) =>
        try
          let a = v.get_array()?
          h.assert_true(a.size() == 2, "Length of array should be 2, but got " + a.size().string())
          let a0 = a(0)?.get_string()?
          h.assert_eq[String](a0, "hello world", "The first item should be 'hello world', but got " + a0)
          let a1 = a(1)?.get_string()?
          h.assert_eq[String](a1, "hello world", "The second item should be 'hello world', but got " + a1)
          h.complete_action("array-encode-then-decode-1")
        else
          h.fail(msg)
        end
      | (_DecodeContinue, let ctx: _DecodeContext) =>
        match ctx.acc
        | let acc': Array[U8] iso^ =>
          h.fail("Decoding is not completed, acc: " + String.from_iso_array(acc'))
        else
          h.fail("Decoding is not completed")
        end
      | (_DecodeContinueWithCr, _) => h.fail("Decoding is not completed, missing LF")
      | (_DecodeError, let e: String) => h.fail(e)
      end
    else
      h.fail()
    end

  fun _test_encode_then_decode2(h: TestHelper) =>
    h.expect_action("array-encode-then-decode-2")
    let entity = RedisValue.array([RedisValue.array([RedisValue.simple_string("hello world")])])
    try
      let bytes: Array[U8] val = RespEncoder(consume entity)?.array()
      let bytes': Array[U8] ref = recover ref Array[U8](bytes.size()) end
      bytes'.copy_from(bytes, 0, 0, bytes.size())
      let msg = "Decoded result is not an array"
      match RespDecoder(bytes')
      | (_DecodeDone, (let v: RedisValue val, let r: Array[U8] ref)) =>
        try
          let a = v.get_array()?
          h.assert_true(a.size() == 1, "Length of array should be 1, but got " + a.size().string())
          let a0 = a(0)?.get_array()?
          h.assert_true(a0.size() == 1, "Length of array should be 1, but got " + a0.size().string())
          let a0' = a0(0)?.get_string()?
          h.assert_eq[String](a0', "hello world", "The first item should be 'hello world', but got " + a0')
          h.complete_action("array-encode-then-decode-2")
        else
          h.fail(msg)
        end
      | (_DecodeContinue, let ctx: _DecodeContext) =>
        match ctx.acc
        | let acc': Array[U8] iso^ =>
          h.fail("Decoding is not completed, acc: " + String.from_iso_array(acc'))
        else
          h.fail("Decoding is not completed")
        end
      | (_DecodeContinueWithCr, _) => h.fail("Decoding is not completed, missing LF")
      | (_DecodeError, let e: String) => h.fail(e)
      end
    else
      h.fail()
    end

  fun _test_decode_partially1(h: TestHelper) =>
    h.expect_action("array-decode-partially-1")
    let msg = "Decoded result is not a array"
    let bytes1: Array[U8] ref = recover ref Array[U8](4) end
    bytes1.append("*1\r\n".array())
    let bytes2: Array[U8] ref = recover ref Array[U8](14) end
    bytes2.append("+hello world\r\n".array())
    match RespDecoder(bytes1)
    | (_DecodeContinue, let ctx1: _DecodeContext) =>
      match RespDecoder.cont(ctx1, bytes2)
      | (_DecodeDone, (let v: RedisValue val, let r: Array[U8] ref)) =>
        try
          let a = v.get_array()?
          h.assert_true(a.size() == 1)
          let a0 = a(0)?.get_string()?
          h.assert_eq[String](a0, "hello world")
          h.complete_action("array-decode-partially-1")
        else
          h.fail(msg)
        end
      | (_DecodeContinue, let ctx2: _DecodeContext) =>
        h.fail("Decoding is not completed")
      | (_DecodeContinueWithCr, _) => h.fail("Decoding is not completed, missing LF")
      | (_DecodeError, let e: String) => h.fail(e)
      end
    else
      h.fail("")
    end
