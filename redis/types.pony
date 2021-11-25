use "collections"

primitive RedisNull
  fun box string(): String val => "null"

primitive RedisNumber
  fun box string(): String val => "number"

primitive RedisBlobString
  fun box string(): String val => "blob-string"

primitive RedisSimpleString
  fun box string(): String val => "simple-string"

primitive RedisArray
  fun box string(): String val => "array"

primitive RedisSimpleError
  fun box string(): String val => "simple-error"

type RedisKind is (RedisNull | RedisNumber | RedisBlobString | RedisSimpleString | RedisArray | RedisSimpleError)

class RedisValue is (Hashable & Equatable[RedisValue])
  let _value: (None | String | I64 | Array[RedisValue val] val)
  let kind: RedisKind

  new iso null() =>
    _value = None
    kind = RedisNull

  new iso number(v: I64) =>
    _value = v
    kind = RedisNumber

  new iso blob_string(v: String) =>
    _value = v
    kind = RedisBlobString

  new iso simple_string(v: String) =>
    _value = v
    kind = RedisSimpleString

  new iso array(v: Array[RedisValue val] val) =>
    _value = v
    kind = RedisArray

  new iso simple_error(v: String) =>
    _value = v
    kind = RedisSimpleError

  fun box hash() : USize val =>
    match _value
    | let s: String => s.hash()
    | let i: I64 => i.usize()
    | let a: this->Array[RedisValue val] =>
      var h: USize = USize(0)
      for i in a.values() do
        h = h xor i.hash()
      end
      return h
    else
      return 0
    end

  fun box eq(that: box->RedisValue): Bool =>
    if (this.kind is that.kind) then
      match _value
      | None => that.is_null()
      | let s: String => try s.eq(that.get_string()?) else false end
      | let i: I64 => try i == that.get_number()? else false end
      | let a: this->Array[RedisValue val] =>
        try
          let t = that.get_array()?
          if (a.size() == t.size()) then
            var ah: USize = USize(0)
            for (idx, item) in a.pairs() do
              ah = ah xor idx
              ah = ah xor item.hash()
            end
            var th: USize = USize(0)
            for (idx, item) in t.pairs() do
              th = th xor idx
              th = th xor item.hash()
            end
            ah == th
          else
            false
          end
        else
          false
        end
      else
        false
      end
    else
      false
    end

  fun box ne(that: box->RedisValue): Bool => not eq(that)

  fun box is_null(): Bool =>
    _value is None

  fun box get_string(): String ? =>
    match _value
    | let s: String => s
    else
      error
    end

  fun box get_number(): I64 ? =>
    match _value
    | let i: I64 => i
    else
      error
    end

  fun box get_array(): this->Array[RedisValue val] val ? =>
    match _value
    | let a: this->Array[RedisValue val] val => a
    else
      error
    end
