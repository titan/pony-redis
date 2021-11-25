use "collections"
use "promises"

trait val RedisValueExtractor[A: Any val]
  new val create()
  fun val apply(r: Redis tag, v: RedisValue val): A

primitive _NumberExtractor is RedisValueExtractor[I64]
  fun apply(r: Redis tag, v: RedisValue val): I64 =>
    match v.kind
    | RedisNumber => try v.get_number()? else -1 end
    else
      -1
    end

primitive _BlobStringOrNoneExtractor is RedisValueExtractor[(String | None)]
  fun apply(r: Redis tag, v: RedisValue val): (String | None) =>
    match v.kind
    | RedisBlobString => try v.get_string()? else None end
    else
      None
    end

primitive _OkayOrNoneExtractor is RedisValueExtractor[Bool]
  fun apply(r: Redis tag, v: RedisValue val): Bool =>
    match v.kind
    | RedisSimpleString => try v.get_string()? == "OK" else false end
    else
      false
    end

primitive _SingleStreamItemArrayExtractor is RedisValueExtractor[Array[(String, Map[String, String] val)] val]
  fun apply(r: Redis tag, v: RedisValue val): Array[(String, Map[String, String] val)] val =>
    match v.kind
    | RedisArray =>
      try
        let result = recover Array[(String, Map[String, String] val)] end
        for item in v.get_array()?.values() do
          match item.kind
          | RedisArray =>
            let id = try item.get_array()?(0)?.get_string()? else continue end
            let values = try item.get_array()?(1)?.get_array()? else continue end
            var field: (String | None) = None
            let pairs: Map[String, String] iso = recover Map[String, String] end
            for subitem in values.values() do
              match field
              | None => field = try subitem.get_string()? else "" end
              | let field': String =>
                pairs.insert(field', try subitem.get_string()? else "" end)
                field = None
              end
            end
            result.push((id, consume pairs))
          else
            continue
          end
        end
        consume result
      else
        recover val Array[(String, Map[String, String] val)] end
      end
    else
      recover val Array[(String, Map[String, String] val)] end
    end

primitive _MultiplayStreamItemArrayExtractor is RedisValueExtractor[(None | Map[String, Array[(String, Map[String, String] val)] val] val)]
  fun apply(r: Redis tag, v: RedisValue val): (None | Map[String, Array[(String, Map[String, String] val)] val] val) =>
    match v.kind
    | RedisArray =>
      try
        let result = recover Map[String, Array[(String, Map[String, String] val)] val] end
        for keyitem in v.get_array()?.values() do
          match keyitem.kind
          | RedisArray =>
            let key = try keyitem.get_array()?(0)?.get_string()? else continue end
            let streamitems = try keyitem.get_array()?(1)?.get_array()? else continue end
            let stream = recover Array[(String, Map[String, String] val)] end
            for item in streamitems.values() do
              match item.kind
              | RedisArray =>
                let id = try item.get_array()?(0)?.get_string()? else continue end
                let values = try item.get_array()?(1)?.get_array()? else continue end
                var field: (String | None) = None
                let pairs: Map[String, String] iso = recover Map[String, String] end
                for subitem in values.values() do
                  match field
                  | None => field = try subitem.get_string()? else "" end
                  | let field': String =>
                    pairs.insert(field', try subitem.get_string()? else "" end)
                    field = None
                  end
                end
                stream.push((id, consume pairs))
              else
                continue
              end
            end
            result.insert(key, consume stream)
          end
        end
        consume result
      else
        None
      end
    else
      None
    end
