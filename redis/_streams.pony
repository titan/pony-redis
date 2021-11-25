use "ponytest"
use "collections"

class iso _TestStreams is UnitTest
  var _redis: (Redis | None) = None

  fun name(): String => "Streams"

  fun label(): String => "stream"

  fun ref set_up(h: TestHelper)? =>
    _redis = Redis(h.env.root as AmbientAuth)
    match _redis
    | let redis': Redis => redis'.connect("localhost", "6379")
    | None => h.fail("Cannot initialize Redis actor")
    end

  fun ref tear_down(h: TestHelper) =>
    match _redis
    | let redis': Redis => redis'.dispose()
    | None => h.fail("Redis actor is not initialized")
    end

  fun timed_out(h: TestHelper) =>
    h.complete(false)

  fun apply(h: TestHelper) =>
    match _redis
    | let redis': Redis =>
      //_test_xadd(h, redis')
      //_test_xdel(h, redis')
      //_test_xlen(h, redis')
      //_test_xrange(h, redis')
      _test_xread(h, redis')
      h.long_test(3_000_000_000)
    | None => h.fail("Redis actor is not initialized")
    end

  fun _test_xadd(h: TestHelper, redis: Redis) =>
    h.expect_action("stream-xadd")
    redis.xadd("stream", [("field1", "value1"); ("field2", "value2")]).next[None]({(v: (String | None)) =>
      match v
      | let id: String =>
        h.complete_action("stream-xadd")
      else
        h.fail("invalid response of xadd")
      end
    })

  fun _test_xdel(h: TestHelper, redis: Redis) =>
    h.expect_action("stream-xdel")
    redis.xadd("stream", [("field5", "value5"); ("field6", "value6")]).next[None]({(v: (String | None))(h) =>
      match v
      | let id: String =>
        redis.xdel("stream", [id]).next[None]({(v': I64) =>
          h.assert_eq[I64](v', 1)
          h.complete_action("stream-xdel")
        })
      else
        h.fail("xadd failed before xdel")
      end
    })

  fun _test_xlen(h: TestHelper, redis: Redis) =>
    h.expect_action("stream-xlen")
    redis.xlen("stream").next[None]({(v: I64)(h) =>
      h.complete_action("stream-xlen")
    })

  fun _test_xrange(h: TestHelper, redis: Redis) =>
    h.expect_action("stream-xrange")
    redis.xadd("stream", [("field9", "value9"); ("field10", "value10"); ("field3", "value3"); ("field4", "value4")]).next[None]({(v: (String | None))(h) =>
      match v
      | let id: String =>
        redis.xrange("stream", id, id).next[None]({(a: Array[(String, Map[String, String] val)] val) =>
          h.assert_eq[USize](a.size(), 1, "Length of result is not 1")
          try h.assert_eq[String](a(0)?._1, id, "Id of item is incorrect") else h.fail("Length of items is not 1") end
          try h.assert_eq[String](a(0)?._2("field9")?, "value9") else h.fail("Field9 not found") end
          try h.assert_eq[String](a(0)?._2("field10")?, "value10") else h.fail("Field10 not found") end
          try h.assert_eq[String](a(0)?._2("field3")?, "value3") else h.fail("Field3 not found") end
          try h.assert_eq[String](a(0)?._2("field4")?, "value4") else h.fail("Field4 not found") end
          h.complete_action("stream-xrange")
        })
      else
        h.fail("xadd failed before xrange")
      end
    })

  fun _test_xread(h: TestHelper, redis: Redis) =>
    h.expect_action("stream-xread")
    redis.xadd("stream", [("field11", "value11"); ("field12", "value12")]).next[None]({(v: (String | None))(h) =>
      match v
      | let id: String =>
        redis.xadd("stream", [("field13", "value13"); ("field14", "value14")]).next[None]({(v': (String | None))(h) =>
          match v'
          | let id': String =>
            redis.xread([("stream", id)]).next[None]({(r: (None | Map[String, Array[(String, Map[String, String] val)] val] val)) =>
              match r
              | let m: Map[String, Array[(String, Map[String, String] val)] val] val =>
                h.assert_eq[USize](m.size(), 1, "Size of streams is not 1")
                try h.assert_eq[USize](m("stream")?.size(), 1, "Length of items is not 1") else h.fail("Name of stream is not found") end
                try h.assert_eq[String](m("stream")?(0)?._1, id', "Id of item is incorrect") else h.fail("Lenght of items is not 1") end
                try h.assert_eq[String](m("stream")?(0)?._2("field13")?, "value13") else h.fail("Field13 is not found") end
                try h.assert_eq[String](m("stream")?(0)?._2("field14")?, "value14") else h.fail("Field14 is not found") end
                h.complete_action("stream-xread")
              else
                h.fail()
              end
            })
          else
            h.fail("xadd2 failed before xread")
          end
        })
      else
        h.fail("xadd failed before xread")
      end
    })
