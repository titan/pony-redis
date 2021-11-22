use "ponytest"

class iso _TestStrings is UnitTest
  var _redis: (Redis | None) = None

  fun name(): String => "Strings"

  fun label(): String => "string"

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
      _test_set(h, redis')
      _test_get(h, redis')
      h.long_test(1_000_000_000)
    | None => h.fail("Redis actor is not initialized")
    end

  fun _test_set(h: TestHelper, redis: Redis) =>
    h.expect_action("string-set")
    redis.set("hello", "world").next[None]({(v: Bool) =>
      h.assert_true(v)
      h.complete_action("string-set")
      })

  fun _test_get(h: TestHelper, redis: Redis) =>
    h.expect_action("string-get")
    redis.get("hello").next[None]({(v: (String | None)) =>
      match v
      | let v': String =>
        h.assert_eq[String](v', "world")
        h.complete_action("string-get")
      else
        h.fail()
      end
    })
