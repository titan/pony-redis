use "ponytest"

actor Main is TestList
  new create(env: Env) =>
    PonyTest(env, this)

  new make() =>
    None

  fun tag tests(test: PonyTest) =>
    test(_TestBlobString)
    test(_TestSimpleString)
    test(_TestSimpleError)
    test(_TestNumber)
    test(_TestArray)
    test(_TestStrings)
    test(_TestStreams)
