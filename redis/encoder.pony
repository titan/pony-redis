primitive RespEncoder
  fun apply(input: RedisValue val): String val ? =>
    match input.kind
    | RedisBlobString =>
      let str: String = input.get_string()?
      let lenstr: String = str.size().string()
      let output = recover String(1 + lenstr.size() + 2 + str.size() + 2) end
      output.append("$")
      output.append(lenstr)
      output.append("\r\n")
      output.append(str)
      output.append("\r\n")
      output
    | RedisSimpleString =>
      let str: String = input.get_string()?
      let output = recover String(1 + str.size() + 2) end
      output.append("+")
      output.append(str)
      output.append("\r\n")
      output
    | RedisSimpleError =>
      let str: String = input.get_string()?
      let output = recover String(1 + str.size() + 2) end
      output.append("-")
      output.append(str)
      output.append("\r\n")
      output
    | RedisNumber =>
      let numstr: String = input.get_number()?.string()
      let output = recover String(1 + numstr.size() + 2) end
      output.append(":")
      output.append(numstr)
      output.append("\r\n")
      output
    | RedisArray =>
      let a = input.get_array()?
      let lenstr: String = a.size().string()
      let output = recover String(1 + lenstr.size() + 2 + (a.size() * 4)) end
      output.append("*")
      output.append(lenstr)
      output.append("\r\n")
      for item in a.values() do
        output.append(apply(item)?)
      end
      output
    | RedisNull => "*-1\r\n"
    end

