-- Exact integer arithmetic on Lua numbers, which are doubles: integers are
-- exact only up to 2^53. Times are microseconds (about 2^51 today), periods
-- are below 2^50 and counts below 2^32. Every value stays below 2^53 except
-- count x period products, which go through mul_div.

-- floor(a / b) for 0 <= a < 2^53 and 0 < b < 2^53. The float division can
-- be off by one near an integer, so the estimate is corrected.
local function idiv(a, b)
  local q = math.floor(a / b)
  while q * b > a do q = q - 1 end
  while (q + 1) * b <= a do q = q + 1 end
  return q
end

local function ceil_div(a, b)
  local q = idiv(a, b)
  if q * b < a then return q + 1 end
  return q
end

-- Results larger than this saturate; callers only compare them with values
-- below 2^50, so saturating changes no answer.
local CAP = 4503599627370496 -- 2^52

-- floor(a * b / d) and whether the division left a remainder, for
-- 0 <= a < 2^32, 0 <= b < 2^50 and 0 < d < 2^50, without forming the
-- product: binary long multiplication, keeping quotient and remainder.
local function mul_div(a, b, d)
  local q, r = 0, 0
  for i = 31, 0, -1 do
    q = q * 2
    r = r * 2
    if math.floor(a / 2 ^ i) % 2 == 1 then r = r + b end
    if r >= d then
      local k = idiv(r, d)
      q = q + k
      r = r - k * d
    end
    if q >= CAP then return CAP, true end
  end
  return q, r > 0
end

local function mul_div_floor(a, b, d)
  if d == 0 then return CAP end
  local q = mul_div(a, b, d)
  return q
end

local function mul_div_ceil(a, b, d)
  if d == 0 then return CAP end
  local q, rest = mul_div(a, b, d)
  if rest and q < CAP then return q + 1 end
  return q
end

-- The start of the period-aligned window that contains now. math.fmod is
-- exact on integers, unlike `%`, which divides in floating point.
local function window_start(now, period)
  return now - math.fmod(now, period)
end

-- Storage for a state kept as one string: read with GET, written with SET
-- and an expiry at its idle time, or deleted once it is idle already.
local function string_storage(decode, encode, idle_at)
  local storage = {}
  function storage.read(key)
    return decode(redis.call("GET", key))
  end
  function storage.write(key, spec, state, now)
    local idle = idle_at(spec, state)
    if idle > now then
      redis.call("SET", key, encode(state), "PXAT", ceil_div(idle, 1000))
    else
      redis.call("DEL", key)
    end
  end
  return storage
end
