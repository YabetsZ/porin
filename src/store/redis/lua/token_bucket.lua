-- Token bucket as GCRA, mirroring src/algorithm/gcra.rs. The state is the
-- theoretical arrival time (TAT). The store rejects rates whose full refill
-- is 2^51 us or more, so every value here stays exact.

local function decode(raw)
  return { tat = tonumber(raw) or 0 }
end

local function encode(state)
  return string.format("%.0f", state.tat)
end

local function bucket(spec)
  local interval = ceil_div(spec.period, spec.count)
  if interval < 1 then interval = 1 end
  return { interval = interval, burst = spec.burst, tolerance = interval * spec.burst }
end

local function stats(b, now, tat)
  local spare = now + b.tolerance - tat
  if spare < 0 then spare = 0 end
  local remaining = idiv(spare, b.interval)
  if remaining > b.burst then remaining = b.burst end
  local reset = tat - now
  if reset < 0 then reset = 0 end
  return remaining, reset
end

local function check(spec, state, now, cost)
  local b = bucket(spec)
  -- If the clock went backwards, the stored TAT is kept, which under-admits.
  local tat = math.max(state.tat, now)
  if cost > b.burst then
    local remaining, reset = stats(b, now, tat)
    return { allowed = false, remaining = remaining, reset = reset, retry = -1 }
  end
  local new_tat = tat + b.interval * cost
  local allow_at = new_tat - b.tolerance
  if allow_at < 0 then allow_at = 0 end
  if now >= allow_at then
    local remaining, reset = stats(b, now, new_tat)
    return { allowed = true, remaining = remaining, reset = reset, commit = { tat = new_tat } }
  end
  local remaining, reset = stats(b, now, tat)
  return { allowed = false, remaining = remaining, reset = reset, retry = allow_at - now }
end

local function idle_at(_, state)
  return state.tat
end

local storage = string_storage(decode, encode, idle_at)
