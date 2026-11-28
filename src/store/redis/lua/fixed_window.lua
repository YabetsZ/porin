-- Fixed window, mirroring src/algorithm/fixed_window.rs.

local function decode(raw)
  if not raw then return { start = 0, used = 0 } end
  local start, used = string.match(raw, "^(%d+):(%d+)$")
  if not start then error("porin: unreadable fixed window state") end
  return { start = tonumber(start), used = tonumber(used) }
end

local function encode(state)
  return string.format("%.0f:%.0f", state.start, state.used)
end

local function check(spec, state, now, cost)
  local period, limit = spec.period, spec.count
  -- A later window starts empty. If the clock went backwards into an
  -- earlier window, the stored window and its count are kept.
  local current = window_start(now, period)
  local start, used = state.start, state.used
  if current > start then start, used = current, 0 end
  local window_end = start + period - now
  if window_end < 0 then window_end = 0 end
  local function report(spent)
    local remaining = limit - spent
    if remaining < 0 then remaining = 0 end
    local reset = window_end
    if spent == 0 then reset = 0 end
    return remaining, reset
  end
  if cost > limit then
    local remaining, reset = report(used)
    return { allowed = false, remaining = remaining, reset = reset, retry = -1 }
  end
  local total = used + cost
  if total <= limit then
    local remaining, reset = report(total)
    return {
      allowed = true, remaining = remaining, reset = reset,
      commit = { start = start, used = total },
    }
  end
  local remaining, reset = report(used)
  return { allowed = false, remaining = remaining, reset = reset, retry = window_end }
end

local function idle_at(spec, state)
  if state.used == 0 then return 0 end
  return state.start + spec.period
end

local storage = string_storage(decode, encode, idle_at)
