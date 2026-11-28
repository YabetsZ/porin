-- Sliding window counter, mirroring src/algorithm/sliding_window.rs.

local U32_MAX = 4294967295

local function decode(raw)
  if not raw then return { start = 0, previous = 0, current = 0 } end
  local start, previous, current = string.match(raw, "^(%d+):(%d+):(%d+)$")
  if not start then error("porin: unreadable sliding window state") end
  return { start = tonumber(start), previous = tonumber(previous), current = tonumber(current) }
end

local function encode(state)
  return string.format("%.0f:%.0f:%.0f", state.start, state.previous, state.current)
end

-- The state as seen from the window starting at current_start.
local function roll(state, current_start, period)
  if current_start <= state.start then
    return state
  elseif state.start + period == current_start then
    return { start = current_start, previous = state.current, current = 0 }
  end
  return { start = current_start, previous = 0, current = 0 }
end

local function weighted(count, left, period)
  local w = mul_div_ceil(count, left, period)
  if w > U32_MAX then w = U32_MAX end
  return w
end

local function retry_after(window, left, period, limit, cost)
  local budget = limit - window.current - cost
  if budget >= 0 and window.previous > 0 then
    local overlap_allowed = mul_div_floor(budget, period, window.previous)
    local wait = left - overlap_allowed
    if wait < 0 then wait = 0 end
    if wait < left then return wait end
  end
  if window.current == 0 then return left end
  local rest = limit - cost
  if rest < 0 then rest = 0 end
  local overlap_allowed = mul_div_floor(rest, period, window.current)
  local tail = period - overlap_allowed
  if tail < 0 then tail = 0 end
  return left + tail
end

local function check(spec, state, now, cost)
  local period, limit = spec.period, spec.count
  local window = roll(state, window_start(now, period), period)
  local left = window.start + period - now
  if left < 0 then left = 0 end
  if left > period then left = period end
  local estimate = weighted(window.previous, left, period) + window.current
  if estimate > U32_MAX then estimate = U32_MAX end
  local reset = 0
  if window.current > 0 then
    reset = left + period
  elseif window.previous > 0 then
    reset = left
  end
  local function remaining_after(used)
    local remaining = limit - used
    if remaining < 0 then remaining = 0 end
    return remaining
  end
  if cost > limit then
    return { allowed = false, remaining = remaining_after(estimate), reset = reset, retry = -1 }
  end
  local total = estimate + cost
  if total <= limit then
    local after_reset = reset
    if cost > 0 then after_reset = left + period end
    return {
      allowed = true, remaining = remaining_after(total), reset = after_reset,
      commit = { start = window.start, previous = window.previous, current = window.current + cost },
    }
  end
  return {
    allowed = false, remaining = remaining_after(estimate), reset = reset,
    retry = retry_after(window, left, period, limit, cost),
  }
end

local function idle_at(spec, state)
  if state.current > 0 then return state.start + 2 * spec.period end
  if state.previous > 0 then return state.start + spec.period end
  return 0
end

local storage = string_storage(decode, encode, idle_at)
