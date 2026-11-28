-- Checks every rate of a quota for one key and records the hit only if all
-- of them allow it, atomically, as Redis runs a script alone.
--
-- KEYS[i]      the state of rate i
-- ARGV[1]      "hit" records an allowed hit, "test" only checks
-- ARGV[2]      cost
-- ARGV[3]      "" to read Redis TIME, or a time in microseconds (tests only)
-- ARGV[4 + 3(i - 1) ..]  period (us), count, burst of rate i
--
-- Returns {allowed (1 or 0), remaining, reset_after (us), retry_after
-- (us, -1 for never, 0 when allowed)}. The algorithm part defines check and
-- storage: storage.read(key) loads the state check reads, and
-- storage.write(key, spec, commit, now) records an allowed hit's commit and
-- sets the key to expire once the state is idle.

local record = ARGV[1] == "hit"
local cost = tonumber(ARGV[2])
local now
if ARGV[3] ~= "" then
  now = tonumber(ARGV[3])
else
  local time = redis.call("TIME")
  now = tonumber(time[1]) * 1000000 + tonumber(time[2])
end

local specs, states, results = {}, {}, {}
local all_allowed = true
for i = 1, #KEYS do
  local base = 4 + 3 * (i - 1)
  specs[i] = {
    period = tonumber(ARGV[base]),
    count = tonumber(ARGV[base + 1]),
    burst = tonumber(ARGV[base + 2]),
  }
  states[i] = storage.read(KEYS[i])
  results[i] = check(specs[i], states[i], now, cost)
  if not results[i].allowed then all_allowed = false end
end

-- The tightest stats: fewest units remaining, longest reset.
local function tighten(summary, result)
  if result.remaining < summary.remaining then summary.remaining = result.remaining end
  if result.reset > summary.reset then summary.reset = result.reset end
end

local summary = { remaining = 4294967295, reset = 0 }
if all_allowed then
  for i = 1, #KEYS do
    tighten(summary, results[i])
    -- A peek records nothing.
    if record and cost > 0 then
      storage.write(KEYS[i], specs[i], results[i].commit, now)
    end
  end
  return { 1, summary.remaining, summary.reset, 0 }
end

-- Allowed rates reported stats as if the hit were recorded. It isn't, so
-- report what every rate has without it, and wait for the slowest denial.
local retry = 0
for i = 1, #KEYS do
  tighten(summary, check(specs[i], states[i], now, 0))
  local result = results[i]
  if not result.allowed then
    if result.retry == -1 or retry == -1 then
      retry = -1
    elseif result.retry > retry then
      retry = result.retry
    end
  end
end
return { 0, summary.remaining, summary.reset, retry }
