-- Sliding log, mirroring src/algorithm/sliding_log.rs. The log of allowed
-- hits is a sorted set, so a call runs O(log n) commands and removes at
-- most PRUNE_LIMIT entries, however many entries the key holds.
--
-- Each hit is a member scored by its time (us). The member is
-- "<end>:<cost>:<base>". <end> is the running total of costs up to and
-- including this hit, padded to 16 digits. Ends grow strictly and times
-- never decrease, so the set's order (score, then member bytes) is the
-- order of the hits, and the cost of a run of entries is the difference of
-- two ends: nothing walks the log to add costs up.
--
-- <base> is the end of the last entry the Rust log would have pruned when
-- this hit was recorded; only the newest entry's base is read. A hit
-- removes at most PRUNE_LIMIT expired entries, so a set can hold pruned
-- entries for a while; they never count, even if the clock goes back far
-- enough to bring their times into the window again.

-- Removing an entry takes about 0.2 us, so this bounds a hit's pruning to
-- about 0.2 ms. The rest goes on later hits, which remove more than they add.
local PRUNE_LIMIT = 1000

local function score(time)
  return string.format("%.0f", time)
end

local function member(finish, cost, base)
  return string.format("%016.0f:%.0f:%.0f", finish, cost, base)
end

-- The entry whose member is reply[at] and score reply[at + 1].
local function entry(reply, at)
  local finish, cost, base = string.match(reply[at], "^(%d+):(%d+):(%d+)$")
  return {
    time = tonumber(reply[at + 1]), finish = tonumber(finish), cost = tonumber(cost),
    base = tonumber(base),
  }
end

local function entry_at(key, rank)
  local reply = redis.call("ZRANGE", key, rank, rank, "WITHSCORES")
  if #reply == 0 then return nil end
  return entry(reply, 1)
end

-- Rewrites every end as an offset from where the oldest entry in the set
-- starts, so ends stay positive even for pruned entries still in the set,
-- and returns the new base and the newest entry's new end. Pruned entries
-- left in the set are few and the live costs sum to at most the count,
-- below 2^32, so this runs about once per 2^52 units admitted to a key
-- that never goes idle: at least 2^20 full periods.
local function renumber(key, base)
  local reply = redis.call("ZRANGE", key, 0, -1, "WITHSCORES")
  local first = entry(reply, 1)
  local shift = first.finish - first.cost
  redis.call("UNLINK", key)
  local finish, args = 0, {}
  for at = 1, #reply, 2 do
    local old = entry(reply, at)
    finish = old.finish - shift
    args[#args + 1] = score(old.time)
    -- Only the newest entry's base is read, and the hit being recorded
    -- becomes the newest.
    args[#args + 1] = member(finish, old.cost, 0)
    if #args >= 1000 then
      redis.call("ZADD", key, unpack(args))
      args = {}
    end
  end
  if #args > 0 then redis.call("ZADD", key, unpack(args)) end
  return base - shift, finish
end

local storage = {}

function storage.read(key)
  return { key = key, oldest = entry_at(key, 0), newest = entry_at(key, -1) }
end

local function check(spec, state, now, cost)
  local period, limit = spec.period, spec.count
  local key, oldest, newest = state.key, state.oldest, state.newest
  -- A hit at time t counts while now < t + period, so the entries scored
  -- at or below cutoff have expired. They are a prefix of the log.
  local cutoff = now - period
  -- Every cost up to start has expired or been pruned: start is the later
  -- of the newest entry's base and the end of the newest expired entry.
  local start, expired = 0, false
  if newest then
    if newest.time <= cutoff then
      start, expired = newest.finish, true
    elseif oldest.time <= cutoff then
      local reply = redis.call("ZREVRANGEBYSCORE", key, score(cutoff), "-inf",
        "WITHSCORES", "LIMIT", 0, 1)
      start, expired = entry(reply, 1).finish, true
    else
      start = oldest.finish - oldest.cost
    end
    if newest.base > start then start = newest.base end
  end
  local used, reset = 0, 0
  if newest then used = newest.finish - start end
  local function until_expiry(time)
    local left = time + period - now
    if left < 0 then left = 0 end
    return left
  end
  if newest and newest.time > cutoff then reset = until_expiry(newest.time) end
  local function remaining_after(spent)
    local remaining = limit - spent
    if remaining < 0 then remaining = 0 end
    return remaining
  end
  if cost > limit then
    return { allowed = false, remaining = remaining_after(used), reset = reset, retry = -1 }
  end
  local total = used + cost
  if total <= limit then
    local after_reset = reset
    if cost > 0 then after_reset = period end
    return {
      allowed = true, remaining = remaining_after(total), reset = after_reset,
      commit = { expired = expired, cutoff = cutoff, start = start, newest = newest, cost = cost },
    }
  end
  -- Wait until enough of the oldest live entries expire: until the first
  -- entry whose end is at least start + must_free, found by a binary
  -- search over ranks. Ends at or below start belong to entries that no
  -- longer count, so the search never stops on one. The newest entry's end
  -- is start + used, and must_free <= used, so the newest always qualifies.
  local target = start + total - limit
  local low, high = 0, redis.call("ZCARD", key) - 1
  if expired then low = redis.call("ZCOUNT", key, "-inf", score(cutoff)) end
  local found = newest
  while low < high do
    local middle = math.floor((low + high) / 2)
    local candidate = entry_at(key, middle)
    if candidate.finish >= target then
      high, found = middle, candidate
    else
      low = middle + 1
    end
  end
  local retry = until_expiry(found.time)
  return { allowed = false, remaining = remaining_after(used), reset = reset, retry = retry }
end

-- Prunes expired entries and appends the hit. The driver records only hits
-- that cost something.
function storage.write(key, spec, commit, now)
  local newest = commit.newest
  -- Keep times non-decreasing if the clock went backwards. The entry then
  -- expires later than it would, which under-admits. An expired newest
  -- entry is older than now, so it never moves the time.
  local time = now
  if newest and newest.time > time then time = newest.time end
  local finish, base = commit.cost, 0
  if newest and newest.time <= commit.cutoff then
    -- Every entry has expired. UNLINK drops the key at once and frees a
    -- large set off the main thread; the ZADD below starts a new one, which
    -- numbers its ends from zero again.
    redis.call("UNLINK", key)
  elseif newest then
    if commit.expired then
      local count = redis.call("ZCOUNT", key, "-inf", score(commit.cutoff))
      redis.call("ZREMRANGEBYRANK", key, 0, math.min(count, PRUNE_LIMIT) - 1)
    end
    finish, base = newest.finish + commit.cost, commit.start
    if finish > CAP then
      local last
      base, last = renumber(key, base)
      finish = last + commit.cost
    end
  end
  redis.call("ZADD", key, score(time), member(finish, commit.cost, base))
  -- The newest entry is the last to leave the window, at time + period,
  -- which is after now.
  redis.call("PEXPIREAT", key, ceil_div(time + spec.period, 1000))
end
