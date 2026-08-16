# Lua scripts

Lua scripts are used in Redis to guarantee atomicity by executing multiple commands as a single, uninterrupted operation on the server. When implementing a distributed lock with a fencing token, this atomicity helps prevent race conditions where a stale client can continue operating after its lock has expired.
Without Lua, a client would have to check a lock status, read a token, and delete the lock using separate API calls. If another process interrupts between those steps, the lock breaks. Lua forces Redis to execute all those steps as one single block of work.

---

## The Problem: Why You Need a Fencing Token

A standard distributed lock works like this:

1.  Client A acquires a lock with a Time-To-Live (TTL) of 10 seconds.
2.  Client A gets hit by a long Stop-the-World garbage collection (GC) pause or a network glitch for 12 seconds.
3.  The lock expires on the Redis server.
4.  Client B acquires the newly expired lock.
5.  Client A wakes up from the pause, unaware that time passed, and tries to write to your database.
6.  Data corruption occurs because Client A and Client B are writing concurrently.

A f*encing token* solves this. It is a monotonically increasing number (like a counter or a transaction ID) attached to the lock. The storage engine (database) will reject any write request if a newer token has already been processed.

## 🛡️ Implementing the Patterns in Redis via Lua

To implement this robustly, you need Lua scripts for two primary operations: safely releasing a lock and validating/generating tokens.

## 1. The Standard "Atomic Release" Script

When Client A finishes its work, it must safely release the lock. It should only delete the lock if it still owns it. If the lock expired and Client B took it, Client A must not accidentally delete Client B's lock.

```lua
-- KEYS[1]: The lock key name (e.g., "lock:order_123")
-- ARGV[1]: The unique client identifier value (e.g., a UUID)

if redis.call("get", KEYS[1]) == ARGV[1] then
return redis.call("del", KEYS[1])
else
return 0 -- Lock was lost or belonged to someone else, do nothing
end
```

## 2. The Fencing Token Acquire Script

To combine locking with fencing, you can store both the owner identifier and a fencing token inside a Redis Hash map, incrementing the token on every successful acquisition.

```lua
-- KEYS[1]: The lock key name
-- KEYS[2]: A global counter key for generating tokens
-- ARGV[1]: The unique client identifier (UUID)
-- ARGV[2]: Lock expiration time in milliseconds (TTL)

-- Check if lock is free
local current_owner = redis.call("hget", KEYS[1], "owner")

if not current_owner or current_owner == ARGV[1] then
    -- Lock is free or already held by this client (re-entrant)
    -- Increment the global counter to get a new fencing token
    local token = redis.call("incr", KEYS[2])

    -- Save the owner and token to the lock hash
    redis.call("hset", KEYS[1], "owner", ARGV[1], "token", token)
    redis.call("pexpire", KEYS[1], ARGV[2])

    return token -- Return token to client to use in database writes
else
    return 0 -- Lock acquisition failed
end
```

---

## How the Full Architecture Operates

The timeline below maps out how a system uses Redis Lua scripting to safely manage a distributed lock, issue fencing tokens, and block late-arriving clients from corrupting data.

![Distributed lock and fencing](/docs/redis-distributed-lock-and-fencing.png)

1.  Acquire & Generate: Client A executes the Lua script. It secures the lock and receives Token: 101.
2.  The Pause: Client A experiences a system freeze. The Redis TTL expires, destroying the lock.
3.  The Overtake: Client B runs the Lua script, claims the lock, and gets Token: 102.
4.  The Safe Write: Client B updates the database with Token: 102. The database records that 102 is the latest processed token.
5.  The Fencing Block: Client A wakes up and attempts a write using Token: 101. The database evaluates 101 < 102 and rejects the transaction, successfully preventing data corruption.

---

## ⚠️ Critical Implementation Rules

- Do Not Write Infinite Loops: Lua scripts block the entire Redis server while running. Keep your logic simple (O(1) complexity) and avoid long loops.
- Handle Clustering Appropriately: If using Redis Cluster, all keys passed into your Lua script (KEYS[1], KEYS[2]) must map to the same hash slot. You can enforce this using Redis hash tags, naming your keys like {lock:123}:key and {lock:123}:counter.
