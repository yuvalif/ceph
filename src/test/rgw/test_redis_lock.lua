#!lua name=rgwlib

---
--- To be removed later ---
---
--- Linux Error codes
local lerrorCodes = {
    EPERM = 1,
    ENOENT = 2,
    EBUSY = 16,
    EEXIST = 17
}

--- Assert if the lock is held by the owner of the cookie
--- @param keys table A single element list - lock name
--- @param args table A single-element list - cookie 
--- @return number 0 if the lock is held by the owner of the cookie,
--- -lerrorCodes.EBUSY if the lock is held by another owner, 
--- -lerrorCodes.ENOENT if the lock does not exist
local function assert_lock(keys, args)
    local name = keys[1]
    local cookie = args[1]
    if redis.call('EXISTS', name) == 1 then
        local existing_cookie = redis.call('GET', name)
        if existing_cookie == cookie then
            return 0 -- success
        else
            return -lerrorCodes.EBUSY
        end
    end
    return -lerrorCodes.ENOENT
end

--- Acquire a lock on a resource.
--- It sets a key with a cookie value if the key does not exist.
--- If the key exists and the value is same as cookie, it extends the lock.
--- If the key exists and the value is different from cookie, it fails.  
---@param keys table A single element list - lock name
---@param args table A two-element list - cookie and timeout
---@return number 0 if the lock is acquired or extended
local function lock(keys, args)
    local name = keys[1]
    local cookie = args[1]
    local timeout = args[2]
    local lock_status = assert_lock(keys, args)
    if lock_status == 0 then
        redis.call('PEXPIRE', name, timeout)
        return 0
    elseif lock_status == -lerrorCodes.ENOENT then
        redis.call('SET', name, cookie, 'PX', timeout)
        return 0
    end
    return lock_status
end

--- Release the lock on a resource.
--- It deletes the key if the value matches the cookie.
---@param keys table A single element list - lock name
---@param args table A single-element list - cookie
local function unlock(keys, args)
    local name = keys[1]
    local cookie = args[1]
    local lock_status = assert_lock(keys, args)
    if lock_status == 0 then
        redis.call('DEL', name)
        return 0
    end
    return lock_status
end

--- Register the functions.
redis.register_function('lock', lock)
redis.register_function('unlock', unlock)
redis.register_function('assert_lock', assert_lock)
