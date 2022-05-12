local core = require("apisix.core")
local ngx_re = require("ngx.re")
local api_key = require("apisix.api_key")
local _M = {
    version = 0.1,
}

local function check_conf(user_id, conf)
    if not conf then
        return nil, { error_msg = "missing configurations" }
    end

    -- set id
    local user_id = conf.user_id or user_id
    if not user_id then
        return nil, { error_msg = "missing user_id" }
    end

    -- check input with default api_key schema
    local ok, err = core.schema.check(core.schema.api_key, conf)
    if not ok then
        return nil, { error_msg = "invalid configuration: " .. err }
    end

    return user_id
end

-- get api_key
function _M.get(user_id)
    if not user_id then
        return 400, { error_msg = "missing id" }
    end

    local key = "/api_keys"
    if user_id then
        key = key .. "/" .. user_id
    end

    -- get previous map
    local res, err = core.etcd.get(key)
    if not res then
        core.log.error("failed to get api_key[", key, "]: ", err)
        return 500, { error_msg = err }
    end
    if res.status == 404 then
        core.log.error("api_key[", key, "]: ", err)
        return res.status, res.body
    end

    if not res.body.node.value or type(res.body.node.value) ~= "table" then
        core.log.error("previous map illegal")
        return 500, { error_msg = "previous map illegal" }
    end
    local previous_map = res.body.node.value

    previous_map.count = api_key.get_count(user_id)

    return res.status, previous_map
end

-- create account record
function _M.put(user_id, conf)
    -- validate and get input user_id
    local user_id, err = check_conf(user_id, conf)
    if not user_id then
        return 400, err
    end

    -- form user info
    local user_map = {
        user_id = user_id,
        api_key = conf.api_key,
    }

    -- form etcd key
    local key = "/api_keys/" .. user_id

    -- call redis storage
    -- case count = 0, develope account
    if conf.count == 0 then
        local _, err = api_key.set_count(user_id, 0)
        if err ~= nil then
            core.log.error("failed to create develope account:", err)
            return 500, "failed to set develope account" .. err
        end
    else
        local _, err = api_key.count_increase_by(user_id, conf.count)
        if err ~= nil then
            core.log.error("failed to create account:", err)
            return 500, "failed to recharge account" .. err
        end
    end

    -- call etcd storage
    local res, err = core.etcd.set(key, user_map)
    if err ~= nil then
        core.log.error("failed to save in etcd:", err)
        return 500, "failed to recharge account" .. err
    end

    return res.status, res.body
end


function _M.delete(user_id)
    if not user_id or user_id == "" then
        return 400, { error_msg = "missing user_id" }
    end

    -- get etcd storage
    local key = "/api_keys/" .. user_id
    local res, err = core.etcd.get(key)
    if not res then
        core.log.error("failed to get api_key[", key, "]: ", err)
        return 500, { error_msg = err }
    end

    -- case key not found
    if res.status == 404 then
        core.log.error("api_key[", key, "]: ", err)
        return res.status, res.body
    end

    -- call etcd storage
    local res, _ = core.etcd.delete(key)
    return res.status, res.body
end


return _M
