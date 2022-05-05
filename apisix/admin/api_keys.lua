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

-- add api_id in account
function _M.post(id, conf)
    -- validate and get input api_id
    local user_id, err = check_conf(id, conf)
    if not user_id then
        return 400, err
    end

    -- form etcd key
    local key = "/api_keys/" .. user_id

    -- get etcd storage
    local previous_map
    local res, err = core.etcd.get(key)
    if not res then
        core.log.error("get etcd result failed")
        return 500, { error_msg = err }
    end

    -- parse etcd return
    -- case empty key
    if res.status == 404 then
        core.log.error("empty previous map")
        return res.status, res.body
    end

    -- case success, get previous map
    if res.status == 200 then
        -- previous map illegal
        if not res.body.node.value or type(res.body.node.value) ~= "table" then
            core.log.error("previous map illegal")
            return 500, { error_msg = "previous map illegal" }
        end
        previous_map = res.body.node.value
    end

    if not previous_map.api_ids then
        previous_map.api_ids = { conf.api_id }
    else
        -- case api_id already exists
        for _, api_id in ipairs(previous_map.api_ids) do
            if api_id == conf.api_id then
                local err_msg = "api_id[" .. conf.api_id .. "] already bind with user_id[" .. user_id .. "]"
                core.log.error(err_msg)
                return 500, { error_msg = err_msg }
            end
        end

        -- set value
        table.insert(previous_map.api_ids, conf.api_id)
    end

    -- call etcd storage
    local res, _ = core.etcd.set(key, previous_map)

    return res.status, res.body
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
        api_ids = {},
    }

    -- form etcd key
    local key = "/api_keys/" .. user_id
    local res, _ = core.etcd.get(key)
    if res.status ~= 404 and res.body.node.value then
        user_map.api_ids = res.body.node.value.api_ids
    end

    -- call redis storage
    -- case count = 0, develope account
    if conf.count == 0 then
        local _, err = api_key.set_count(user_id, -65535)
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


function _M.delete(id)
    -- check id format, {user_id}.{api_id} is required
    if not id then
        return 400, { error_msg = "missing id" }
    end
    local id_segs = ngx_re.split(id, "\\.")
    local api_id, user_id = id_segs[2], id_segs[1]
    if not api_id or not user_id then
        local err_msg = "invalid id parameter pattern, '{user_id}.{api_id}' is required"
        core.log.error(err_msg)
        return 500, { error_msg = err_msg }
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

    -- case previous map illegal
    if not res.body.node.value or type(res.body.node.value) ~= "table" then
        core.log.error("previous map illegal")
        return 500, { error_msg = "previous map illegal" }
    end
    local previous_map = res.body.node.value

    -- delete target api_id
    local flag = false
    for index, cur_api_id in ipairs(previous_map.api_ids) do
        if api_id == cur_api_id then
            table.remove(previous_map.api_ids, index)
            flag = true
            break
        end
    end

    -- case api_id is not bind with user
    if not flag then
        local err_msg = "api_id[" .. api_id .. "] does not exist in api_key[" .. user_id .. "]"
        core.log.error(err_msg)
        return 500, { error_msg = err_msg }
    end

    -- call etcd storage
    local res, _ = core.etcd.set(key, previous_map)
    return res.status, res.body
end

return _M
