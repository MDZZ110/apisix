local core = require("apisix.core")
local plugin = require("apisix.plugin")
local uuid = require("resty.jit-uuid")
local sleep = ngx.sleep
local tonumber = tonumber
local error = error
local ipairs = ipairs
local pairs = pairs
local type = type
local api_keys

local _M = {
    version = 0.3,
}

local function plugin_api_key()
    local plugins = {}
    if api_keys.values == nil then
        return plugins
    end
    -- form table
    local config_name = "qingcloud-auth"
    plugins[config_name] = {
        nodes = {},
        conf_version = api_keys.conf_version
    }

    -- traverse api_keys.values
    for _, api_key in ipairs(api_keys.values) do
        if type(api_key) ~= "table" then
            goto CONTINUE
        end
        local user_id = api_key.value.user_id
        plugins[config_name].nodes[user_id] = api_key.value
        :: CONTINUE ::
    end
    return plugins
end


function _M.plugin(plugin_name)
    local plugin_conf = core.lrucache.global("/api_keys", api_keys.conf_version, plugin_api_key)
    return plugin_conf[plugin_name]
end


function _M.api_keys()
    if not api_keys then
        return nil, nil
    end
    return api_keys.values, api_keys.conf_version
end

--local redis_client

--local function get_redis()
--    if not redis_client then
--        redis_client = core.redis.new()
--    end
--    return redis_client
--end

local function get_redis()
    return core.redis.get_instance()
end

-- set user count
function _M.set_count(user_id, count)
    local red = get_redis()
    if not red then
        return 0, "failed to initialize Redis"
    end

    local key = "/apisix/count/" .. user_id
    local ok, err = red:set(key, count)
    if err then
        core.log.error("failed to set key[" .. key .. "] from redis, err:" .. err)
        return 0, err
    end
    return count, nil
end

-- get user count
function _M.get_count(user_id)
    local red = get_redis()
    if not red then
        return 0, "failed to initialize Redis"
    end

    local key = "/apisix/count/" .. user_id
    local count, err = red:get(key)
    if err ~= nil then
        core.log.error("failed to get key[" .. key .. "] from redis, err:" .. err)
        return 0, err
    end
    return tonumber(count), nil
end

-- user count decrease
function _M.count_decline_by(user_id, number)
    local red = get_redis()
    if not red then
        return 0, "failed to initialize Redis"
    end

    local key = "/apisix/count/" .. user_id
    local count, err = red:decrby(key, number)
    if err ~= nil then
        core.log.error("failed to decrease key[" .. key .. "] from redis, err:" .. err)
        return 0, err
    end
    return tonumber(count), nil
end

function _M.count_increase_by(user_id, number)
    local red = get_redis()
    if not red then
        return 0, "failed to initialize Redis"
    end

    local key = "/apisix/count/" .. user_id
    local count, err = red:incrby(key, number)
    if err ~= nil then
        core.log.error("failed to increase key[" .. key .. "] from redis, err:" .. err)
        return 0, err
    end
    return tonumber(count), nil
end

-- init work
-- register sychonizer
function _M.init_worker()
    -- register scheduler in config_etcd to synchronize api_keys per 30s
    -- api_keys include config of schedular task
    local err
    api_keys, err = core.config.new("/api_keys", {
        automatic = true,
        item_schema = core.schema.api_key
    })
    core.log.warn("after init_worker, api_keys: ", core.json.delay_encode(api_keys))
    if not api_keys then
        error("failed to create etcd instance for fetching api_keys: " .. err)
        return
    end
end

return _M
