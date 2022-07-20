local core = require("apisix.core")
local api_key = require("apisix.api_key")
local plugin_name = "qingcloud-auth"
local api_keys_lrucache = core.lrucache.new({
    type = "plugin",
})
local schema = {
    type = "object",
}


local _M = {
    version = 0.1,
    priority = 2505,
    type = 'auth',
    name = plugin_name,
    schema = schema,
}

-- generate api_key map from cache
local create_api_key_cache
do
    local api_key_set = {}

    function create_api_key_cache(api_keys)
        core.table.clear(api_key_set)

        for _, api_id_set in pairs(api_keys.nodes) do
            local api_key = api_id_set.api_key
            api_key_set[api_key] = api_id_set
        end

        return api_key_set
    end
end

function _M.check_schema(conf)
    return core.schema.check(schema, conf)
end

function _M.rewrite(conf, ctx)
    local key = core.request.get_host(ctx)
    core.log.warn("get_host "..key)

    -- get config in cache
    local api_key_conf = api_key.plugin(plugin_name)
    core.log.warn("api_keys:", core.json.delay_encode(api_key_conf))
    if not api_key_conf then
        return 401, { message = "Missing related consumer" }
    end

    -- form key map from cache
    -- local api_keys = core.lrucache.plugin(plugin_name, "api_keys", api_key_conf.conf_version, create_api_key_cache, api_key_conf)
    local api_keys = api_keys_lrucache("api_keys", api_key_conf.conf_version, create_api_key_cache, api_key_conf)
    core.log.warn("api_keys:", core.json.delay_encode(api_keys))
    -- check key
    local api_key_set = api_keys[key]
    if not api_key_set or type(api_key_set) ~= "table" then
        return 401, { message = "Invalid API" }
    end
    local user_id = api_key_set.user_id

    -- check account
    local count = api_key.get_count(user_id)
    if not count or (count <= 0 and count ~= -65535) then
        core.log.error("insufficient balance")
        return 401, { message = "insufficient balance" }
    end

    ctx.api_key = key
    ctx.user_id = user_id
    ctx.api_ver = api_key_conf.conf_version
    core.log.info("hit key-auth rewrite")
end

-- after call
function _M.header_filter(conf, ctx)
    local upstream_status = core.response.get_upstream_status(ctx)
    if upstream_status and upstream_status >= 200 and upstream_status < 300 then
        -- set count = count - 1
        local user_id = ctx.user_id
        ngx.timer.at(0, function()
            core.log.warn("after call, try to reduce the count for user "..user_id)
            local count = api_key.get_count(user_id)
            if count ~= -65535 then
                local _, err = api_key.count_decline_by(user_id, 1)
                if err ~= nil then
                    core.log.error("failed to charge account, err: ", err)
                end
            end
        end)
    end
end

return _M
