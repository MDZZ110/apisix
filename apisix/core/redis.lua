-- Redis API
--
-- @module core.redis

local redis_cluster    = require("resty.rediscluster")
local fetch_local_conf = require("apisix.core.config_local").local_conf
local clone_tab        = require("table.clone")
local log              = require("apisix.core.log")
local json             = require("apisix.core.json")
local lrucache         = require("apisix.core.lrucache")
local resolver         = require("apisix.core.resolver")
local ngx_re_find      = ngx.re.find
local shared_dict      = "plugin-qingcloud-redis-cluster-slot-lock"
local ipairs           = ipairs
local tonumber         = tonumber

local client_cache = lrucache.new({ttl = 300, count = 20})

local _M = {}
_M._VERSION = '0.01'

local function _is_addr(host)
    return ngx_re_find(hostname, [[\d+?\.\d+?\.\d+?\.\d+$]], "jo")
end

local function parse_domain(host, enable_host)
    if enable_host then
        return host
    end

    if _is_addr(host) then
        return host
    end

    local ip, err = resolver.parse_domain(host)
    if err then
        return nil, "dns resolver domain failed: " .. host .. " error: ".. err
    end
end

local function new_redis_cluster()
    local local_conf, err = fetch_local_conf()
    if not local_conf then
        log.error("failed to fetch local conf for redis")
        return nil, err
    end

    local redis_conf = clone_tab(local_conf.redis)
    redis_conf.dict_name = shared_dict

    local serv_list_with_ip = {}
    for i, item in ipairs(redis_conf.serv_list) do
        local node_ip, err = parse_domain(item.ip, redis_conf.enable_host)
        if err then
            return nil, err
        end
        serv_list_with_ip[i] = { ip = node_ip, port = tonumber(item.port) }
    end
    redis_conf.serv_list = serv_list_with_ip

    log.info(json.delay_encode(redis_conf))
    local redis_cli = redis_cluster:new(redis_conf)

    return redis_cli
end

function _M.get_instance()
    local redis_instance, err = client_cache("redis_instance", nil, new_redis_cluster)
    if err then
        log.error("get redis client failed ".. err)
        return nil
    end

    return redis_instance
end

return _M
