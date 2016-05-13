local mysql = require'resty.mysql'

local open = function(conf)
    local connect = function()
        local db = mysql:new()
        local ok, err, errno, sqlstate = db:connect(conf)

        assert(ok, "failed to connect: ", err, ": ", errno, " ", sqlstate)

        if conf.charset then
            if db:get_reused_times() == 0 then
                db:query("SET NAMES " .. conf.charset)
            end
        end

        return db
    end

    local query = function(query_str)
        local db = connect()
        local res, err, errno, sqlstate = db:query(query_str)

        if not res then
            return false, "bad result: ", err, ": ", errno, ": ", sqlstate, "."
        end

        local ok, err = db:set_keepalive(10000, 50)
        if not ok then
            ngx.log(ngx.ERR, "failed to set keepalive: ", err)
        end

        return true, res
    end

    local get_quote_char = function()
        return '`'
    end

    return { 
        query = query;
        get_quote_char = get_quote_char;
    }
end


return open
