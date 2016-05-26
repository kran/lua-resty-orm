local mysql = require'resty.mysql'
local quote_sql_str = ngx.quote_sql_str
local assert = assert
local ipairs = ipairs
local table_concat = table.concat
local table_insert = table.insert
local lpeg = require'lpeg'
local quote_sql_str = ngx.quote_sql_str

local open = function(conf)
    local connect = function()
        local db, err = mysql:new()
        assert(not err, "failed to create: ", err)

        local ok, err, errno, sqlstate = db:connect(conf)
        assert(ok, "failed to connect: ", err, ": ", errno, " ", sqlstate)

        if conf.charset then
            if db:get_reused_times() == 0 then
                db:query("SET NAMES " .. conf.charset)
            end
        end

        return db
    end

    local config = function()
        return conf
    end

    local query = function(query_str)
        if conf.debug then
            ngx.log(ngx.DEBUG, '[SQL] ' .. query_str)
        end

        local db = connect()
        local res, err, errno, sqlstate = db:query(query_str)
        if not res then
            return false, table_concat({"bad result: " .. err, errno, sqlstate}, ', ') 
        end

        if err == 'again' then res = { res } end
        while err == "again" do
            local tmp
            tmp, err, errno, sqlstate = db:read_result()
            if not tmp then
                return false, table_concat({"bad result: " .. err, errno, sqlstate}, ', ') 
            end

            table_insert(res, tmp)
        end

        local ok, err = db:set_keepalive(10000, 50)
        if not ok then
            ngx.log(ngx.ERR, "failed to set keepalive: ", err)
        end

        return true, res
    end

    local escape_identifier = function(id)
        local repl = '`%1`'
        local openp, endp = lpeg.P'[', lpeg.P']'
        local quote_pat = openp * lpeg.C(( 1 - endp)^1) * endp
        return lpeg.Cs((quote_pat/repl + 1)^0):match(id)
    end

    local function escape_literal(val)
        local typ = type(val)

        if typ == 'boolean' then
            return val and 1 or 0
        elseif typ == 'string' then
            return quote_sql_str(val)
        elseif typ == 'number' then
            return val
        elseif typ == 'nil' then
            return "NULL"
        elseif typ == 'table' then
            if val._type then 
                return tostring(val) 
            end
            return table_concat(fun.map(escape_literal, val), ', ')
        else
            return tostring(val)
        end
    end

    local returning = function(column)
        return false
    end

    local get_schema = function(table_name)

        table_name = table_name:gsub('%[?([^%]]+)%]?', "'%1'")

        local ok, res = query([[
            select column_name, data_type, column_key, character_maximum_length 
            from INFORMATION_SCHEMA.COLUMNS where table_name = ]] .. table_name) 

        assert(ok, res)


        say(res)

        local fields = {  }
        for _, f in ipairs(res) do
            fields[f.column_name] = f
            if f.column_key == 'PRI' then
                if fields.__pk__ then
                    error('not implement for tables have multiple pk')
                end
                fields.__pk__ = f.column_name
            end
        end

        return fields
    end

    local limit_all = function()
        return  '18446744073709551615'
    end

    return { 
        query = query;
        get_schema = get_schema;
        config = config;
        escape_identifier = escape_identifier;
        escape_literal = escape_literal;
        quote_sql_str = quote_sql_str;
        returning = returning;
        limit_all = limit_all;
    }
end


return open
