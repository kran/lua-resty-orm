local pgmoon = require'pgmoon'
local quote_sql_str = ngx.quote_sql_str
local assert = assert
local ipairs = ipairs
local tostring = tostring
local table_concat = table.concat
local table_insert = table.insert
local lpeg = require'lpeg'
local ngx = ngx


local open = function(conf)
    local _connect = function()
        local db = pgmoon.new(conf)
        assert(db, "failed to create pgmoon object")

        assert(db:connect(), "failed connecting to db")

        if conf.charset then
            if db.sock:getreusedtimes() == 0 then
                db:query("SET NAMES " .. conf.charset)
            end
        end

        return  {
            conn = db;
            query = function(self, str) return db:query(str) end;
            set_keepalive = function(self, ...) return db:keepalive(...) end;
            start_transaction = function() return db:query('BEGIN') end;
            commit = function() return db:query('COMMIT') end;
            rollback = function() return db:query('ROLLBACK') end;
        }
    end

    local function connect()
        local key = "trans_" .. tostring(coroutine.running())
        local conn = ngx.ctx[key]
        if conn then
            return true, conn
        end

        return false, _connect()
    end

    local config = function()
        return conf
    end

    local query = function(query_str)
        if conf.debug then
            ngx.log(ngx.DEBUG, '[SQL] ' .. query_str)
        end

        local in_trans, db = connect()
        local res, err, errno, sqlstate = db.conn:query(query_str)
        if not res then
            return nil, table_concat({"bad result: " .. err}, ', ') 
        end

        if not in_trans then
            local ok, err = db.conn:keepalive(10000, 50)
            if not ok then
                ngx.log(ngx.ERR, "failed to set keepalive: ", err)
            end
        end

        return true, res
    end

    local escape_identifier = function(id)
        local repl = '"%1"'
        local openp, endp = lpeg.P'[', lpeg.P']'
        local quote_pat = openp * lpeg.C( ( 1 - endp ) ^ 1) * endp
        return lpeg.Cs( ( quote_pat/repl + 1 ) ^ 0 ):match(id)
    end
    
    local quote_sql_str = function(str)
        return "'" .. tostring((str:gsub("'", "''"))) .. "'"
    end

    local function escape_literal(val)
        local typ = type(val)

        if typ == 'boolean' then
            return val and "TRUE" or "FALSE"
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
        return column
    end

    local get_schema = function(table_name)

        table_name = table_name:gsub('%[?([^%]]+)%]?', "'%1'")
        local ok, columns = query([[
            select column_name, data_type, character_maximum_length 
            from INFORMATION_SCHEMA.COLUMNS where table_name = ]]
            .. table_name ) 

        assert(ok, columns)

        local ok, pk = query([[ 
            SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type
            FROM   pg_index i
            JOIN   pg_attribute a ON a.attrelid = i.indrelid
            AND a.attnum = ANY(i.indkey)
            WHERE  i.indrelid = ]].. table_name .. [[::regclass
            AND    i.indisprimary;
        ]])

        assert(ok, pk)
        assert(#pk == 1, 'not implement for tables have multiple pk or none pk')

        local fields = { __pk__ = pk[1].attname }
        for _, f in ipairs(columns) do
            fields[f.column_name] = f
        end

        return fields
    end

    local limit_all = function()
        return  'ALL'
    end

    return { 
        connect = connect;
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

