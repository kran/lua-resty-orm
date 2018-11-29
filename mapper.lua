local lpeg = require('lpeg')
local P, Cs = lpeg.P, lpeg.Cs
local cjson = require('cjson')
local setmetatable = setmetatable
local pcall        = pcall
local type         = type
local assert       = assert
local tostring     = tostring
local tonumber     = tonumber
local table_insert = table.insert
local table_concat = table.concat
local ngx_null     = ngx.null
local sprintf = string.format

local _M = {  }
local _P = setmetatable({ __type = 'P' }, { 
    __index = _M, 
    __tostring = function(self) return self:tosql() end
})

local sql_error = function(err)
    return setmetatable({ err = err }, {
        __index = function(self) return self end;
        __call  = function(self) return self, true end; --> res, err
    })
end

_P.append = function(self, sql, ...)
    local ok, res = pcall(self.quote, self, sql, {...})
    if not ok then 
        return sql_error(res), true
    else
        table_insert(self.sql, res)
        return self, nil
    end
end

_P.append_named = function(self, name, sql, ...)
    local self, err = self:append(sql, ...)
    if err then return self, true end

    if self.names[name] then
        return sql_error(sprintf('name "%s" already exists', name)), true
    end

    self.names[name] = #self.sql

    return self, nil
end

_P.get_named_part = function(self, name)
    local index = self.names[name]
    if not index then 
        return sql_error(sprintf('name "%s" not found', name)), true
    end

    return self.sql[index], nil
end

_P.replace = function(self, name, sql, ...)
    local index = self.names[name]
    if not index then 
        return sql_error(sprintf('name "%s" not found', name)), true
    end

    local ok, res = pcall(self.quote, self, sql, {...})
    if not ok then
        return sql_error(res), true
    end

    self.sql[index] = res

    return self, nil
end

_P.scalar = function(self)
    local res, err = self:first(nil)
    if err then return res, err end

    local _, val = next(res)
    return val, nil
end

_P.first = function(self, dao)
    local res, err = self:query(dao)
    if err then 
        return res, err
    else
        return res[1], nil
    end
end

_P.query = function(self, dao)
    local ok, res = self.driver.query(self:tosql())
    if not ok then
        return sql_error(res), true
    end

    return res, nil
end

_P.tosql = function(self) 
    return table_concat(self.sql, ' ')
end

_M.quote_identity = function(self, str)
    return self.driver.escape_identifier(str)
end

_M.quote = function(self, sql, params)
    local sql = self:quote_identity(sql)
    assert(type(params) == 'table', 'params must be array')
    local params_count = #params

    local repfunc = function(args)
        local counter = 0
        return function(func)
            return function()
                counter = counter + 1
                if counter > params_count then 
                    assert(false, "no enough parameters for sql")
                end
                return func(args[counter])
            end
        end
    end

    local R = repfunc(params)

    local pt  = P'?t'/R(function(arg)
        if type(arg) ~= 'table' then
            arg = { arg }
        end

        return self.driver.escape_literal(arg)
    end)
    -- local pjson = P'?j'/R(function(arg)
    --     assert(type(arg) == 'table', 'parameter must be table type')
    --     return self.driver.quote_sql_str(cjson.encode(arg))
    -- end)
    local pb = P'?b'/R(function(arg) return arg and 1 or 0 end)
    local pe = P'?e'/R(tostring)
    local pd  = P'?d'/R(tonumber)
    local pi  = P'?i'/R(function(arg) 
        return self:quote_identity(sprintf("[%s]", arg))
    end)
    local pn  = P'?n'/R(function(arg) 
        if arg == ngx_null then
            return 'NULL'
        end
        return arg and 'NOT NULL' or 'NULL' 
    end)
    local ps  = P'?s'/R(self.driver.quote_sql_str)
    local pa  = P'?'/R(self.driver.escape_literal)

    local patt = Cs((pb + pe + pt + pd + pi + pn + ps + pa + 1)^0)
    return patt:match(sql)
end

_M.prepare = function(self, sql, ...) 
    local ok, res = pcall(self.quote, self, sql, {...})

    -- DEBUG
    -- local res = self:quote(sql, {...})
    -- local ok = true

    if not ok then 
        return sql_error(res), true
    else
        return setmetatable({ sql = { res }, names = {}, driver = self.driver }, { __index = _P }), nil
    end
end

return function(driver)
    return setmetatable({ driver = driver }, { __index = _M })
end

