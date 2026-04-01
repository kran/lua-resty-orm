local type = type
local tostring = tostring
local string_gsub = string.gsub
local error = error

local _M = {}

local quote_sql_str = ngx and ngx.quote_sql_str
if not quote_sql_str then
    quote_sql_str = function(s)
        return "'" .. string_gsub(s, "'", "''") .. "'"
    end
end

local ngx_null = ngx and ngx.null

local function escape_value(val)
    local t = type(val)

    if t == "string" then
        return quote_sql_str(val)

    elseif t == "number" then
        return tostring(val)

    elseif t == "boolean" then
        return val and "TRUE" or "FALSE"

    elseif val == nil or val == ngx_null then
        return "NULL"

    else
        error("resty_formatter: cannot escape sql value of type '" .. t .. "'")
    end
end

function _M.format(idx, arg)
    return escape_value(arg)
end

return _M
