local table_insert = table.insert
local unpack = unpack
local ipairs = ipairs
local pairs = pairs

local _M = {  }


local function kmap(func, tbl)
    if not tbl then return end

    local res = {}
    for k, v in pairs(tbl) do
        local rk, rv = func(k, v)
        if rk then 
            res[rk] = rv 
        else 
            table_insert(res, rv)
        end
    end

    return res
end

_M.kmap = kmap

local function map(func, tbl) 
    if not tbl then return end

    local res = {}
    for k, v in pairs(tbl) do
        local rv = func(v, k)
        if rv ~= nil then 
            table_insert(res, rv)
        end
    end

    return res
end

_M.map = map

local function filter(func, tbl)
    return kmap(function(k, v)
        local rk, rv = func(k, v)
        if rk then
            return rk, rv
        end
    end, tbl)
end

_M.filter = filter


local function reduce(func, acc, tbl) 
    if not tbl then return end

    for k, v in pairs(tbl) do
        acc = func(k, v, acc)
    end

    return acc
end

_M.reduce = reduce

local function curry(func, ...) 
    if select('#', ...) == 0 then return func end
    local args = { ... }
    return function( ... )
        local clone = { unpack(args) } 
        for _, v in ipairs{...} do table_insert(clone , v) end
        return func(unpack(clone))
    end
end
_M.curry = curry

local function chain(...)
    local args = { ... }
    return function(...)
        local real_arg = {...}
        for i=1, #args do
            real_arg = { args[i](unpack(real_arg)) }
        end
        return unpack(real_arg)
    end
end
_M.chain = chain


_M.table_keys = curry(kmap, function(k, v) return nil, k end)
_M.table_vals = curry(map, function(v) return v end)
_M.table_clone = curry(kmap, function(k,v) return k, v end)


return _M
