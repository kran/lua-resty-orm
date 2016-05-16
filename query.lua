local fun = require'orm.func'
local lpeg = require'lpeg'

local table_insert = table.insert
local table_concat = table.concat
local ipairs, pairs = ipairs, pairs
local strlen = string.len
local tostring = tostring
local type, unpack = type, unpack
local P, R, S = lpeg.P, lpeg.R, lpeg.S
local C, Ct, Cs = lpeg.C, lpeg.Ct, lpeg.Cs
local quote_sql_str = ngx.quote_sql_str

local _T = {}  


local function isqb(tbl)
    return type(tbl) == 'table' and tbl._type == 'query'
end

local function quote_key(qchar, key)
    local openp, endp = P'[', P']'
    local quote_pat = openp * C(( 1 - endp)^1) * endp
    local repl = qchar .. '%1' .. qchar
    return Cs((quote_pat/repl + 1)^0):match(key)
end


local function quote_var(val)
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
        return table_concat(fun.map(quote_var, val), ', ')
    else
        return tostring(val)
    end
end

local expr = function(str)
    local expression = str
    return setmetatable({ }, {
        __tostring = function(tbl)
            return expression
        end;
        __index = function(tbl, key)
            if key == '_type' then 
                return 'expr' 
            end
        end;
        __newindex = function(tbl, key, val) 
            error('no new value allowed')
        end
    })
end

local build_cond = function(condition, params)
    if not params then params = {} end

    local replace_param = function(args)
        local counter = { 0 }
        local typ = type(args)
        return function(func)
            return function(cap)
                counter[1] = counter[1] + 1
                return func(args[counter[1]])
            end
        end
    end

    local repl = replace_param(params)

    -- table
    local parr  = P'?t'/repl(function(arg)
        if type(arg) ~= 'table' then
            arg = { arg }
        end

        return quote_var(arg)
    end)
    local pbool = P'?b'/repl(function(arg) return arg and 1 or 0 end)
    local porig = P'?e'/repl(function(arg) return tostring(arg) end)
    -- local pgrp  = P'?p'/repl(function(arg) return '('.. tostring(arg) ..')' end)
    local pnum  = P'?d'/repl(tonumber)
    local pnil  = P'?n'/repl(function(arg) return arg and 'NOT NULL' or 'NULL'end)
    local pstr  = P'?s'/repl(quote_sql_str)
    local pany  = P'??'/repl(quote_var)


    local patt = Cs((porig + parr + pnum + pstr + pbool + pnil + pany + 1)^0)
    local cond = '(' .. patt:match(condition) .. ')'


    return cond

end

_T.one = function(self, callback)
    assert(self._state == 'select', 'select context required')

    local ok, res = self:limit(1):exec(callback)
    if ok then res = res[1] end
    return ok, res
end


_T.exec = function(self, callback)
    local ok, res = self._db.query(self:build())
    if callback then
        return callback(ok, res)
    else
        return ok, res
    end
end

_T.all = function(self, callback)
    assert(self._state == 'select', 'select context required')
    return self:exec(callback)
end

_T.from = function(self, tname, alias)
    if isqb(tname) then
        if alias then tname = tname:as(alias) end
        self._from = tname:build()
    else
        if alias then tname = tname .. ' ' .. alias end
        self._from = self:quote_key(tname) 
    end

    return self
end

_T.build_where = function(self, cond, params) 
    cond = self:quote_key(cond)
    return build_cond(cond, params)
end

_T.quote_key = function (self, key)
    return quote_key(self._quote_char, key)
end

_T.select = function(self, fields)
    self._select = self:quote_key(fields)
    return self
end


local function add_cond(self, field, op, cond, params)
    if not params then params = {} end
    if type(self[field]) ~= 'table' then self[field] = {} end

    table_insert(self[field], { op, cond, params })
end

_T.where = function(self, condition, ...)
    return self:and_where(condition, ...)
end

_T.or_where = function(self, condition, ...)
    add_cond(self, '_where', 'AND', condition, {...})
    return self
end

_T.and_where = function(self, condition, ...)
    add_cond(self, '_where', 'OR', condition, {...})
    return self
end

_T.having = function(self, condition, ...)
    return self:and_having(condition, ...)
end

_T.and_having = function(self, condition, ...)
    add_cond(self, '_having', 'AND', condition, {...})
    return self
end

_T.or_having = function(self, condition, ...)
    add_cond(self, '_having', 'OR', condition, {...})
    return self
end

_T.join = function(self, tbl, mode, cond, param)
    local cond = build_cond(self:quote_key(cond), param)
    if not self._join then self._join = '' end
    self._join =  table_concat({self._join, mode, 'JOIN', self:quote_key(tbl), 'ON', cond}, ' ')
    return self
end

_T.left_join = function(self, tbl, cond, ...)
    return self:join(tbl, 'LEFT', cond, {...})
end

_T.right_join = function(self, tbl, cond, ...)
    return self:join(tbl, 'RIGHT', cond, {...})
end

_T.inner_join = function(self, tbl, cond, ...)
    return self:join(tbl, 'INNER', cond, {...})
end

_T.group_by = function(self, ...)
   self._group_by = false

   local args = { ... }
   if #args > 0 then
        self._group_by = fun.reduce(function(k, v, acc) 
            if not acc then 
                return self:quote_key(v)
            else
                return acc .. ', ' .. self:quote_key(v) 
            end
        end, false, args)
    end
   return self
end

_T.order_by = function(self, ...)
   self._order_by = false

   local args = { ... }
   if #args > 0 then
        self._order_by = fun.reduce(function(k, v, acc) 
            if not acc then 
                return self:quote_key(v)
            else
                return acc .. ', ' .. self:quote_key(v) 
            end
        end, false, args)
   end
   return self
end

_T.limit = function(self, ...)
    self._limit = table_concat({...}, ', ')
    return self
end

_T.as = function(self, as)
    self._alias = as
    return self
end

_T.set = function(self, key, val)
    self._set = self._set or {  }
    if type(key) ~= 'table' then
        key = { [key] = val }
    end

    for k, v in pairs(key) do
        self._set[k] = v
    end

    return self
end

_T.values = function(self, vals)
    self._values = self._values or {  }
    for k, v in pairs(vals) do
        self._values[k] = v
    end
    
    return self
end


_T.delete = function(self, table_name) 
    self._state = 'delete'
    if table_name then
        self:from(table_name)
    end
    return self
end

_T.update = function(self, table_name) 
    self._state = 'update'
    if table_name then
        self:from(table_name)
    end
    return self
end

_T.insert = function(self, table_name)
    self._state = 'insert'
    if table_name then
        self:from(table_name)
    end
    return self
end

_T.for_update = function(self)
    self._for_update = 'FOR UPDATE'
    return self
end


_T.build = function(self, ...)
    local ctx = self._state

    local _make = function(fields)
        if not fields then return end

        return fun.reduce(function(k, v, acc)
            local tmp = self:build_where(v[2], v[3])
            if strlen(acc) > 0 then
                return table_concat({acc, v[1], tmp}, ' ')
            else
                return tmp
            end
        end, '', fields)
    end

    local _concat = function(f)
        if f[2] and strlen(f[2]) > 0 then 
            return f[1] .. ' ' .. f[2]
        end
        return nil 
    end

    local concat = fun.curry(fun.map, _concat)

    local builders = {
        select = function() 
            local sql = table_concat(concat{
                {'SELECT',   self._select},
                {'FROM',     self._from},
                {'',         self._join }, -- join
                {'WHERE',    _make(self._where)},
                {'GROUP BY', self._group_by},
                {'HAVING',   _make(self._having)},
                {'ORDER BY', self._order_by},
                {'LIMIT',    self._limit },
                {'', self._for_update} -- for update
            }, " ")

            if self._alias then
                sql = '(' .. sql ..') AS ' .. self._alias
            end

            return sql
        end;

        delete = function()
            return table_concat(concat{
                { 'DELETE FROM', self._from },
                { 'WHERE', _make(self._where) }
            }, ' ')
        end;

        update = function()
            return table_concat(concat{
                { 'UPDATE', self._from },
                { 'SET',  fun.reduce(function(k, v, acc)
                    local where = ''
                    if type(k) == 'number' then 
                        where = self:quote_key(v)
                    else
                        where = self:quote_key(k) .. '=' .. quote_var(v)
                    end
                    if not acc then return where end
                    return acc .. ', ' .. where
                end, nil, self._set)},
                { 'WHERE', _make(self._where) }
            }, ' ')
        end;

        insert = function()
            -- insert into `table` (f1, f2) values ( v1, v2)
            local keys = fun.table_keys(self._values)
            local vals = fun.map(function(v, k) 
                return quote_var(self._values[v])
            end, keys)

            return table_concat(concat{
                { 'INSERT INTO', self._from },
                { '', '('.. self:quote_key(table_concat(keys, ', ')) .. ')'},
                { 'VALUES', '(' .. table_concat(vals, ', ') .. ')' }
            }, ' ')

        end;
    }

    return builders[ctx](...)

end

local function create_query(db)

    local qb = {
        _db         = db,
        _quote_char = db.get_quote_char(),
        _state      = 'select',
        _type       = 'query',

        _from        = false,
        _select      = '*',
        _join        = false,
        _where       = false,
        _using_index = false,
        _having      = false,
        _group_by    = false,
        _limit       = false,
        _alias       = false,
        _order_by    = false,
        _set         = false,
        _values      = false,
        _for_update  = false,
    }

    local mt = {
        __index = _T,
        __newindex = function(tbl, key, val)
            error('no new value allowed')
        end;
        __tostring = function(self)
            return self:build()
        end;
    }

    return setmetatable(qb, mt)
end


return { 
    expr   = expr,
    create = create_query,
}
