local setmetatable = setmetatable
local assert = assert
local type = type
local ipairs = ipairs
local pairs = pairs
local rawget = rawget
local rawset = rawset
local unpack = unpack
local sprintf = string.format
local table_concat = table.concat
local table_insert = table.insert

local _R = {}  -- read from db
local _W = {}  -- write to db

local call_hook = function(model, hook_name, ...)
    local hook = model.hooks[hook_name]
    if type(hook) == 'function' then
        return hook(model, ...)
    end
end

-- `tbl` changed
local as_record = function(model, tbl) 
    local dirty = {}
    local mt = setmetatable({
        get_model = function() return model  end;
        get_dirty = function() return dirty end;
        clear_dirty = function() dirty = {} end;
    }, { __index = _W })
    mt.__index = mt

    return setmetatable(tbl, mt)
end

local as_collection = function(resultset)
    local casted = function() 
        local res = {}
        for _, v in ipairs(resultset) do
            table_insert(res, v:casted())
        end
        return res
    end
    return setmetatable(resultset, {
        __index = { casted = casted }
    })
end


_W.casted = function(self)
    local result = {}
    for k, _ in pairs(self) do
        result[k] = self:get(k)
    end

    return result
end

_W.get = function(self, key)
    local val = rawget(self, key)

    local fd = self.get_model().table.fields[key]
    if not fd then return val end

    local cast_out = fd.cast and fd.cast[2]
    if type(cast_out) ~= 'function' then 
        return val
    end

    local ok, res = pcall(cast_out, val)
    if not ok then 
        return fd.default
    else
        return res
    end
end

_W.set = function(self, key, val)
    if val == nil then
        error(sprintf('value for "%s" is nil', key))
    end
    local fd = self.get_model().table.fields[key]
    if fd then
        local cast_in = fd.cast and fd.cast[1]
        if type(cast_in) == 'function' then
            val = cast_in(val)
        end
        self.get_dirty()[key] = val
    end
    rawset(self, key, val)
end

_W.delete = function(self)
    local table = self.get_model().table

    if not self[table.pk] then
        return table.mapper.sql_error('primary key required'), true
    end

    local res, err = self.get_model():prepare_delete()
        :set_named('W', '?i = ' .. table.fields[table.pk].fmt, table.pk, self[table.pk]):query()

    if not err then
        self:clear_dirty()
    end

    return res, err
end

_W.save = function(self)
    local table = self.get_model().table
    local params = {}
    local sql;
    local res
    local err

    if self[table.pk] then  -- UPDATE
        local err_msg, has_err = call_hook(self.get_model(), 'before_save', self, 'update')
        if has_err then
            return table.mapper.sql_error(err_msg), true
        end

        local parts = {}
        for k, v in pairs(self.get_dirty()) do
            table_insert(parts, sprintf("[%s] = %s", k, table.fields[k].fmt))
            table_insert(params, v)
        end
        if #parts == 0 then 
            return table.mapper.sql_error('no field to update'), true
        end

        table_insert(params, self[table.pk])

        sql = sprintf("UPDATE [%s] SET %s WHERE [%s] = %s", 
                table.table_name, table_concat(parts, ', '), table.pk, table.fields[table.pk].fmt)

        res, err = self.get_model():prepare(sql, unpack(params)):query()
    else  -- INSERT
        local err_msg, has_err = call_hook(self.get_model(), 'before_save', self, 'insert')
        if has_err then
            return table.mapper.sql_error(err_msg), true
        end
        local fields = {}
        local phs = {}
        for k, v in pairs(self.get_dirty()) do
            table_insert(fields, sprintf("[%s]", k))
            table_insert(phs, table.fields[k].fmt)
            table_insert(params, v)
        end
        if #fields == 0 then 
            return table.mapper.sql_error('no field to save'), true
        end

        local returning = table.mapper.driver.returning(sprintf('[%s] AS [%s]', table.pk, 'insert_id'))

        sql = sprintf("INSERT INTO [%s] (%s) VALUES (%s) %s", 
                table.table_name, table_concat(fields, ', '), table_concat(phs, ', '), returning)

        res, err = self.get_model():prepare(sql, unpack(params)):query()
        if not err then
            self[table.pk] = res.insert_id
        end
    end


    if err then 
        return res, true 
    else
        self:clear_dirty()
        return res, nil
    end
end

_W.load = function(self, tbl)
    local fields = self.get_model().table.fields
    for k, v in pairs(tbl) do
        if fields[k] then 
            self:set(k, v)
        end
    end
end

-- never modify the `tbl` argument
_R.create_record = function(self, tbl)
    local record = as_record(self, {})
    record:load(tbl)
    return record
end


_R.prepare = function(self, sql, ...)
    return self.table.mapper:prepare(sql, ...)
end

_R.prepare_select = function(self, where, ...)
    assert(where, 'where condition required')
    return self.table.mapper:prepare('select')
        :append_named('S', '*')
        :append('from')
        :append_named('T', '?i', self.table.table_name)
        :append('where')
        :append_named('W', where, ...)
        :append_named('G', '')
        :append_named('H', '')
        :append_named('O', '')
        :append_named('L', '')
        :append_named('LOCK', '')
end

_R.prepare_update = function(self)
    return self.table.mapper:prepare('update')
        :append_named('T', '?i', self.table.table_name)
        :append('set')
        :append_named('V', '')
        :append('where')
        :append_named('W', '')
        :append_named('L', '')
end

_R.prepare_insert = function(self)
    return self.table.mapper:prepare('insert into')
        :append_named('T', '?i', self.table.table_name)
        :append('(')
        :append_named('F', '')
        :append(') values (')
        :append_named('V', '')
        :append(')')
end

_R.prepare_delete = function(self)
    return self.table.mapper:prepare('delete from')
        :append_named('T', '?i', self.table.table_name)
        :append('where')
        :append_named('W', '')
end

_R.find_all = function(self, sql, ...)
    sql = sql or '1 = 1'
    -- local fsql = sprintf("SELECT * FROM [%s] WHERE %s", self.table.table_name, sql)
    -- local rows, err = self:prepare(fsql, ...):query()
    local rows, err = self:prepare_select(sql, ...):query()

    if err then return rows, err end

    for _, v in ipairs(rows) do
        as_record(self, v)
        call_hook(self, 'after_find', v)
    end

    return as_collection(rows), nil
end

_R.find_one = function(self, sql, ...)
    local rows, err = self:find_all(sql, ...)
    if err then return row, err end

    if #rows > 1 then 
        ngx.log(ngx.DEBUG, sql, ' result number = ', #rows)
    end

    return rows[1], nil
end

_R.collect = function(self, sel, ...)
    -- local fsql = sprintf("SELECT %s FROM [%s]", sel, self.table.table_name)
    return self:prepare_select():set_named('S', sel, ...)
end

local find_one_pattern = '^find_one_by_(.+)$'
local find_all_pattern = '^find_all_by_(.+)$'

local model_index = function(self, key)
    local match = key:match(find_one_pattern)
    if match then 
        return function(this, val) 
            return self:find_one(sprintf("[%s] = ? LIMIT 1", match), val)
        end
    end

    local match = key:match(find_all_pattern)
    if match then 
        return function(this, val) 
            return self:find_all(sprintf("[%s] = ?", match), val)
        end
    end

    return _R[key]
end

local model = function(table, name)
    assert(name, 'model name required')
    local hooks = {}
    return setmetatable({ table = table, hooks = hooks, name = name }, { __index = model_index})
end

return function(mapper)
    -- local hooks = { }
    return function(table_name, pk, fields)
        return { 
            model      = model;
            table_name = table_name;
            pk         = pk;
            mapper     = mapper;
            -- hooks      = hooks;
            fields     = fields;
            get_schema = function()
                return mapper.driver.get_schema(table_name)
            end;
        }
    end
end
