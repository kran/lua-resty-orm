local setmetatable = setmetatable
local assert = assert
local type = type
local ipairs = ipairs
local pairs = pairs
local unpack = unpack
local sprintf = string.format
local table_concat = table.concat
local table_insert = table.insert

local _R = {}
local _W = {}

local call_hook = function(model, hook_name, ...)
    local hook = model.table.hooks[hook_name]
    if type(hook) == 'function' then
        return hook(model, ...)
    end
    return ...
end

_W.get = function(self, key, cast) 
    local val = rawget(self, key)

    if not cast then return val end

    local fd = self.get_model().table.fields[key]
    if not fd then return val end

    local cast_out = fd.cast and fd.cast[2]
    if type(cast_out) ~= 'function' then 
        return val
    end

    return fd.cast(val)
end;

_W.set = function(self, key, val)
    rawset(self, key, val)

    local fd = self.get_model().table.fields[key]
    if fd then
        local cast_in = fd.cast and fd.cast[1]
        if type(cast_in) == 'function' then
            val = cast_in(val)
        end
        self.get_dirty()[key] = val
    end
end;

_W.delete = function(self)
    local table = self.get_model().table
    if not self[table.pk] then
        return { err = 'primary key required' }, true
    end

    local sql = sprintf("DELETE FROM [%s] WHERE [%s] = %s",
        table.table_name, table.pk, table.fields[table.pk].fmt);

    return self.get_model():prepare(sql, self[table.pk]):query()
end

_W.save = function(self)
    local table = self.get_model().table
    if self[table.pk] then  -- UPDATE
        local parts = {}
        local params = {}
        for k, v in pairs(self.get_dirty()) do
            table_insert(parts, sprintf("[%s] = %s", k, table.fields[k].fmt))
            table_insert(params, v)
        end
        if #parts == 0 then 
            return self, nil
        end

        table_insert(params, self[table.pk])

        local sql = sprintf("UPDATE [%s] SET %s WHERE [%s] = %s", 
                table.table_name, table_concat(parts, ', '), table.pk, table.fields[table.pk].fmt)

        return self.get_model():prepare(sql, unpack(params)):query()
    else  -- INSERT
        local fields = {}
        local phs = {}
        local params = {}
        for k, v in pairs(self.get_dirty()) do
            table_insert(fields, sprintf("[%s]", k))
            table_insert(phs, table.fields[k].fmt)
            table_insert(params, v)
        end
        if #fields == 0 then 
            return self, nil
        end

        local sql = sprintf("INSERT INTO [%s] (%s) VALUES (%s)", 
                table.table_name, table_concat(fields, ', '), table_concat(phs, ', '))

        return self.get_model():prepare(sql, unpack(params)):query()
    end
end

_W.load = function(self, tbl)
    local fields = self.table.fields
    for k, v in pairs(tbl) do
        if fields[k] then 
            self:set(k, v)
        end
    end
end

-- `tbl` changed
local as_record = function(model, tbl) 
    local dirty = {}
    local mt = setmetatable({
        get_model = function() return mode  end;
        get_dirty = function() return dirty end;
    }, { __index = _W })
    mt.__index = mt

    return setmetatable(tbl, mt)
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

_R.find_all = function(self, sql, ...)
    local fsql = sprintf("SELECT * FROM [%s] WHERE %s", self.table.table_name, sql)
    local rows, err = self:prepare(fsql, ...):query()

    if err then return rows, err end

    for _, v in ipairs(rows) do
        as_record(self, v)
        call_hook(self, 'after_find', v)
    end

    return rows, nil
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
    local fsql = sprintf("SELECT %s FROM [%s]", sel, self.table.table_name)
    return self:prepare(fsql, ...)
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

local model = function(table)
    return setmetatable({ table = table }, { __index = model_index})
end

local define_table = function(mapper)
    local hooks = {
        after_find = function(this, row) 
            return row 
        end;
    }
    return function(table_name, pk, fields)
        return { 
            model      = model;
            table_name = table_name;
            pk         = pk;
            mapper     = mapper;
            hooks      = hooks;
            fields     = fields;
        }
    end
end

return function(mapper)
    return define_table(mapper)
end

