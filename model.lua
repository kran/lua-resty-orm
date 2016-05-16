local fun = require'orm.func'
local cache = require'orm.cache'
local table_concat = table.concat

local function define_model(DB, Query, attrs)
    local Model = {}

    local typ = type(attrs)
    if typ == 'string' then
        local tname = attrs
        local conf = DB.config()
        local cache_key = table_concat({'orm', conf.host, conf.port, conf.database, tname}, '^') 
        local data, stale = cache:get(cache_key)
        if not data then
            attrs = DB.fetch_schema(tname)
            cache:set(cache_key, fun.table_clone(attrs), conf.expires)
        else
            attrs = fun.table_clone(data)
        end
    elseif typ == 'table' then
        -- pass
    else
        error('attributes required')
    end


    local table_name = attrs.__table__
    assert(table_name, 'table name required')
    attrs.__table__ = nil

    local primary_key = attrs.__pk__ or 'id'
    assert(attrs[primary_key], 'primary key required')
    attrs.__pk__ = nil

    local function filter_attrs(params)
        return fun.kmap(function(k, v)
            if type(k) == 'number' then
                return k, v
            elseif attrs[k] ~= nil then
                return k, v
            end
        end, params)
    end

    local function pop_models(ok, rows)
        if not ok then return ok, rows end

        return ok, fun.map(function(row)
            local model = Model.new(row, false)
            model:trigger('AfterFind')
            return model
        end, rows)
    end

    Model.find_all = function(cond, ...)
        local ok, res = Query():from(table_name)
                :where(cond, ...):all(pop_models)
        if ok and #res == 0 then
            return false, "no record found"
        end

        return ok, res
    end

    Model.find_one = function(cond, ...)
        local ok, res = Query():from(table_name)
                :where(cond, ...):one(pop_models)

        if ok and not res then
            return false, "no record found"
        end

        return ok, res
    end

    Model.query = function()
        return Query():from(table_name)
    end

    Model.update_where = function(set, cond, ...)
        return Query():update(table_name)
                :where(cond, ...):set(set):exec()
    end

    Model.delete_where = function(cond, ...)
        return Query():delete(table_name)
                :where(cond, ...):exec()
    end


    local ModelMeta = {}

    ModelMeta.__index = function(self, key)
        if ModelMeta[key] then
            return ModelMeta[key]
        else
            return self.__attrs__[key]
        end
    end

    ModelMeta.__newindex = function(self, k, v)
        if attrs[k] ~= nil then
            self.__attrs__[k] = v
            self.__dirty_attrs__[k] = true
        end
    end

    function ModelMeta:set_dirty(attr)
        self.__dirty_attrs__[attr] = true
    end

    function ModelMeta:get_dirty_attrs()
        local count = 0
        local res = fun.kmap(function(k, v)
            count = count + 1
            return k, self.__attrs__[k] 
        end, self.__dirty_attrs__)
        return res, count
    end

    function ModelMeta:save()
        if self[primary_key] then -- update
            self:trigger('BeforeSave')
            local res = "no dirty attributes"
            local ok = false
            local dirty_attrs, count = self:get_dirty_attrs()
            if count > 0 then
                ok, res = Query():update(table_name)
                    :where(primary_key .. ' = ?d ', self[primary_key])
                    :set(dirty_attrs):exec()

                if ok then
                    self:set_none_dirty()
                end
            end

            return ok, res
        else -- insert
            self:trigger('BeforeSave')
            local ok, res = Query():insert(table_name):values(self.__attrs__):exec()

            if ok then 
                self[primary_key] = res.insert_id
                self:set_none_dirty()
                return ok, res.insert_id
            else
                return false, res
            end
        end
    end

    function ModelMeta:set_none_dirty()
        self.__dirty_attrs__ = {}
    end

    function ModelMeta:delete()
        assert(self[primary_key], 'primary key ['.. primary_key .. '] required')

        return Query():delete(table_name):where(primary_key .. '= ?d', self[primary_key]):exec()
    end

    function ModelMeta:load(data)
        if type(data) == 'table' then
            fun.kmap(function(k, v) self[k] = v end, data)
        end
    end

    function ModelMeta:trigger(event, ...)
        local method = Model['on'..event]
        if type(method) == 'function' then
            return method(self, ...)
        end
    end

    Model.new = function(data, dirty)
        local instance = { __attrs__ = {}, __rels__ = {}, __dirty_attrs__ = {}  }
        setmetatable(instance, ModelMeta)

        instance:load(data)
        if not dirty then
            instance:set_none_dirty()
        end

        return instance
    end

    return Model
end

return define_model
