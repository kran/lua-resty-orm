local fun = require'orm.func'
local cache = require'orm.cache'
local table_concat = table.concat
local setmetatable = setmetatable
local rawget = rawget
local assert = assert
local type = type

local function define_model(DB, Query, table_name)

    local _M = {  }
    local _relations = {  }

    assert(type(table_name) == 'string', 'table name required')
    table_name = DB.escape_identity(table_name)

    _M.table_name = function() 
        return table_name 
    end

    -- User.has_one{ model = 'models.profile', as = 'profile', link = { 'user_id', 'id'} }
    _M.has_one = function(conf)
        _relations[conf.as] = conf
    end

    _M.has_many = _M.has_one

    local _init_model = function(Model)

        local attrs 

        local conf = DB.config()
        local cache_key = table_concat({'orm', conf.host, conf.port, conf.database, table_name}, '^') 
        local data, stale = cache:get(cache_key)

        if not data then
            attrs = DB.get_schema(table_name)
            cache:set(cache_key, fun.table_clone(attrs), conf.expires)
        else
            attrs = fun.table_clone(data)
        end

        assert(attrs, 'initializing model failed')
        assert(attrs.__pk__, 'primary key required')
        local pk = attrs.__pk__
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
                local model = Model.new(row, true)
                model:trigger('AfterFind')
                return model
            end, rows)
        end

        local function query()
            return Query():from(table_name)
        end

        Model.find = function(with)
            local q = query()
            getmetatable(q).__call = function(self)
                if self._state == 'select' then
                    return pop_models(self:exec())
                end
                return self:exec()
            end

            return q
        end

        -- if with then
        --     with = type(with) == 'string' and { with } or with
        --     for _, w in with do
        --         local rel = _relations[w]
        --         if not rel then
        --             error('relation '..w..' not found')
        --         end
        --         self:left_join(require(conf.model).table_name(), )
        --     end
        -- end

        Model.group = function(expr, cond, ...)
            local q = query():select(expr .. ' AS group__res')
            if cond then q:where(cond, ...) end

            local ok, res = q()
            if ok and #res > 0 then
                return res[1].group__res
            end
            return nil
        end

        Model.count = function(cond, ...)
            return Model.group('COUNT(*)', cond, ...)
        end

        Model.find_all = function(cond, ...)
            return Model.find():where(cond, ...)()
        end

        Model.find_one = function(cond, ...)
            local ok, records = Model.find():where(cond, ...):limit(1)()

            if ok then records = records[1] end
            return ok, records
        end

        Model.update_where = function(set, cond, ...)
            return query():update():where(cond, ...):set(set)()
        end

        Model.delete_where = function(cond, ...)
            return query():delete():where(cond, ...)()
        end

        Model.__index = function(self, key)
            if Model[key] then
                return Model[key]
            else
                return self.__attrs__[key]
            end
        end

        Model.__newindex = function(self, k, v)
            if attrs[k] ~= nil then
                self.__attrs__[k] = v
                self.__dirty_attrs__[k] = true
            end
        end

        function Model:set_dirty(attr)
            self.__dirty_attrs__[attr] = true
        end

        function Model:get_dirty_attrs()
            local count = 0
            local res = fun.kmap(function(k, v)
                count = count + 1
                return k, self.__attrs__[k] 
            end, self.__dirty_attrs__)
            return res, count
        end

        function Model:save()
            if self[pk] then -- update

                self:trigger('BeforeSave')

                local res = "no dirty attributes"
                local ok = false
                local dirty_attrs, count = self:get_dirty_attrs()
                if count > 0 then
                    ok, res = query():update():where(pk .. ' = ?d ', self[pk]):set(dirty_attrs)()

                    if ok then
                        self:set_none_dirty()
                    end
                end

                return ok, res
            else -- insert

                self:trigger('BeforeSave')

                local ok, res = query():insert():values(self.__attrs__)()

                if ok then 
                    self[pk] = res.insert_id
                    self:set_none_dirty()
                    self.__is_new__ = false
                    return ok, res
                else
                    return false, res
                end
            end
        end

        function Model:set_none_dirty()
            self.__dirty_attrs__ = {}
        end

        function Model:delete()
            assert(self[pk], 'primary key ['.. pk .. '] required')

            return query():delete():where(pk .. '= ?d', self[pk])()
        end

        function Model:load(data)
            if type(data) == 'table' then
                fun.kmap(function(k, v) 
                    self[k] = v 
                end, data)
            end
        end

        function Model:trigger(event, ...)
            local method = Model['on'..event]
            if type(method) == 'function' then
                return method(self, ...)
            end
        end

        function Model:is_new()
            return self.__is_new__
        end

        Model.new = function(data, not_dirty)
            local instance = { __attrs__ = {}, __dirty_attrs__ = {} , __is_new__ = true }
            setmetatable(instance, Model)

            instance:load(data)
            if not_dirty then
                instance:set_none_dirty()
                -- while loading from db, records are not new
                instance.__is_new__ = false
            end

            return instance
        end

        setmetatable(Model, nil)
    end

    return setmetatable({}, {
        __index = function(self, key)
            _init_model(self)
            return rawget(self, key)
        end
    })
end

return define_model

