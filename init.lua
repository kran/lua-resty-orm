local Query = require'orm.query'
local Model = require'orm.model'
local assert = assert
local pcall = pcall
local ngx_ctx = ngx.ctx

local function open(conf)
    local driver = conf.driver
    assert(driver, "please specific db driver")

    local ok, db = pcall(require, 'orm.drivers.' .. driver)
    assert(ok, 'no driver for ' .. driver)

    local conn = db(conf)

    local create_query = function() 
        return Query.create(conn) 
    end

    local define_model = function(table_name) 
        return Model(conn, create_query, table_name) 
    end

    local transaction = function(fn)
        local thread = coroutine.create(fn)
        local key = "trans_" .. tostring(thread)
        local in_trans, db = conn.connect()
        if in_trans then 
            return error("transaction can't be nested") 
        end

        local ok, err = db:start_transaction()
        assert(ok, err)

        ngx_ctx[key] = db

        local status, res
        while coroutine.status(thread) ~= 'dead' do
            status, res = coroutine.resume(thread, db)
        end

        db:set_keepalive(10000, 50)
        ngx_ctx[key] = nil

        return status, res
    end

    return {
        db           = conn;
        transaction  = transaction;
        create_query = create_query;
        define_model = define_model;
        expr         = Query.expr(conn);
    }
end

return {
    open = open;
}
