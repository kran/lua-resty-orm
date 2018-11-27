local Mapper = require'orm.mapper'
local assert = assert
local pcall = pcall

local function open(conf)
    local driver = conf.driver
    assert(driver, "please specific db driver")

    local ok, db = pcall(require, 'orm.drivers.' .. driver)
    assert(ok, 'no driver for ' .. driver)

    local conn = db(conf)

    local transaction = function(fn)
        local in_trans, db = conn.connect()
        if in_trans then 
            return error("transaction can't be nested") 
        end

        local ok, err = db:start_transaction()
        assert(ok, err)

        local thread = coroutine.create(fn)
        local key = "trans_" .. tostring(thread)

        ngx.ctx[key] = db

        local status, res
        while coroutine.status(thread) ~= 'dead' do
            status, res = coroutine.resume(thread, db)
        end

        db:set_keepalive(10000, 50)
        ngx.ctx[key] = nil

        return status, res
    end

    return {
        db           = conn;
        transaction  = transaction;
        mapper       = Mapper(conn);
    }
end

return {
    open = open;
}
