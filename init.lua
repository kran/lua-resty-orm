local Query = require'orm.query'
local Model = require'orm.model'
local assert = assert
local pcall = pcall

local function open(conf)
    local driver = conf.driver
    assert(driver, "please specific db driver")

    local ok, db = pcall(require, 'orm.drivers.' .. driver)
    assert(ok, 'no driver for ' .. driver)

    local conn = db(conf)

    local create_query = function() 
        return Query.create(conn) 
    end

    local define_model = function(attrs) 
        return Model(conn, create_query, attrs) 
    end

    return {
        db = conn;
        create_query = create_query;
        define_model = define_model;
        expr        = Query.expr;
    }
end

return {
    open = open;
}
