local Query = require'orm.query'
local Model = require'orm.model'

local function open(conf)
    local driver = conf.driver
    assert(driver, "please specific db driver")

    local ok, db = pcall(require, 'orm.drivers.' .. driver)
    assert(ok, 'no driver for ' .. driver)

    local conn = db(conf)
    local create_query = function(func) 
        return Query.create(conn, func) 
    end

    return {
        create_query = create_query;
        define_model = function(attrs) return Model(create_query, attrs) end;
        expr        = Query.expr;
    }
end

return {
    open = open;
}
