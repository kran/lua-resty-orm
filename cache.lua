local lrucache = require'resty.lrucache'

local c, err = lrucache.new(200)
if not c then
    return error("failed to create the cache: " .. (err or "unknown"))
end

return c
