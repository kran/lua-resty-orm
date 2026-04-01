local setmetatable = setmetatable
local type = type
local tostring = tostring
local string_format = string.format
local table_concat = table.concat
local table_insert = table.insert
local unpack = unpack or table.unpack 

local _M = {}
_M.__index = _M

-- 内置常量宏
_M.F = "F"
_M.I = "I"

-- 格式化与转义工具
function _M.qmark_formatter(idx) return "?" end
function _M.dollar_formatter(idx) return "$" .. tostring(idx) end

function _M.mysql_quoter(s) return "`" .. string.gsub(s, "`", "``") .. "`" end
function _M.ansi_quoter(s)  return '"' .. string.gsub(s, '"', '""') .. '"' end

-- expr 表达式包装器 (用于 insert/update 非绑定值)
local expr_meta = {}
function _M.expr(sql, ...)
    return setmetatable({sql = sql, args = {...}}, expr_meta)
end
local function is_expr(val)
    return type(val) == "table" and getmetatable(val) == expr_meta
end

-- 辅助函数：从 table 中提取列和值，并保证排序稳定性
local function extract_cols_vals(data)
    local cols = {}
    local vals = {}
    for k, _ in pairs(data) do
        table_insert(cols, k)
    end
    table.sort(cols)
    for _, k in ipairs(cols) do
        table_insert(vals, data[k])
    end
    return cols, vals
end

-- 核心 Builder
function _M.new(quoter, format)
    local obj = {
        main_nodes = {},
        var_nodes = {},
        quoter = quoter or _M.mysql_quoter,
        formatter = format or _M.qmark_formatter,
        err = nil,
        copy_id = 0
    }
    return setmetatable(obj, _M)
end

-- immutable 模式的核心 copy
function _M:copy()
    local clone = _M.new(self.quoter, self.formatter)
    clone.err = self.err
    clone.copy_id = self.copy_id + 1

    for _, v in ipairs(self.main_nodes) do
        table_insert(clone.main_nodes, v)
    end
    for k, v in pairs(self.var_nodes) do
        clone.var_nodes[k] = v
    end
    return clone
end

function _M:set_quoter(quoter)
    local clone = self:copy()
    clone.quoter = quoter
    return clone
end

function _M:set_formatter(formatter)
    local clone = self:copy()
    clone.formatter = formatter
    return clone
end

function _M:add(query, ...)
    local clone = self:copy()
    if clone.err then return clone end
    table_insert(clone.main_nodes, {raw_sql = query, args = {...}})
    return clone
end

function _M:add_if(cond, query, ...)
    if cond then return self:add(query, ...) end
    return self
end

function _M:var(key, query, ...)
    local clone = self:copy()
    if clone.err then return clone end
    clone.var_nodes[key] = {raw_sql = query, args = {...}}
    return clone
end

function _M:select(table_name, where, ...)
    return self:add("select ${" .. _M.F .. ":*} from " .. self.quoter(table_name) .. " where " .. where, ...)
end

function _M:insert(table_name, data)
    local cols, vals = extract_cols_vals(data)
    if #cols == 0 then
        local clone = self:copy()
        clone.err = "insert: empty data"
        return clone
    end

    local quoted_cols = {}
    local placeholders = {}
    local bind_args = {}
    local prefix = self.copy_id
    local result = self

    for i, c in ipairs(cols) do
        quoted_cols[i] = self.quoter(c)
        local v = vals[i]
        if is_expr(v) then
            local var_name = string_format("__expr_%d_%d", prefix, i)
            placeholders[i] = "${" .. var_name .. "}"
            result = result:var(var_name, v.sql, unpack(v.args))
        else
            table_insert(bind_args, v)
            placeholders[i] = "#{" .. tostring(#bind_args) .. "}"
        end
    end

    local query = string_format("insert ${%s} into %s (%s) values (%s)",
        _M.I, self.quoter(table_name), table_concat(quoted_cols, ", "), table_concat(placeholders, ", ")
    )
    return result:add(query, unpack(bind_args))
end

function _M:update(table_name, data, where, ...)
    local cols, vals = extract_cols_vals(data)
    if #cols == 0 then
        local clone = self:copy()
        clone.err = "update: empty data"
        return clone
    end

    local set_clauses = {}
    local bind_args = {}
    local prefix = self.copy_id
    local result = self

    for i, c in ipairs(cols) do
        local v = vals[i]
        if is_expr(v) then
            local var_name = string_format("__expr_%d_%d", prefix, i)
            set_clauses[i] = self.quoter(c) .. "=${" .. var_name .. "}"
            result = result:var(var_name, v.sql, unpack(v.args))
        else
            table_insert(bind_args, v)
            set_clauses[i] = self.quoter(c) .. "=#{" .. tostring(#bind_args) .. "}"
        end
    end

    local set_query = string_format("update %s set %s where", self.quoter(table_name), table_concat(set_clauses, ", "))
    return result:add(set_query, unpack(bind_args)):add(where, ...)
end

function _M:delete(table_name, where, ...)
    return self:add(string_format("delete from %s where", self.quoter(table_name))):add(where, ...)
end

function _M:batch(rows)
    if not rows or #rows == 0 then
        local clone = self:copy()
        clone.err = "batch: empty rows"
        return clone
    end
    local width = #(rows[1])
    if width == 0 then
        local clone = self:copy()
        clone.err = "batch: empty row"
        return clone
    end

    local builder = {}
    local args = {}
    local arg_idx = 1

    for i, row in ipairs(rows) do
        if #row ~= width then
            local clone = self:copy()
            clone.err = string_format("batch: row %d has length %d, expected %d", i, #row, width)
            return clone
        end
        if i > 1 then table_insert(builder, ", ") end

        table_insert(builder, "(")
        for j, val in ipairs(row) do
            if j > 1 then table_insert(builder, ", ") end
            table_insert(builder, "#{" .. tostring(arg_idx) .. "}")
            table_insert(args, val)
            arg_idx = arg_idx + 1
        end
        table_insert(builder, ")")
    end

    return self:add(table_concat(builder), unpack(args))
end

-- 宏解析与编译引擎
local function resolve_arg(args, content)
    local idx = tonumber(content)
    if idx then
        if idx < 1 or idx > #args then
            return nil, "index " .. idx .. " out of bounds"
        end
        return args[idx]
    end
    
    if #args == 0 then return nil, "no args" end
    local last_arg = args[#args]
    if type(last_arg) == "table" then
        local val = last_arg[content]
        if val ~= nil then return val end
        return nil, "named arg '" .. content .. "' not found"
    end
    return nil, "named args source must be a table"
end

function _M:build()
    if self.err then return "", {}, self.err end

    local sql_builder = {}
    local final_args = {}
    local arg_count = 0

    local function write_text(s) table_insert(sql_builder, s) end

    local parse
    parse = function(n)
        local str = n.raw_sql
        local i = 1
        
        while i <= #str do
            local start_idx = string.find(str, "{", i, true)
            if not start_idx then
                write_text(string.sub(str, i))
                break
            end

            if start_idx >= 3 and string.sub(str, start_idx-2, start_idx-2) == "\\" then
                local pre = string.sub(str, start_idx-1, start_idx-1)
                if pre == "#" or pre == "$" or pre == "@" or pre == "!" then
                    write_text(string.sub(str, i, start_idx-3))
                    write_text(string.sub(str, start_idx-1, start_idx))
                    i = start_idx + 1
                    goto continue
                end
            end

            if start_idx == 1 then
                write_text(string.sub(str, i, start_idx))
                i = start_idx + 1
                goto continue
            end

            local prefix = string.sub(str, start_idx-1, start_idx-1)
            if prefix ~= "#" and prefix ~= "$" and prefix ~= "@" and prefix ~= "!" then
                write_text(string.sub(str, i, start_idx))
                i = start_idx + 1
                goto continue
            end

            write_text(string.sub(str, i, start_idx-2))

            local end_idx = string.find(str, "}", start_idx, true)
            if not end_idx then
                return "sqlo: unclosed brace in " .. str
            end

            local content = string.sub(str, start_idx+1, end_idx-1)

            if prefix == "#" then
                local arg_val, err = resolve_arg(n.args, content)
                if err then return err end

                if type(arg_val) == "table" and not is_expr(arg_val) and #arg_val > 0 then
                    for j, v in ipairs(arg_val) do
                        if j > 1 then write_text(", ") end
                        arg_count = arg_count + 1
                        write_text(self.formatter(arg_count, v))
                        table_insert(final_args, v)
                    end
                else
                    arg_count = arg_count + 1
                    write_text(self.formatter(arg_count, arg_val))
                    table_insert(final_args, arg_val)
                end

            elseif prefix == "$" then
                local key, default_val = string.match(content, "^([^:]+):?(.*)$")
                key = (key or ""):match("^%s*(.-)%s*$")
                default_val = (default_val or ""):match("^%s*(.-)%s*$")

                if self.var_nodes[key] then
                    local err = parse(self.var_nodes[key])
                    if err then return err end
                elseif default_val ~= "" then
                    local err = parse({raw_sql = default_val, args = {}})
                    if err then return err end
                end

            elseif prefix == "@" then
                local val, err = resolve_arg(n.args, content)
                if err then return err end
                write_text(self.quoter(tostring(val)))

            elseif prefix == "!" then
                local val, err = resolve_arg(n.args, content)
                if err then return err end
                write_text(tostring(val))
            end

            i = end_idx + 1
            ::continue::
        end
        return nil
    end

    for i, node in ipairs(self.main_nodes) do
        if i > 1 then write_text("\n") end
        local err = parse(node)
        if err then return "", {}, err end
    end

    return table_concat(sql_builder), final_args, nil
end

return setmetatable({}, _M)
