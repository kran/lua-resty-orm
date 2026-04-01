local Q = require("./builder")
local resty_fmt = require("./resty_formatter")

-- 一个极简的测试辅助函数
local function run_test(name, fn)
    io.write(string.format("Running %-35s ... ", name))
    local status, err = pcall(fn)
    if status then
        print("✅ PASS")
    else
        print("❌ FAIL")
        print("   " .. tostring(err))
    end
end

-- 深度比较 table 的辅助函数
local function deep_compare(t1, t2)
    if type(t1) ~= "table" or type(t2) ~= "table" then return t1 == t2 end
    if #t1 ~= #t2 then return false end
    for i = 1, #t1 do
        if t1[i] ~= t2[i] then return false end
    end
    return true
end

print("==========================================")
print("   Starting Q Builder Unit Tests")
print("==========================================\n")

local db = Q.new()

-- ---------------------------------------------------------
-- 1. 测试基本的不可变特性 (Immutability)
-- ---------------------------------------------------------
run_test("Immutability (不可变链式调用)", function()
    local base = db:select("users", "status = 1")
    
    local q1 = base:add_if(true, "AND age > #{1}", 18)
    local q2 = base:add_if(true, "AND score > #{1}", 90)

    local sql_base = base:build()
    local sql1 = q1:build()
    local sql2 = q2:build()

    -- 核心断言：base 不应该被后续的 add 修改
    assert(not string.find(sql_base, "age"), "Base instance was mutated!")
    assert(not string.find(sql_base, "score"), "Base instance was mutated!")
    
    assert(string.find(sql1, "age > ?"), "q1 should contain age")
    assert(not string.find(sql1, "score"), "q1 should not contain score")
    
    assert(string.find(sql2, "score > ?"), "q2 should contain score")
    assert(not string.find(sql2, "age"), "q2 should not contain age")
end)

-- ---------------------------------------------------------
-- 2. 测试命名参数与切片展开 (Named Args & Slice Expansion)
-- ---------------------------------------------------------
run_test("Named Args & Slice Expansion", function()
    local q = db:add("SELECT * FROM users WHERE role = #{role} AND id IN (#{ids})", {
        role = "admin",
        ids = {101, 102, 103} -- 切片应该被展开为 ?, ?, ?
    })

    local sql, args, err = q:build()
    assert(err == nil, err)
    assert(sql == "SELECT * FROM users WHERE role = ? AND id IN (?, ?, ?)", "SQL mismatch: " .. sql)
    
    assert(deep_compare(args, {"admin", 101, 102, 103}), "Args mismatch")
end)

-- ---------------------------------------------------------
-- 3. 测试复杂的 Update 与原生表达式 (Expr)
-- ---------------------------------------------------------
run_test("Update & Raw Expression (Expr)", function()
    -- 注意：Lua 表遍历无序，但我们在 extract_cols_vals 中做了按 key 字母排序
    -- 所以生成的 SQL 字段顺序一定是 age, login_count, updated_at
    local q = db:update("users", {
        updated_at = Q.expr("NOW()"),
        age = 20,
        login_count = Q.expr("login_count + #{1}", 1)
    }, "id = #{1}", 99)

    local sql, args, err = q:build()
    assert(err == nil, err)
    
    local expected_sql = "update `users` set `age`=?, `login_count`=login_count + ?, `updated_at`=NOW() where\nid = ?"
    assert(sql == expected_sql, "SQL mismatch:\nExp: " .. expected_sql .. "\nGot: " .. sql)
    assert(deep_compare(args, {20, 1, 99}), "Args mismatch")
end)

-- ---------------------------------------------------------
-- 4. 测试批量插入 (Batch Insert)
-- ---------------------------------------------------------
run_test("Batch Insert Builder", function()
    local q = db:add("INSERT INTO `logs` (`level`, `msg`) VALUES\n"):batch({
        {"info", "system start"},
        {"warn", "high memory"},
        {"error", "crash"}
    })

    local sql, args, err = q:build()
    assert(err == nil, err)
    
    local expected_sql = "INSERT INTO `logs` (`level`, `msg`) VALUES\n\n(?, ?), (?, ?), (?, ?)"
    assert(sql == expected_sql, "SQL mismatch: " .. sql)
    assert(deep_compare(args, {"info", "system start", "warn", "high memory", "error", "crash"}), "Args mismatch")
end)

-- ---------------------------------------------------------
-- 5. 测试转义与 Postgres 方言切换 (Dialect Switch)
-- ---------------------------------------------------------
run_test("Postgres Dialect ($1, $2)", function()
    local pg_db = Q.new(Q.ansi_quoter, resty_fmt.format)
    
    local q = pg_db:insert("user_orders", {
        user_id = Q.expr("a+#{1}", "2"),
        amount = 99.9
    })

    local sql, args, err = q:build()
    assert(err == nil, err)
    
    -- 字典按字母排序：amount, user_id
    local expected_sql = 'INSERT  INTO "user_orders" ("amount", "user_id") VALUES ($1, $2)'
    assert(sql == expected_sql, "SQL mismatch: " .. sql)
    assert(deep_compare(args, {99.9, 10}), "Args mismatch")
end)

print("\n==========================================")
print("   All tests finished!")
print("==========================================")
