# stupidql

`stupidql` is an immutable, chainable SQL query stupidql designed for Lua. It is focused on SQL building only, with no dependency on any database drivers.

## Quick Example

```lua
local stupidql = require "stupidql"

-- Create a stupidql with default MySQL formatter
local b = stupidql.new()

-- Build a query (immutable, safe to share)
b = b:add("select ${F:*} from users u")
     :add("join orders o on o.user_id = u.id where 1 = 1")
     :add_if(status ~= "", "and u.status = #{1}", status)
     :add_if(min_age > 0, "and u.age >= #{1}", min_age)
     :add("order by u.id desc")

-- Get the final SQL and arguments
local sql, args, err = b:build()
-- sql: "select * from users u\njoin orders o on o.user_id = u.id where 1 = 1\nand u.status = $1\norder by u.id desc"
-- args: {status, min_age}

-- Reuse the same stupidql for a different purpose — original is untouched
local count_b = b:var("F", "count(1)"):add("limit 1")
local count_sql, count_args, _ = count_b:build()
-- count_sql: "select count(1) from users u\n...\nlimit 1"
```

---

## Design Philosophy

1. **Immutability**: Every method that mutates state (`add`, `var`, `add_if`) returns a new copy. The original instance is never modified, making it safe for concurrent use and reuse across requests.
2. **SQL complexity stays in SQL**: No semantic stupidql chains — you write SQL, `stupidql` handles parameter binding, identifier quoting, and placeholder formatting.
3. **Deterministic output**: Table keys are sorted lexicographically, ensuring consistent SQL for query plan cache reuse.
4. **Pure stupidql**: Focused solely on SQL building, execution is handled separately.

---

## Installation

```bash
# Copy the file to your project directory
cp stupidql.lua your_project/libs/
```

---

## Initialization

```lua
local stupidql = require "stupidql"

-- Default: MySQL backtick quoting + ? placeholders
local b = stupidql.new()

-- PostgreSQL: ANSI double-quote quoting + $1 placeholders
local pg_b = stupidql.new(
    stupidql.ansi_quoter,      -- "identifier"
    stupidql.dollar_formatter   -- $1, $2, ...
)

-- Custom formatter and quoter
local custom_b = stupidql.new(
    function(s) return "[" .. s .. "]" end,  -- [identifier]
    function(idx) return ":" .. idx end      -- :1, :2, ... (Oracle style)
)
```

Built-in quoters: `stupidql.mysql_quoter`, `stupidql.ansi_quoter`.
Built-in formatters: `stupidql.qmark_formatter`, `stupidql.dollar_formatter`.
see `resty_formatter.lua` for lua-resty-mysql(no placeholders)

---

## Core stupidql

### Macro Syntax

| Syntax | Behavior |
|--------|----------|
| `#{1}` | Bind parameter by positional index (1-based) |
| `#{name}` | Bind parameter from named arg (last arg as table) |
| `${key:default}` | Variable expansion — uses `var(key, ...)` if set, otherwise `default` |
| `@{1}` / `@{name}` | Identifier quoting (table/column names); `user` → `` `user` `` or `"user"` |
| `!{1}` / `!{name}` | Raw interpolation (injection risk); outputs value directly |
| `\#{...}` | Escape — outputs `#{...}` literally (also works for `\${`, `\@{`, `\!{`) |

Arrays/tables passed to `#{}` are auto-expanded: `#{1}` with `{1,2,3}` → `$1, $2, $3`.

`@{}` and `!{}` return an error if the resolved value is `nil` or not found.

### Methods

- `new(quoter, format)` — create a new stupidql
- `add(query, ...)` — append a SQL fragment
- `add_if(cond, query, ...)` — conditional append
- `var(name, query, ...)` — register a named variable, expanded when `${name}` appears

```lua
-- Positional parameters
local b = stupidql.new()
b = b:add("select * from users where status = #{1}", "active")

-- Named parameters (last arg as table)
b = b:add("where id = #{id} and name = #{name}", {id = 42, name = "alice"})
```

### Constants

- `stupidql.F` — Field selector constant (used with `select`)
- `stupidql.I` — INSERT constant (used with `insert`)

### var — Declarative Variable Slots

`var(name, query, ...)` registers a named variable. It doesn't produce any SQL output by itself — it only takes effect when `${name}` or `${name:default}` appears in a query. This makes it **position-independent** and **purely declarative**.

```lua
local b = stupidql.new()

-- Basic: Var overrides inline default
local base = b:add("select ${F:*} from users order by id")
local count_b = base:var("F", "id, name")
local sql = count_b:build()  -- select id, name from users order by id
local sql = base:build()   -- select * from users order by id

-- Position doesn't matter — these are equivalent
b:add("select ${F:*} from t"):var("F", "id")
b:var("F", "id"):add("select ${F:*} from t")

-- Multiple slots with defaults
b:add("select ${F:*} from users ${where:} ${order:order by id} ${limit:}")
 :var("where", "where status = #{1}", "active")
 :var("limit", "limit #{1}", 20)
-- select * from users where status = $1 order by id limit $2

-- Same Var referenced multiple times
b:add("select ${F:*} from t where ${F:*} is not null")
 :var("F", "name")
-- select name from t where name is not null

-- Immutable: var returns a new copy, base is unchanged
local base  = b:add("select ${F:*} from users")
local count = base:var("F", "count(1)")  -- for counting
local data  = base:add("limit 10")            -- for data query
-- base, count, data are three independent queries

-- var with parameter binding — each var has its own args namespace
b:add("select ${F:*} from users ${filter}")
 :var("filter", "where age > #{1} and city = #{2}", 18, "NYC")
-- select * from users where age > $1 and city = $2
```

**Key properties:**
- `${key:default}` — if var not set, uses text after `:` as fallback (plain text only, no nested `{}`)
- `${key}` — if var not set and no default, outputs empty string
- Each var has its own args namespace — `#{1}` in different vars never conflict

---

## DML Helpers

Generate `select`, `insert`, `update`, `delete`. Keys are sorted.

```lua
local b = stupidql.new()

-- Select: shorthand, implicitly uses ${F:*}
b:select("users", "age > #{1} order by age", 18)
-- select * from `users` where age > ? order by age

-- Insert
b:insert("users", {name = "alice", age = 25})
-- insert  into `users` (`age`, `name`) values (?, ?)

-- Update
b:update("users", {name = "bob"}, "id = #{1}", 42)
-- update `users` set `name`=? where id = ?

-- Delete
b:delete("users", "id = #{1}", 42)
-- delete from `users` where id = ?
```

### Expr — Raw SQL Values

Use `stupidql.expr()` to embed raw SQL expressions in `insert`/`update` values. Expr values are expanded through the build engine via `var`, so `#{}` macros work naturally inside them:

```lua
b:update("stats", {
    views = stupidql.expr("views + 1"),
    score = stupidql.expr("score + #{1}", 10),
}, "id = #{1}", 1)
-- update `stats` set `score`=score + ?, `views`=views + 1 where id = ?
```

### batch — Multi-row Value Groups

`batch` generates `(?,?,...), (?,?,...)` and appends it to the current query:

```lua
local rows = {
    {1, "alice"},
    {2, "bob"},
    {3, "carol"},
}

-- Bulk INSERT
b:add("insert into users (id, name) values"):batch(rows)
-- insert into users (id, name) values ($1,$2), ($3,$4), ($5,$6)

-- Multi-column IN
b:add("select * from users where (id, name) in ("):batch(rows):add(")")
```

---

## Terminal Methods

- `build()` — returns `(sql, args, err)` or `("", {}, err)` on error

```lua
local sql, args, err = b:build()
if err then
    print("Error:", err)
else
    print("SQL:", sql)
    print("Args:", unpack(args))
end
```

---

## License

MIT
