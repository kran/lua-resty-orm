# lua-resty-orm
----
Simple ORM for [openresty](http://openresty.org) 

#Status
----
This library is not production ready.

#Usage
----

##connect to database:
```
local orm = require'orm'.open{
  driver = 'mysql', -- or 'postgresql'
  port = 3306,
  host = '127.0.0.1',
  user = 'root',
  password = '123456',
  database = 'test',
  charset = 'utf8mb4',
  expires = 100,  -- cache expires time period
  debug = true -- log sql with ngx.log 
}
```
##orm.expr(expression)

Create sql expression which will never be escaped.

##orm.create_query():

This is the query builder, can now build select, update, insert, delete sql.
```
local sql = orm.create_query():from('table_name'):where('[id] = ?d', 99):one()
-- SELSECT * FROM table_name WHERE `id` = 99 LIMIT 1
```
#####*from(table, alias):*
```
query:from('table') -- SELECT * FROM table
query:from('[table]') -- SELECT * FROM `table`
query:from(another_query:from('user', 'u')) -- SELECT * FROM (SELECT * FROM user) AS u
```
#####*select(fields):*
```
query:select('t1, t2, [t3]') -- SELECT t1, t2, `t3` ...
```

#####*where(cond, ...), and\_where(cond, ...), or_where(cond, ...):*
```
query:where('id = ?d or [key] like ?s', '10', '"lua-%-orm"') -- WHERE id = 10 or `key` like '\"lua-%-orm\"'
query:where('id in (?t)', 1) -- WHERE id in (1)
query:where('id in (?t)', {1, 2, 'a'}) --WHERE id in (1,2,'a')
-- ?t can be ? if don't know type of param

```
- `?t`  table  {1,2,'a'} => 1,2,'a'
- `?b`  bool(0, 1), only false or nil will be converted to 0
- `?e`  expression: MAX(id) | MIN(id) ...
- `?d`  digit number, convert by tonumber
- `?n`  NULL, false and nil wil be converted to 'NULL', orther 'NOT NULL'
- `?s`  string, escape by ngx.quote\_sql\_str
- `?`  any, convert by guessing the value type

THESE modifiers can be used in where/having/join methods

#####*having(cond, ...), and_having(cond, ...), or_having(cond, ...):*

just like `where`

#####*join(tbl, cond, ...), left\_join, right\_join, inner_join:*

JOIN `tbl` ON `cond` , `...` params will be used in `cond`

#####*group_by(...), order_by(...):*

Accept multiple `group by` | `order_by` expressions

#####*limit(limit_num):*

limit for select sql

#####*offset(offset_num):*

offset for select sql

#####*as(alias):*

Set alias for `select` type sql.

#####*set(key, value), set(hashmap):*

Used in the `UPDATE tbl SET ...` sql.

#####*values(hashmap):*

Used in the `INSERT INTO tbl (...) VALUES (...)`

#####*delete(tbl), update(tbl), insert(tbl):*

Set the query type, `tbl` param is optional, which can also be setted by `from` method.

#####*for_update():*

`SELECT * FROM tbl WHERE id=1 FOR UPDATE`

#####*build():*

Return the sql string

#####*exec():*

Send query to database , returning (status, results)


##orm.define_model(table_name):

`define_model` accept table name as paramater and cache table fields in lrucache.

_WARNING:_ the table must have an auto increment column as its primary key

METHODS:

- *Model.new([attributes])*  create new instance  
- *Model.query()*  same as orm.create\_query():from(Model.table\_name())
- *Model.find()*  same as query(), but return Model instance
- *Model.find\_one(cond, ...)*  find one record by condition
- *Model.find\_all(cond, ...)*  find all records by condition
- *Model.update\_where(attributes, cond, ...)*  update records filter by condition  
- *Model.delete\_where(cond, ...)*  delete records filter by condition  

- *model:save()*  save the record, if pk is not nil then `update()` will be called, otherwise `insert()` will be called   
- *model:load(attributes)*  load attributes to instance
- *model:set\_dirty(attribute)*  make attribute dirty ( will be updated to database ) 
- *model:is\_new()*  return if this instance is new or load from database

This method define a model:

```
local User = orm.define_model('tbl_user')

-- build query from User
User.query() -- eq to orm.create_query():from('tbl_user')

-- fetch 

-- SELECT * FROM tbl_user WHERE id > 1 LIMIT 10
local ok, users = User.find():where('id > 1'):limit(10)() -- notice the ()

-- SELECT * FROM tbl_user WHERE id > 10
local ok, users = User.find_all('id > ?d', 10)
if ok then
  for _, u in ipairs(users) do
    print(user.name)
  end
end

-- SELECT * FROM tbl_user WHERE id = 10 LIMIT 1
local ok, user = User.find_one('id = 10')
if ok then
  user.name = 'new name'
  local ok, res = user:save()  -- update user
end

-- UPDATE tbl_user SET name='name updated' WHERE id > 10
local attrs = { name = 'name updated' }
User.update_where(attrs, 'id > ?', 10) 

-- DELETE FROM tbl_user WHERE id = 10
User.delete_where('id = ?', 10) --delete all by condition
user:delete()  -- delete user instance

-- create new 
local attrs = { name = 'new one' }
local user = User.new(attrs)
user:save()

local user = User.new()
user:load(attrs) -- same as User.new(attrs)

```

#TODO
----

* [model] event (after\_find, before\_save & etc)
* [model] attributes validation

