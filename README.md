
# lua-resty-orm
----
Simple ~~ORM~~ for [openresty](http://openresty.org) 

2018/11/27 - ORM is the hell, let SQL rocks!

# Status
----
This library is **NOT** production ready.

# Usage
----

## example:
```lua
local orm = require'orm'.open{
  driver = 'mysql', -- or 'postgresql'
  port = 3306,
  host = '127.0.0.1',
  user = 'root',
  password = '123456',
  database = 'test',
  charset = 'utf8mb4',
  expires = 100,  -- cache expires time
  debug = true -- log sql with ngx.log 
}

-- Mapper is the main feature of this library: 
-- MySQL: select `name`, `age` from `users` where id > 10 limit 20
local res, err = orm.mapper:prepare("select [name], [age] from [users] where id > ?d", 10):append("limit ?d", 20):query()
-- you can chain call any method, and handle errors at the end
-- when error occured, any chained calls will be ignored
if err then 
	ngx.say(res.err)
else
	ngx.say(cjson.encode(res))
end
```
## placeholders

placeholders can be used is sql, and will be converted to the right value for every supported driver or error on failed.

```
- `?j`  JSON  `cjson.encode` and ngx.quote_sql_str used
- `?t`  table  {1,2,'a'} => 1,2,'a'
- `?b`  bool(0, 1), only false or nil will be converted to 0 for mysql, TRUE | FALSE in postgresql
- `?e`  expression: MAX(id) | MIN(id) ...
- `?d`  digit number, convert by tonumber
- `?n`  NULL, false and nil wil be converted to 'NULL', orther 'NOT NULL'
- `?s`  string, escaped by ngx.quote\_sql\_str
- `?`   any, convert by guessing the value type
```

## orm.transaction(fn)

run `fn` in transaction: 

```lua
orm.transaction(function(conn)
    -- any mapper query
end)
```

*Notice: transaction can't be nested now. maybe won't be fixed.*

## mapper_instance, err = mapper:prepare(sql, ...)

prepare sql statement and convert any parameters. 

- mapper_instance lua table support most of the features
- err  true/false

## res, err = mapper_instance:append(sql, ...)

like `prepare`, just append another more sql statement

## sql = mapper_instance:tosql()

return  generated sql string

## res, err = mapper_instance:query()

send sql to database and return the resultset or return error

- res error or resultset
- err  true/false
## res, err = mapper_instance:first()

get first record from resultset

## res, err = mapper_instance:scalar()

get first row and first column of the resultset, handy on getting aggrating result, eg: `count(*)`, `max(id)`...


