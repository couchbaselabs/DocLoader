# DocLoader

Run:
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass="couchbase.test.sdk.Loader" -Dexec.args="-n <ip> -user <username> -pwd <password> -b <bucket-name> -p 11210 -create_s 0 -create_e 10000000 -cr 100 -ops 100000 -docSize 1024 -scope <> -collection <>"
```

```
Required options: n, user, pwd, b, p
usage: Supported Options
 -b,--bucket <arg>              Bucket
 -collection <arg>              Collection
 -cr,--create <arg>             Creates%
 -create_e,--create_e <arg>     Creates Start
 -create_s,--create_s <arg>     Creates Start
 -delete_e,--delete_e <arg>     Delete End
 -delete_s,--delete_s <arg>     Delete Start
 -deleted,--deleted <arg>       To verify deleted docs
 -dl,--delete <arg>             Deletes%
 -docSize,--docSize <arg>       Size of the doc
 -durability <arg>              Durability Level
 -ex,--expiry <arg>             Expiry%
 -expiry_e,--expiry_e <arg>     Expiry End
 -expiry_s,--expiry_s <arg>     Expiry Start
 -gtm,--gtm <arg>               Go for max doc ops
 -keySize,--keySize <arg>       Size of the key
 -keyType,--keyType <arg>       Random/Sequential/Reverse
 -loadType,--loadType <arg>     Hot/Cold
 -n,--node <arg>                IP Address
 -ops,--ops <arg>               Ops/Sec
 -p,--port <arg>                Memcached Port
 -pwd,--rest_password <arg>     Password
 -rd,--read <arg>               Reads%
 -read_e,--read_e <arg>         Read End
 -read_s,--read_s <arg>         Read Start
 -replace_e,--replace_w <arg>   Replace End
 -replace_s,--replace_s <arg>   Replace Start
 -scope <arg>                   Scope
 -touch_e,--touch_e <arg>       Touch End
 -touch_s,--touch_s <arg>       Touch Start
 -transaction_patterns <arg>    Transaction load pattern
 -up,--update <arg>             Updates%
 -update_e,--update_e <arg>     Update End
 -update_s,--update_s <arg>     Update Start
 -user,--rest_username <arg>    Username
 -validate,--validate <arg>     Validate Data during Reads
 -valueType,--valueType <arg>
 -w,--workers <arg>             Workers
 ```
