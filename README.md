# cpredis
读写数据拷贝redis db, 不挑食高低版本，需支持SCAN命令，Codis拷贝Group的节点
# 为什么写这东西
redis-port, redis-shake 都没法拷贝我的线上数据（redis6+）到Codis（3.2.8）集群
所以想得数据量不特别大的情况可以通过读写数据来进行拷贝，这样就可以跳过RBD数据格式的不统一问题
经过我们的实用使用，效率其实还可以，需要的拿走，记得点赞哦！

# 缺点
需要停机，所以适合量不是特别大的数据迁移
数据类型只支持了，string,list,hash,zset 因为我们只用到了这些，有其他需求的可以完善一下
