本文件记录实现过程中遇到的问题

1. 有两笔数据，第一笔写入后，再写第二笔。此时 memtable 中包括所有的数据。需要在一次写入后，对 memtable 的内容清空
    修改方案：使用两个 active_memtable 和 immutable 。每次写入时，由 active_memtable 创建 immutable。写入完成后，将 immutable 清空

2. 增加 手动 flush 的接口
    a. 若 memtable 的大小 > config.memtable_size 时，自动 flush
    b. 人为调用 flush() 或 flush_aysnc() 时
    
3. 场景：
    sst_0.sst {key_0,  value_0}  ... {key_9,  value_9}   版本号 2 ~ 11
    sst_1.sst {key_10, value_10} ... {key_19, value_19}  版本号 12 ~ 21

    当前版本为 21

    当查找时 如查找key0时，
        bool may_contain_version(Version snap_ver) const {
            return snap_ver >= min_version && snap_ver <= max_version;
        }
    会导致将 sst_0.sst 直接跳过，导致查找失败


4. std::map<string, std::unique_ptr<MVCCNode>> MemTableImpl::data;
    key_0 ~ key_99 在 data 中的排序是混乱的？
    key_0
    key_1
    key_10
    key_11
    key_12
    ...
    key_19
    key_2
    key_21
    key_22
    ...

    会影响 范围查询 的正确性！
    默认比较器为字典序比较，修改为 自然序比较器

5. mvcc版本 还没有加入 cache 和 bloom_filter 

6. MVCCKVStore::~MVCCKVStore() 析构时，假如当前 memtable 中的容量较少，没有自动触发 flush。应该在析构时，主动触发 flush?

7. 
    每次 启动时，都会从现有的 sst 文件中读数据到 keys 中；如果现有的 sst 文件很多，会不会很浪费内存？
    每次 启动时，都会从现有的 wal 文件中读数据到 active_memtable->data 中，版本号是怎么获取的？保存到磁盘了吗？

8. 
    1. sstable  层有      std::vector<string> keys;
    2. memtable 层有      std::map<string, std::unique_ptr<MVCCNode>, NaturalLess> data;
    3. cache  缓存层      std::list<Node> lru_list; std::unordered_map<string, std::list<Node>::iterator> cache_map;
    他们都存在于内存中，为什么同样的数据要搞三份？明明都是内存数据，为什么有的叫 memtable 有的叫 table

9. MVCC 实现原理
    对于 版本号：当执行 put 时，版本号+1；当拍一个快照时，就保存当前最新的版本号，不+1。

   数据结构
    std::map<string, std::unique_ptr<MVCCNode>, NaturalLess> MVCCKVStore::memtable::active_memtable::data;
    Key(string) ->  MVCCNode*
    实例：
    user:1      ->  MVCCNode*
                    {
                        key      :  user:1
                        versions :  [1]  ->  VersionedValue {value_1, version_1, deleted_1}
                                    [3]  ->  VersionedValue {value_3, version_3, deleted_3}
                                    [4]  ->  VersionedValue {value_4, version_4, deleted_4}
                                    ... (当添加新版本时，就在此处添加新的数据)
                    }
    user:2      ->  MVCCNode*
                    {
                        key      :  user:2
                        versions :  [2]  ->  VersionedValue {value_2, version_2, deleted_2}
                                    [5]  ->  VersionedValue {value_5, version_5, deleted_5}
                                    ... (当添加新版本时，就在此处添加新的数据)
                    }

    当查找时，只查找小于指定版本号的数据。

    当前版本：在 sstable 落盘持久化时，只保存每个 key 的最新值，且不保存版本号信息。标准做法是什么？

10. MVCC 的垃圾回收功能



11. Transaction 的实现原理总结

12. MVCC 的事务隔离级别实现
    1. Read Uncommitted
    2. Read Committed
    3. Repeatable Read
    4. Serializable

13. gRPC 功能的实现分析
    底层 socket() listen() bind() accept() connect() epoll_wait() 等。gRPC 把这些封装起来了。

14. raft 的实现 以及 和 kvstore 项目的融合

15. 增加命令行功能(客户端)，类似 redis-cli，提供一些命令行工具来操作 MVCCKVStore，例如 put、get、delete、snapshot 等命令。

16. 请问为什么kvstore中 跳表使用了无锁，内存序；但是上层如 memtable sstable 又使用了 mutex

17. raft 的实现
    1. 状态机与持久化
    2. 领导者选举
    3. 日志复制
    4. 集群管理