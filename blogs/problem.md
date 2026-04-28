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

18. gRPC 功能
    scan 范围查询功能还没通
    stat 统计信息功能还没通
    将服务正式发布

19. MVCC 的垃圾回收功能
    1. 版本号的回收：当某个版本号不再被任何快照引用时，可以安全地回收该版本号对应的数据。
    2. 数据的回收：当某个版本号对应的数据被标记为删除，并且不再被任何快照引用时，可以安全地回收该数据。
    3. 实现细节：可以使用一个后台线程定期扫描所有的快照和事务，找出不再被引用的版本号和数据，并进行回收。

20. MVCC 的事务隔离级别实现
    1. Read Uncommitted：允许读取未提交的数据。实现时，读操作可以直接读取 memtable 中的最新版本数据，无需考虑事务状态。
    2. Read Committed：只允许读取已提交的数据。实现时，读操作需要检查版本号对应的事务是否已提交，如果未提交则跳过该版本数据。
    3. Repeatable Read：保证在同一事务中多次读取同一数据时，结果一致。实现时，读操作需要记录第一次读取的数据版本号，并在后续读取时确保版本号不变。
    4. Serializable：保证事务之间完全隔离，仿佛它们是串行执行的一样。实现时，可以使用锁机制或者时间戳机制来确保事务之间的隔离。

21. gRPC 功能的实现分析
    1. 底层通信：gRPC 使用 HTTP/2 协议进行通信，支持双向流、流控和多路复用等特性。它还使用 Protocol Buffers 作为默认的序列化机制，提供高效的数据传输。
    2. 服务定义：通过 .proto 文件定义服务接口和消息类型，gRPC 会自动生成相应的代码，使得开发者可以专注于业务逻辑的实现。
    3. 服务器端实现：在服务器端实现服务接口，处理客户端请求，并返回响应。gRPC 提供了多种编程语言的支持，使得跨语言开发变得容易。
    4. 客户端实现：在客户端创建 gRPC stub，通过调用 stub 的方法来发送请求并接收响应。gRPC 支持同步和异步两种调用方式，满足不同的应用需求。

22. raft 的实现
    1. 状态机与持久化：每个节点维护一个状态机，记录当前的任期号、投票信息和日志条目等。通过持久化机制确保节点在崩溃后能够恢复到之前的状态。
    2. 领导者选举：当集群启动或领导者失效时，节点会发起选举，投票选出新的领导者。领导者负责处理客户端请求和日志复制。
    3. 日志复制：领导者将客户端请求转换为日志条目，并将其复制到其他节点。当大多数节点确认日志条目后，领导者将其应用到状态机中，并向客户端返回结果。
    4. 集群管理：raft 提供了动态添加和移除节点的功能，使得集群能够灵活地扩展和缩减。同时，raft 还处理网络分区和节点故障等情况，确保系统的高可用性。

23. 增加命令行功能(客户端)
    1. 实现一个简单的命令行界面，接受用户输入的命令并解析。
    2. 支持基本的操作命令，如 put、get、delete、snapshot 等。
    3. 将解析后的命令转换为 gRPC 请求，并发送到服务器端。
    4. 接收服务器端的响应，并将结果显示给用户。

24. 请问为什么kvstore中 跳表使用了无锁，内存序；但是上层如 memtable sstable 又使用了 mutex
    1. 跳表作为一个高性能的数据结构，设计时考虑了并发访问的场景，因此采用了无锁和内存序的方式来实现，以提高性能和减少锁竞争。
    2. 上层的 memtable 和 sstable 主要负责数据的持久化和管理，涉及到更多的状态变化和复杂的操作，因此使用 mutex 来保证线程安全，避免数据不一致的问题。
    3. 这种设计权衡了性能和安全性，在跳表中追求高性能，而在 memtable 和 sstable 中确保数据的一致性和正确性。

25. 测试用例
    1. 基础功能测试：测试基本的 put、get、delete 操作，确保数据能够正确存储和检索。
    2. 事务测试：测试事务的提交和回滚功能，确保事务的原子性和一致性。
    3. 并发测试：模拟多个客户端同时访问服务器，测试系统在高并发情况下的性能和稳定性。
    4. 边界条件测试：测试一些特殊情况，如空键、超长键、超大值等，确保系统能够正确处理这些边界条件。
    5. 错误处理测试：测试系统在遇到错误时的表现，如网络故障、磁盘故障等，确保系统能够正确处理错误并保持稳定运行。
    测 QPS、延迟等性能指标，评估系统的性能表现，并进行优化。

26. 什么时候用 shared_ptr ; 什么时候用 unique_ptr ; 
    
    为什么 mutex 前要加 mutable

27. 项目精细化
    kv存储的范围查询、还有批量存储，批量获取、各层级的迭代器
    还有 cache 和 bloomfilter, 把它应用到项目中
    使用命令行来测试和开发完善 各种隔离级别的事务功能
    
28. 
隔离级别            PUT/DELETE                      GET                                             提交时验证                  解决/存在的问题
READ_UNCOMMITTED   不加锁，直接写入 write_set_      无快照，读存储中的最新值（可能是未提交的脏数据）    无验证                      ❌ 脏读、不可重复读、幻读
READ_COMMITTED     不加锁，直接写入 write_set_      无快照，每次只读最新已提交的数据                   无验证                      ❌ 不可重复读、幻读  ✅ 解决脏读
REPEATABLE_READ    不加锁，直接写入 write_set_      快照读 + 共享锁，读事务开始时的版本                写集验证 + 丢失更新检测       ❌ 幻读            ✅ 解决脏读、不可重复读
SNAPSHOT_ISOLATION 不加锁，直接写入 write_set_      快照读，不加锁，读事务开始时的版本                  读写集双重验证              ❌ 写偏斜           ✅ 解决脏读、不可重复读、幻读(部分)
SERIALIZABLE       加 X 锁，写入 write_set_         快照读 + 共享锁，读事务开始时的版本                无需验证 (锁保证了)                             ✅ 解决所有问题

29. 锁的获取场景
  请求的锁 ↓ / 已有的锁 →     SHARED (S)     EXCLUSIVE (X)
│  SHARED (S)                   ✅ 兼容          ❌ 冲突
│  EXCLUSIVE (X)                ❌ 冲突          ❌ 冲突

对于同一个 Key
当只有一个事务，该事务能成功从 S 锁 升级到 X 锁
事务1和事务2 都持有S锁，事务1 不能升级到X锁