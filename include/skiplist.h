// 跳表实现
#pragma once
#include <string>
#include <vector>
#include <atomic>
#include <random>

namespace kvstore {

template<typename Key, typename Value>
class SkipList {
private:
    struct Node {
        Key key;
        Value value;
        std::vector<std::atomic<Node*>> next;

        Node(const Key& k, const Value& v, int level) : key(k), value(v), next(level + 1, nullptr)
        {
            // 手动初始化每个 atomic ，避免拷贝构造
            for (int i = 0; i <= level; i++) {
                next[i].store(nullptr, std::memory_order_relaxed);
            }
        }

        Node(const Node&) = delete;
        Node& operator=(const Node&) = delete;
    };

    const int MAX_LEVEL = 16;
    const float P = 0.5;

    Node* head;
    std::atomic<int> cur_level;
    std::mt19937 rng;
    std::uniform_real_distribution<float> dist;

    int randomlevel() {
        int level = 0;
        while (dist(rng) < P && level < MAX_LEVEL) {
            level++;
        }
        return level;
    }
public:
    SkipList() : cur_level(0), rng(std::random_device{}()), dist(0, 1) {
        head = new Node(Key(), Value(), MAX_LEVEL);
    }

    ~SkipList() {
        Node* node = head;
        while (node) {
            Node* next = node->next[0].load(std::memory_order_relaxed);
            delete node;
            node = next;
        }
    }

    SkipList(const SkipList&) = delete;
    SkipList& operator=(const SkipList&) = delete;

    void put(const Key& key, const Value& value) {
        std::vector<Node*> update(MAX_LEVEL + 1, nullptr); // 记录每层的前驱节点
        Node* x = head;

        // 查找插入位置
        for (int i = cur_level.load(std::memory_order_relaxed); i >= 0; i--) {
            while (true) {
                Node* next = x->next[i].load(std::memory_order_acquire); // 获取当前层的下一个节点
                if (!next || key < next->key) // 找到插入位置
                    break;
                
                if (next->key == key) {
                    // 更新已有节点
                    next->value = value;
                    return;
                }

                x = next;
            }
            update[i] = x; // 记录前驱节点
        }

        // 生成随机层数
        int new_level = randomlevel();
        if (new_level > cur_level.load(std::memory_order_relaxed)) {
            for (int i = cur_level.load() + 1; i <= new_level; i++) {
                update[i] = head;
            }
            cur_level.store(new_level, std::memory_order_release);
        }

        // 插入新节点
        Node* new_node = new Node(key, value, new_level);
        for (int i = 0; i < new_level; i++) {
            new_node->next[i].store(update[i]->next[i].load(std::memory_order_relaxed), std::memory_order_relaxed); // 设置新节点的下一个节点
            update[i]->next[i].store(new_node, std::memory_order_release); // 更新前驱节点的下一个节点指向新节点
        }
    }

    bool get(const Key& key, Value& value) {
        Node* x = head;
        
        for (int i = cur_level.load(std::memory_order_relaxed); i >= 0; i--) {
            while (true) {
                Node* next = x->next[i].load(std::memory_order_acquire);
                if (!next || key < next->key)
                    break;
                if (key == next->key) {
                    value = next->value;
                    return true;
                }

                x = next;
            }
        }

        return false;
    }

    bool del(const Key& key) {
        std::vector<Node*> update(MAX_LEVEL + 1, nullptr);
        Node* x = head;

        for (int i = cur_level.load(std::memory_order_relaxed); i >= 0; i--) {
            while (true) {
                Node* next = x->next[i].load(std::memory_order_acquire);
                if (!next || key < next->key)
                    break;
                
                if (key == next->key) { // 找到节点，准备删除
                    for (int j = 0; j <= i; j++) { // 记录 0~i 层的前驱节点
                        update[j] = x;
                    }
                    goto FOUND;
                }

                x = next;
            }

            update[i] = x; // 记录i层的前驱节点
        }
        return false;

    FOUND:
        // 更新前驱节点的指针，跳过被删除节点
        Node* target = update[0]->next[0].load(std::memory_order_acquire); // 获取被删除节点
        for (int i = 0; i <= cur_level.load(std::memory_order_relaxed); i++) { // 更新前驱节点的指针
            if (update[i]->next[i].load(std::memory_order_acquire) != target)
                break;
            
            update[i]->next[i].store(target->next[i].load(std::memory_order_relaxed), std::memory_order_release);
        }
        delete target;

        // 降低 level
        while (cur_level.load(std::memory_order_relaxed) > 0 && !head->next[cur_level.load(std::memory_order_relaxed)].load(std::memory_order_relaxed)) { // 如果最高层没有节点了，降低 level
            cur_level--;
        }

        return true;
    }

    size_t size() {
        size_t count = 0;

        Node* x = head->next[0].load(std::memory_order_relaxed);
        while (x) {
            count++;
            x = x->next[0].load(std::memory_order_relaxed);
        }
        return count;
    }

    // 迭代器
    class Iterator {
    private:
        Node* current;
    public:
        Iterator(Node* node) : current(node) {}
        bool valid() const {return current != nullptr;}
        void next() {
            if (current)
                current = current->next[0].load(std::memory_order_relaxed);
        }
        Key key() {return current->key;}
        Value value() {return current->value;}
    };

    Iterator begin() {
        return Iterator(head->next[0].load(std::memory_order_relaxed));
    }
};

}