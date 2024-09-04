#ifndef DB_SKIPLIST_H_
#define DB_SKIPLIST_H_
#include <assert.h>
#include <stdint.h>
#include <atomic>
#include <new>

#include "logger/log.h"
#include "logger/log_level.h"
#include "utils/random_util.h"

namespace corekv {
struct SkipListOption {
    static constexpr int32_t KMaxHeight = 20;
    static constexpr unsigned int KBranching = 4;
};

template <typename _KeyType, typename _KeyComparator, typename _Allocator>
class SkipList final {
    struct Node;

public:
private:
    Node *NewNode(const _KeyType &key, int32_t height);
    int32_t RandomHeight();
    int32_t GetMaxHeight() { return cur_height.load(std::memory_order_relaxed); }
    bool KeyIsAfterNode(const _KeyType &key, Node *n) {
        return (n != nullptr && comparator_.Compare(n->key, key));
    }

private:
    _KeyComparator comparator_;
    Node *head_ = nullptr;
    _Allocator arena_;
    std::atomic<int32_t> cur_height;
    RandomUtil rnd_;
};

template <typename _KeyType, typename _KeyComparator, typename _Allocator>
struct SkipList<_KeyType, _KeyComparator, _Allocator>::Node {
    explicit Node(const _KeyType &k) : key(k) {}
    const _KeyType key;

    Node *Next(int32_t n) { return next_[n].load(Std::memory_order_acquire); }
    void SetNext(int n, Node *x) {
        assert(n >= 0);
        next_[n].store(x, std::memory_order_relaxed);
    }
    Node *NoBarrier_Next(int n) { return next_[n].load(std::memory_order_relaxed); }
    void NoBarrier_SetNext(int n, Node *x) { next_[n].store(x, std::memory_order_relaxed); }

private:
    std::atomic<Node *> next_[1];
};

template <typename _KeyType, typename _Comparator, typename _Allocator>
typename SkipList<_KeyType, _Comparator, _Allocator>::Node *
SkipList<_KeyType, _Comparator, _Allocator>::NewNode(const _KeyType &key, int32_t height) {
    char *node_memory =
        (char *)arena_.Allocate(sizeof(Node) + sizeof(std::atomic<Node *>) * (height - 1));
    //定位new写法
    return new (node_memory) Node(key);
}

template <typename _KeyType, typename _Comparator, typename _Allocator>
int32_t SkipList<_KeyType, _Comparator, _Allocator>::RandomHeight() {
    int32_t height = 1;
    while (height < SkipListOption::KMaxHeight
           && (rnd_.GetRandomNum % SkipListOption::KBranching)) {
        height++;
    }
    return height;
}

} // namespace corekv
#endif