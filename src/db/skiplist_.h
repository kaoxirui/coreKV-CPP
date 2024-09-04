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
    static constexpr int32_t kMaxHeight = 20;
    //有多少概率被选中, 空间和时间的折中
    static constexpr unsigned int kBranching = 4;
};

template <typename _KeyType, typename _KeyComparator, typename _Allocator>
//final关键字表示这个类不能被继承
class SkipList final {
    //前向声明，表示skiplist类中有一个node结构体
    struct Node;

public:
    SkipList(_KeyComparator comparator);

    //删除拷贝构造和赋值构造
    SkipList(const SkipList &) = delete;
    SkipList &operator=(const SkipList &) = delete;
    void Insert(const _KeyType &key) {
        // 该对象记录的是要节点要插入位置的前一个对象，本质是链表的插入
        Node *prev[SkipListOption::kMaxHeight] = {nullptr};
        //在key的构造过程中，有一个持续递增的序号，因此理论上不会有重复的key
        Node *node = FindGreaterOrEqual(key, prev);
        if (nullptr != node) {
            //相等则发出警告，不重复插入键
            if (Equal(key, node->key)) {
                LOG(WARN, "key:%s has existed", key);
                return;
            }
        }

        int32_t new_level = RandomHeight();
        int32_t cur_max_level = GetMaxHeight();
        if (new_level > cur_max_level) {
            //因为skiplist存在多层，而刚开始的时候只是分配kMaxHeight个空间，每一层的next并没有真正使用
            for (int32_t index = cur_max_level; index < new_level; ++index) {
                prev[index] = head_;
            }
            // 更新当前的最大值
            cur_height_.store(new_level, std::memory_order_relaxed);
        }
        //插入新节点
        Node *new_node = NewNode(key, new_level);
        for (int32_t index = 0; index < new_level; ++index) {
            new_node->NoBarrier_SetNext(index, prev[index]->NoBarrier_Next(index));
            prev[index]->NoBarrier_SetNext(index, new_node);
        }
    }
    bool Contains(const _KeyType &key) {
        Node *node = FindGreaterOrEqual(key, nullptr);
        //node不为空，即找到了一个大于或等于key的节点
        //不为空且找到了相等的键，函数返回true
        return nullptr != node && Equal(key, node->key);
    }
    bool Equal(const _KeyType &a, const _KeyType &b) { return comparator_.Compare(a, b) == 0; }

private:
    Node *NewNode(const _KeyType &key, int32_t height);
    int32_t RandomHeight();
    int32_t GetMaxHeight() { return cur_height_.load(std::memory_order_relaxed); }
    bool KeyIsAfterNode(const _KeyType &key, Node *n) {
        return (nullptr != n && comparator_.Compare(n->key, key) < 0);
    }
    //找到一个大于等于key的node
    Node *FindGreaterOrEqual(const _KeyType &key, Node **prev) {
        Node *cur = head_;
        //当前有效的最高层
        int32_t level = GetMaxHeight() - 1;
        Node *near_bigger_node = nullptr;
        while (true) {
            // 根据跳表原理，他是从最上层开始，向左或者向下遍历
            Node *next = cur->Next(level);
            // 说明key比next要大，直接往后next即可
            if (KeyIsAfterNode(key, next)) {
                cur = next;
            } else {
                if (prev != NULL) {
                    prev[level] = cur;
                }
                if (level == 0) {
                    return next;
                }
                //进入下一层
                level--;
            }
        }
    }
    // 找到小于key中最大的key
    Node *FindLessThan(const _KeyType &key) {
        //初始化当前节点和层级
        Node *cur = head_;
        int32_t level = GetMaxHeight() - 1;
        while (true) {
            Node *next = cur->Next(level);
            //cmp用来存储比较结果
            /*
            如果next指针为空，表示当前层级的末尾已经没有姐弟那，cmp被设置为1，表示next的键大于key（或不存在
            否则使用Compare比较next->key和key，返回的结果存储在cmp中
            */
            int32_t cmp = (next == nullptr) ? 1 : comparator_.Compare(next->key, key);
            //刚好next大于等于0
            //cmp>=0，next节点的键大于或等于key
            if (cmp >= 0) {
                // 因为高度是随机生成的，在这里只有level=0才能确定到底是哪个node
                if (level == 0) {
                    return cur;
                } else {
                    level--;
                }
            } else { //next节点的键小于key，更新cur为next，继续沿当前层级查找
                cur = next;
            }
        }
    }
    //查找最后一个节点的数据
    Node *FindLast() {
        Node *cur = head_;
        /*
          static表示kBaseLevel是一个静态成员变量，在类的所有实例之间共享
          constexpr 关键字表示 kBaseLevel 是一个常量表达式，意味着它的值在编译时就能确定。
          使用 constexpr 定义的变量不仅是常量，而且还具有编译时计算的特性，这使得它们非常高效。
        */
        static constexpr uint32_t kBaseLevel = 0;
        while (true) {
            Node *next = cur->Next(kBaseLevel);
            if (nullptr == next) {
                return cur;
            }
            cur = next;
        }
    }

private:
    _KeyComparator comparator_; //比较器
    _Allocator arena_;          //内存管理对象
    Node *head_ = nullptr;
    std::atomic<int32_t> cur_height_; //当前有效的层数
    RandomUtil rnd_;
};

// Implementation details follow
template <typename _KeyType, class _KeyComparator, typename _Allocator>
struct SkipList<_KeyType, _KeyComparator, _Allocator>::Node {
    explicit Node(const _KeyType &k) : key(k) {}

    //表示键值在节点创建后不能被修改
    const _KeyType key;

    // Accessors/mutators for links.  Wrapped in methods so we can
    // add the appropriate barriers as necessary.
    Node *Next(int32_t n) {
        // Use an 'acquire load' so that we observe a fully initialized
        // version of the returned Node.
        // std::memory_order_acquire 确保读取时能看到完全初始化的节点，即读取时需要获取所有的内存修改
        return next_[n].load(std::memory_order_acquire);
    }
    void SetNext(int n, Node *x) {
        assert(n >= 0);
        // Use a 'release store' so that anybody who reads through this
        // pointer observes a fully initialized version of the inserted node.
        //std::memory_order_release 确保在写入新节点的指针前，之前的写操作（包括节点的初始化）都已完成
        next_[n].store(x, std::memory_order_release);
    }

    // No-barrier variants that can be safely used in a few locations.
    //不带内存屏障版本，用于在某些场景下不需要严格内存顺序保证时使用，以或得更高的性能
    //使用 std::memory_order_relaxed 允许对内存操作的重排序，但不能保证顺序一致性，因此这些方法应谨慎使用。
    Node *NoBarrier_Next(int n) { return next_[n].load(std::memory_order_relaxed); }
    void NoBarrier_SetNext(int n, Node *x) { next_[n].store(x, std::memory_order_relaxed); }

private:
    // Array of length equal to the node height.  next_[0] is lowest level link.
    //通过声明一个长度为 1 的数组，结构体可以合法地定义，而这个数组在使用时往往会被“扩展”以容纳更多的元素。
    std::atomic<Node *> next_[1];
};

template <typename _KeyType, class _Comparator, typename _Allocator>
SkipList<_KeyType, _Comparator, _Allocator>::SkipList(_Comparator cmp)
    : comparator_(cmp), cur_height_(1), head_(NewNode(0, SkipListOption::kMaxHeight)) {
    for (int i = 0; i < SkipListOption::kMaxHeight; i++) {
        head_->SetNext(i, nullptr);
    }
}

template <typename _KeyType, typename _Comparator, typename _Allocator>
typename SkipList<_KeyType, _Comparator, _Allocator>::Node *
SkipList<_KeyType, _Comparator, _Allocator>::NewNode(const _KeyType &key, int32_t height) {
    char *node_memory =
        (char *)arena_.Allocate(sizeof(Node) + sizeof(std::atomic<Node *>) * (height - 1));
    //内存大小计算：Node结构体基础大小：包含键（key）和一个指向其他节点的 std::atomic<Node *> next_[1] 数组。
    //sizeof(std::atomic<Node *>) * (height - 1)：这是为了给跳表中节点的 next_ 数组分配额外的空间。跳表的节点高度可以大于1，因此需要为每一层分配指针数组。
    //定位new写法
    //在已经分配好的node_memory内存上调用Node的构造函数，初始化一个Node对象
    //与常规new不同，定位new不会重新分配内存，而是直接在已有内存上构造对象
    return new (node_memory) Node(key);
}

template <typename _KeyType, typename _Comparator, typename _Allocator>
int32_t SkipList<_KeyType, _Comparator, _Allocator>::RandomHeight() {
    int32_t height = 1;
    while (height < SkipListOption::kMaxHeight
           && ((rnd_.GetSimpleRandomNum() % SkipListOption::kBranching) == 0)) {
        height++;
    }
    return height;
}

} // namespace corekv
#endif
