#ifndef _DB_UTILS_ATOMIC_H_
#define _DB_UTILS_ATOMIC_H_

#include <assert.h>
#include <memory>
#include <mutex>
#include <queue>
#include <set>
#include <shared_mutex>
#include <unordered_map>

/// @class AtomicMap<K, V>
///
/// Atomically readable, atomically mutable unordered associative container.
/// Implemented as a std::unordered_map guarded by a pthread rwlock.
/// Supports CRUD operations only. Iterators are NOT supported.
template <typename K, typename V> class AtomicMap {
public:
  AtomicMap() {}
  // Returns the number of key-value pairs currently stored in the map.
  int Size() {
    std::shared_lock lock{mutex_};
    return map_.size();
  }

  // Returns true if the map contains a pair with key equal to 'key'.
  bool Contains(const K &key) {
    std::shared_lock lock{mutex_};
    return map_.contains(key);
  }

  // If the map contains a pair with key 'key', sets '*value' equal to the
  // associated value and returns true, else returns false.
  bool Lookup(const K &key, V *value) {
    std::shared_lock lock{mutex_};
    if (map_.contains(key)) {
      *value = map_[key];
      return true;
    } else {
      return false;
    }
  }

  // Atomically inserts the pair (key, value) into the map (clobbering any
  // previous pair with key equal to 'key'.
  void Insert(const K &key, const V &value) {
    std::scoped_lock lock{mutex_};
    map_[key] = value;
  }

  // Synonym for 'Insert(key, value)'.
  void Set(const K &key, const V &value) { Insert(key, value); }
  // Atomically erases any pair with key 'key' from the map.
  void Erase(const K &key) {
    std::scoped_lock lock{mutex_};
    map_.erase(key);
  }

private:
  std::unordered_map<K, V> map_;
  std::shared_mutex mutex_;
};

/// @class AtomicSet<K>
///
/// Atomically readable, atomically mutable container.
/// Implemented as a std::set guarded by a pthread rwlock.
/// Supports CRUD operations only. Iterators are NOT supported.
template <typename V> class AtomicSet {
public:
  AtomicSet() {}
  // Returns the number of key-value pairs currently stored in the map.
  int Size() {
    std::shared_lock lock{mutex_};
    return set_.size();
  }

  // Returns true if the set contains V value.
  bool Contains(const V &value) {
    std::shared_lock lock{mutex_};
    return set_.contains(value);
  }

  // Atomically inserts the value into the set.
  void Insert(const V &value) {
    std::scoped_lock lock{mutex_};
    set_.insert(value);
  }

  // Atomically erases the object value from the set.
  void Erase(const V &value) {
    std::scoped_lock lock{mutex_};
    set_.erase(value);
  }

  V GetFirst() {
    std::scoped_lock lock{mutex_};
    V first = *(set_.begin());
    return first;
  }

  // Returns a copy of the underlying set.
  std::set<V> GetSet() {
    std::shared_lock lock{mutex_};
    return {set_};
  }

private:
  std::set<V> set_;
  std::shared_mutex mutex_;
};

/// @class AtomicQueue<T>
///
/// Queue with atomic push and pop operations.
///
/// @TODO(alex): This should use lower-contention synchronization.
template <typename T> class AtomicQueue {
public:
  AtomicQueue() { mutex_ = std::make_unique<std::mutex>(); }
  // Returns the number of elements currently in the queue.
  int Size() {
    std::scoped_lock lock{*mutex_};
    int size = queue_.size();
    return size;
  }

  // Atomically pushes 'item' onto the queue.
  void Push(const T &item) {
    std::scoped_lock lock{*mutex_};
    queue_.push(item);
  }

  // If the queue is non-empty, (atomically) sets '*result' equal to the front
  // element, pops the front element from the queue, and returns true,
  // otherwise returns false.
  bool Pop(T *result) {
    std::scoped_lock lock{*mutex_};
    if (!queue_.empty()) {
      *result = queue_.front();
      queue_.pop();
      return true;
    } else {
      return false;
    }
  }

  // If mutex is immediately acquired, pushes and returns true, else immediately
  // returns false.
  bool PushNonBlocking(const T &item) {
    std::unique_lock lock{*mutex_, std::try_to_lock};
    if (lock) {
      queue_.push(item);
      return true;
    } else {
      return false;
    }
  }

  // If mutex is immediately acquired AND queue is nonempty, pops and returns
  // true, else returns false.
  bool PopNonBlocking(T *result) {
    std::unique_lock lock{*mutex_, std::try_to_lock};
    if (lock && !queue_.empty()) {
      *result = queue_.front();
      queue_.pop();
      return true;
    } else {
      return false;
    }
  }

private:
  std::queue<T> queue_;
  std::unique_ptr<std::mutex> mutex_;
};

template <typename T> class AtomicVector {
public:
  AtomicVector() {}
  // Returns the number of elements currently stored in the vector.
  int Size() {
    std::shared_lock lock{mutex_};
    int size = vec_.size();
    return size;
  }

  // Atomically accesses the value associated with the id.
  T &operator[](int id) {
    std::shared_lock lock{mutex_};
    T &value = vec_[id];
    return value;
  }

  // Atomically inserts the value into the vector.
  void Push(const T &value) {
    std::scoped_lock lock{mutex_};
    vec_.push_back(value);
  }

  // CMSC 624: TODO(students)
  // Feel free to add more methods as needed.

private:
  std::vector<T> vec_;
  std::shared_mutex mutex_;
};

#endif // _DB_UTILS_ATOMIC_H_
