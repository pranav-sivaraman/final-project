#include <mutex>
#include <queue>

template <class T> class atomic_queue {
private:
  std::queue<T> queue_;
  std::mutex mutex_;

public:
  bool pop(T &f) {
    std::scoped_lock lock{mutex_};
    if (std::empty(queue_))
      return false;

    f = std::move(queue_.front());
    queue_.pop();

    return true;
  }

  bool push(T &&f) {
    std::scoped_lock lock{mutex_};
    queue_.emplace(f);
    return true;
  }

  bool try_push(T &&f) {
    std::unique_lock lock{mutex_, std::try_to_lock};

    if (!lock)
      return false;

    queue_.emplace(f);
    return true;
  }

  bool try_pop(T &f) {
    std::unique_lock lock{mutex_, std::try_to_lock};

    if (!lock || std::empty(queue_)) {
      return false;
    }

    f = std::move(queue_.front());
    queue_.pop();

    return true;
  }
};
