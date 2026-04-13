use std::collections::VecDeque;

/// Bounded FIFO queue used by the asynchronous ingestion pipeline.
#[derive(Debug)]
pub struct IngestionQueue<T> {
    capacity: usize,
    items: VecDeque<T>,
}

impl<T> IngestionQueue<T> {
    /// Creates an empty queue with a fixed maximum item count.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "ingestion queue capacity must be positive");
        Self {
            capacity,
            items: VecDeque::with_capacity(capacity),
        }
    }

    /// Attempts to append an item to the tail of the queue.
    pub fn enqueue(&mut self, item: T) -> Result<(), T> {
        if self.is_full() {
            return Err(item);
        }
        self.items.push_back(item);
        Ok(())
    }

    /// Removes and returns the oldest enqueued item.
    pub fn dequeue(&mut self) -> Option<T> {
        self.items.pop_front()
    }

    /// Returns the number of queued items.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns true when the queue contains no items.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Returns true when the queue has reached its configured capacity.
    pub fn is_full(&self) -> bool {
        self.len() >= self.capacity
    }

    /// Returns the configured queue capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::IngestionQueue;
    use proptest::prelude::*;

    #[test]
    fn queue_preserves_enqueue_order() {
        let mut queue = IngestionQueue::new(4);
        queue.enqueue("first").unwrap();
        queue.enqueue("second").unwrap();
        queue.enqueue("third").unwrap();

        assert_eq!(queue.dequeue(), Some("first"));
        assert_eq!(queue.dequeue(), Some("second"));
        assert_eq!(queue.dequeue(), Some("third"));
    }

    #[test]
    fn queue_reports_empty_when_drained() {
        let mut queue = IngestionQueue::<u64>::new(2);

        assert!(queue.is_empty());
        assert_eq!(queue.dequeue(), None);

        queue.enqueue(10).unwrap();
        assert!(!queue.is_empty());

        assert_eq!(queue.dequeue(), Some(10));
        assert_eq!(queue.dequeue(), None);
        assert!(queue.is_empty());
    }

    #[test]
    fn queue_rejects_items_beyond_capacity() {
        let mut queue = IngestionQueue::new(2);
        queue.enqueue(1).unwrap();
        queue.enqueue(2).unwrap();

        let rejected = queue.enqueue(3).unwrap_err();

        assert_eq!(rejected, 3);
        assert!(queue.is_full());
        assert_eq!(queue.capacity(), 2);
        assert_eq!(queue.len(), 2);
        assert_eq!(queue.dequeue(), Some(1));
        assert_eq!(queue.dequeue(), Some(2));
    }

    proptest! {
        #[test]
        fn queue_fifo_property(values in proptest::collection::vec(0u16..1000, 0..32)) {
            let capacity = values.len().max(1);
            let mut queue = IngestionQueue::new(capacity);
            for value in &values {
                queue.enqueue(*value).unwrap();
            }

            let drained: Vec<_> = std::iter::from_fn(|| queue.dequeue()).collect();
            prop_assert_eq!(drained, values);
        }
    }
}
