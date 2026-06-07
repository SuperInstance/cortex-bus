//! # cortex-bus
//!
//! Event bus with CQRS pattern — Command/Query/Event separation with typed channels,
//! event store, and projections.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// ── Core Types ──────────────────────────────────────────────────────────────

/// Trait for typed events. Any `'static` type can be an event.
pub trait Event: Any + Send + Sync + std::fmt::Debug {
    fn as_any(&self) -> &dyn Any;
    fn event_type_name(&self) -> &'static str;
}

impl Event for String {
    fn as_any(&self) -> &dyn Any { self }
    fn event_type_name(&self) -> &'static str { "String" }
}

impl Event for i64 {
    fn as_any(&self) -> &dyn Any { self }
    fn event_type_name(&self) -> &'static str { "i64" }
}

impl Event for u32 {
    fn as_any(&self) -> &dyn Any { self }
    fn event_type_name(&self) -> &'static str { "u32" }
}

/// A boxed event wrapper.
#[derive(Debug)]
pub struct BoxedEvent {
    inner: Arc<dyn Event>,
}

impl BoxedEvent {
    pub fn new<E: Event + 'static>(event: E) -> Self {
        Self { inner: Arc::new(event) }
    }
    pub fn inner(&self) -> &dyn Event { &*self.inner }
}

/// Trait for commands.
pub trait Command: Any + Send + Sync + std::fmt::Debug {
    fn as_any(&self) -> &dyn Any;
    fn command_type_name(&self) -> &'static str;
}

/// Trait for queries.
pub trait Query: Any + Send + Sync + std::fmt::Debug {
    fn as_any(&self) -> &dyn Any;
    fn query_type_name(&self) -> &'static str;
}

/// Result from a query handler.
#[derive(Debug, Clone)]
pub struct QueryResult {
    data: Arc<dyn Any + Send + Sync>,
}

impl QueryResult {
    pub fn new<T: Any + Send + Sync + 'static>(value: T) -> Self {
        Self { data: Arc::new(value) }
    }
    pub fn downcast<T: Clone + 'static>(&self) -> Option<T> {
        self.data.downcast_ref::<T>().cloned()
    }
}

// ── EventBus ────────────────────────────────────────────────────────────────

type EventHandler = Box<dyn Fn(&BoxedEvent) + Send + Sync>;

/// Priority for event subscribers. Higher = called first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Priority(pub i32);

impl Priority {
    pub const HIGH: Priority = Priority(100);
    pub const NORMAL: Priority = Priority(50);
    pub const LOW: Priority = Priority(0);
}

/// Typed event bus with priority-ordered handlers.
pub struct EventBus {
    handlers: Mutex<Vec<(Priority, TypeId, EventHandler)>>,
}

impl EventBus {
    pub fn new() -> Self {
        Self { handlers: Mutex::new(Vec::new()) }
    }

    /// Subscribe to events of type `E` with a given priority.
    pub fn subscribe<E: Event + 'static>(&self, priority: Priority, handler: impl Fn(&E) + Send + Sync + 'static) {
        let type_id = TypeId::of::<E>();
        let wrapped: EventHandler = Box::new(move |ev: &BoxedEvent| {
            if let Some(concrete) = ev.inner().as_any().downcast_ref::<E>() {
                handler(concrete);
            }
        });
        let mut handlers = self.handlers.lock().unwrap();
        handlers.push((priority, type_id, wrapped));
        handlers.sort_by_key(|b| std::cmp::Reverse(b.0)); // higher priority first
    }

    /// Publish an event to all matching subscribers.
    pub fn publish<E: Event + 'static>(&self, event: E) {
        let boxed = BoxedEvent::new(event);
        let type_id = TypeId::of::<E>();
        let handlers = self.handlers.lock().unwrap();
        for (_, tid, handler) in handlers.iter() {
            if *tid == type_id {
                handler(&boxed);
            }
        }
    }

    /// Publish any boxed event by TypeId lookup.
    pub fn publish_boxed(&self, event: BoxedEvent) {
        let handlers = self.handlers.lock().unwrap();
        // Call all handlers — they filter internally
        for (_, _, handler) in handlers.iter() {
            handler(&event);
        }
    }

    pub fn handler_count(&self) -> usize {
        self.handlers.lock().unwrap().len()
    }
}

impl Default for EventBus {
    fn default() -> Self { Self::new() }
}

// ── CommandBus ──────────────────────────────────────────────────────────────

type CommandHandler = Box<dyn Fn(&dyn Any) -> bool + Send + Sync>;

/// Command bus with typed handler registration and dispatch.
pub struct CommandBus {
    handlers: Mutex<HashMap<TypeId, CommandHandler>>,
}

impl CommandBus {
    pub fn new() -> Self {
        Self { handlers: Mutex::new(HashMap::new()) }
    }

    /// Register a handler for command type `C`.
    pub fn register<C: Command + 'static>(&self, handler: impl Fn(&C) -> bool + Send + Sync + 'static) {
        let type_id = TypeId::of::<C>();
        let wrapped: CommandHandler = Box::new(move |cmd: &dyn Any| {
            if let Some(concrete) = cmd.downcast_ref::<C>() {
                handler(concrete)
            } else {
                false
            }
        });
        self.handlers.lock().unwrap().insert(type_id, wrapped);
    }

    /// Dispatch a command. Returns true if a handler processed it.
    pub fn dispatch<C: Command + 'static>(&self, command: &C) -> bool {
        let type_id = TypeId::of::<C>();
        let handlers = self.handlers.lock().unwrap();
        if let Some(handler) = handlers.get(&type_id) {
            handler(command.as_any())
        } else {
            false
        }
    }

    pub fn handler_count(&self) -> usize {
        self.handlers.lock().unwrap().len()
    }
}

impl Default for CommandBus {
    fn default() -> Self { Self::new() }
}

// ── QueryBus ────────────────────────────────────────────────────────────────

type QueryHandler = Box<dyn Fn(&dyn Any) -> Option<QueryResult> + Send + Sync>;

/// Query bus with typed handler registration returning QueryResult.
pub struct QueryBus {
    handlers: Mutex<HashMap<TypeId, QueryHandler>>,
}

impl QueryBus {
    pub fn new() -> Self {
        Self { handlers: Mutex::new(HashMap::new()) }
    }

    /// Register a handler for query type `Q`.
    pub fn register<Q: Query + 'static>(&self, handler: impl Fn(&Q) -> QueryResult + Send + Sync + 'static) {
        let type_id = TypeId::of::<Q>();
        let wrapped: QueryHandler = Box::new(move |q: &dyn Any| {
            q.downcast_ref::<Q>().map(&handler)
        });
        self.handlers.lock().unwrap().insert(type_id, wrapped);
    }

    /// Execute a query. Returns None if no handler registered.
    pub fn execute<Q: Query + 'static>(&self, query: &Q) -> Option<QueryResult> {
        let type_id = TypeId::of::<Q>();
        let handlers = self.handlers.lock().unwrap();
        handlers.get(&type_id).and_then(|h| h(query.as_any()))
    }

    pub fn handler_count(&self) -> usize {
        self.handlers.lock().unwrap().len()
    }
}

impl Default for QueryBus {
    fn default() -> Self { Self::new() }
}

// ── EventStore ──────────────────────────────────────────────────────────────

/// Append-only event log with replay capability.
pub struct EventStore {
    events: Mutex<Vec<BoxedEvent>>,
    sequence: Mutex<u64>,
}

impl EventStore {
    pub fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            sequence: Mutex::new(0),
        }
    }

    /// Append an event, returning its sequence number.
    pub fn append<E: Event + 'static>(&self, event: E) -> u64 {
        let mut seq = self.sequence.lock().unwrap();
        *seq += 1;
        let n = *seq;
        self.events.lock().unwrap().push(BoxedEvent::new(event));
        n
    }

    /// Total number of stored events.
    pub fn len(&self) -> usize {
        self.events.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.lock().unwrap().is_empty()
    }

    /// Replay events from `start` index (inclusive) through the given callback.
    pub fn replay_from(&self, start: usize, f: impl Fn(usize, &BoxedEvent)) {
        let events = self.events.lock().unwrap();
        for (i, ev) in events.iter().enumerate().skip(start) {
            f(i, ev);
        }
    }

    /// Replay all events.
    pub fn replay_all(&self, f: impl Fn(usize, &BoxedEvent)) {
        self.replay_from(0, f);
    }

    /// Clear the store (for testing).
    pub fn clear(&self) {
        self.events.lock().unwrap().clear();
        *self.sequence.lock().unwrap() = 0;
    }
}

impl Default for EventStore {
    fn default() -> Self { Self::new() }
}

// ── Projection ──────────────────────────────────────────────────────────────

/// A materialized view built from events via a projection function.
pub struct Projection<T: Default + Clone> {
    state: Mutex<T>,
}

impl<T: Default + Clone> Projection<T> {
    pub fn new() -> Self {
        Self { state: Mutex::new(T::default()) }
    }

    pub fn from(initial: T) -> Self {
        Self { state: Mutex::new(initial) }
    }

    /// Apply a mutating function to the projection state.
    pub fn apply(&self, f: impl FnOnce(&mut T)) {
        let mut state = self.state.lock().unwrap();
        f(&mut state);
    }

    /// Get a snapshot of the current state.
    pub fn snapshot(&self) -> T {
        self.state.lock().unwrap().clone()
    }
}

impl<T: Default + Clone> Default for Projection<T> {
    fn default() -> Self { Self::new() }
}

// ── Concrete command/query types for testing ────────────────────────────────

#[derive(Debug)]
pub struct IncrementCommand { pub amount: i64 }
impl Command for IncrementCommand {
    fn as_any(&self) -> &dyn Any { self }
    fn command_type_name(&self) -> &'static str { "IncrementCommand" }
}

#[derive(Debug)]
pub struct ResetCommand;
impl Command for ResetCommand {
    fn as_any(&self) -> &dyn Any { self }
    fn command_type_name(&self) -> &'static str { "ResetCommand" }
}

#[derive(Debug)]
pub struct GetValueQuery;
impl Query for GetValueQuery {
    fn as_any(&self) -> &dyn Any { self }
    fn query_type_name(&self) -> &'static str { "GetValueQuery" }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};

    // Helper: a simple counter projection state
    #[derive(Default, Clone)]
    struct CounterState { value: i64 }

    // ── EventBus Tests ──

    #[test]
    fn event_bus_subscribe_and_publish() {
        let bus = EventBus::new();
        let counter = Arc::new(AtomicI64::new(0));
        let c = counter.clone();
        bus.subscribe::<String>(Priority::NORMAL, move |ev| {
            c.fetch_add(ev.len() as i64, Ordering::SeqCst);
        });
        bus.publish(String::from("hello"));
        assert_eq!(counter.load(Ordering::SeqCst), 5);
    }

    #[test]
    fn event_bus_priority_ordering() {
        let bus = EventBus::new();
        let order: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
        let o1 = order.clone();
        bus.subscribe::<String>(Priority::LOW, move |_| {
            o1.lock().unwrap().push(1);
        });
        let o2 = order.clone();
        bus.subscribe::<String>(Priority::HIGH, move |_| {
            o2.lock().unwrap().push(2);
        });
        let o3 = order.clone();
        bus.subscribe::<String>(Priority::NORMAL, move |_| {
            o3.lock().unwrap().push(3);
        });
        bus.publish(String::from("test"));
        let o = order.lock().unwrap().clone();
        assert_eq!(o, vec![2, 3, 1]); // HIGH, NORMAL, LOW
    }

    #[test]
    fn event_bus_multiple_events() {
        let bus = EventBus::new();
        let counter = Arc::new(AtomicI64::new(0));
        let c = counter.clone();
        bus.subscribe::<i64>(Priority::NORMAL, move |ev| {
            c.fetch_add(*ev, Ordering::SeqCst);
        });
        bus.publish(10i64);
        bus.publish(20i64);
        bus.publish(30i64);
        assert_eq!(counter.load(Ordering::SeqCst), 60);
    }

    #[test]
    fn event_bus_handler_count() {
        let bus = EventBus::new();
        assert_eq!(bus.handler_count(), 0);
        bus.subscribe::<String>(Priority::NORMAL, |_| {});
        bus.subscribe::<i64>(Priority::NORMAL, |_| {});
        assert_eq!(bus.handler_count(), 2);
    }

    // ── CommandBus Tests ──

    #[test]
    fn command_bus_dispatch() {
        let bus = CommandBus::new();
        let counter = Arc::new(AtomicI64::new(0));
        let c = counter.clone();
        bus.register::<IncrementCommand>(move |cmd| {
            c.fetch_add(cmd.amount, Ordering::SeqCst);
            true
        });
        assert!(bus.dispatch(&IncrementCommand { amount: 5 }));
        assert_eq!(counter.load(Ordering::SeqCst), 5);
    }

    #[test]
    fn command_bus_no_handler_returns_false() {
        let bus = CommandBus::new();
        assert!(!bus.dispatch(&ResetCommand));
    }

    #[test]
    fn command_bus_handler_count() {
        let bus = CommandBus::new();
        bus.register::<IncrementCommand>(|_| true);
        bus.register::<ResetCommand>(|_| true);
        assert_eq!(bus.handler_count(), 2);
    }

    // ── QueryBus Tests ──

    #[test]
    fn query_bus_execute() {
        let bus = QueryBus::new();
        bus.register::<GetValueQuery>(|_| {
            QueryResult::new(42i64)
        });
        let result = bus.execute(&GetValueQuery);
        assert!(result.is_some());
        assert_eq!(result.unwrap().downcast::<i64>(), Some(42));
    }

    #[test]
    fn query_bus_no_handler_returns_none() {
        let bus = QueryBus::new();
        assert!(bus.execute(&GetValueQuery).is_none());
    }

    // ── EventStore Tests ──

    #[test]
    fn event_store_append_and_len() {
        let store = EventStore::new();
        assert!(store.is_empty());
        store.append(String::from("e1"));
        store.append(String::from("e2"));
        assert_eq!(store.len(), 2);
        assert!(!store.is_empty());
    }

    #[test]
    fn event_store_replay_all() {
        let store = EventStore::new();
        store.append(String::from("a"));
        store.append(String::from("bb"));
        let total_len = Arc::new(AtomicI64::new(0));
        let t = total_len.clone();
        store.replay_all(|_, ev| {
            if let Some(s) = ev.inner().as_any().downcast_ref::<String>() {
                t.fetch_add(s.len() as i64, Ordering::SeqCst);
            }
        });
        assert_eq!(total_len.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn event_store_replay_from_offset() {
        let store = EventStore::new();
        store.append(String::from("first"));
        store.append(String::from("second"));
        store.append(String::from("third"));
        let count = Arc::new(AtomicI64::new(0));
        let c = count.clone();
        store.replay_from(1, |_, _| {
            c.fetch_add(1, Ordering::SeqCst);
        });
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn event_store_clear() {
        let store = EventStore::new();
        store.append(String::from("x"));
        store.clear();
        assert!(store.is_empty());
    }

    // ── Projection Tests ──

    #[test]
    fn projection_apply_and_snapshot() {
        let proj: Projection<CounterState> = Projection::new();
        proj.apply(|s| s.value += 10);
        proj.apply(|s| s.value += 5);
        assert_eq!(proj.snapshot().value, 15);
    }

    #[test]
    fn projection_from_initial() {
        let proj = Projection::from(CounterState { value: 100 });
        assert_eq!(proj.snapshot().value, 100);
    }

    // ── Integration: Full CQRS flow ──

    #[test]
    fn full_cqrs_integration() {
        let event_bus = EventBus::new();
        let command_bus = CommandBus::new();
        let query_bus = QueryBus::new();
        let store = EventStore::new();
        let projection: Projection<CounterState> = Projection::new();

        // Wire: command handler publishes event
        let eb = Arc::new(event_bus);
        let eb_ref = eb.clone();
        let store_ref = Arc::new(store);
        let store_clone = store_ref.clone();
        let proj = Arc::new(projection);
        let proj_clone = proj.clone();

        // Event subscriber updates projection
        eb.subscribe::<String>(Priority::NORMAL, move |ev| {
            proj_clone.apply(|s| s.value += ev.len() as i64);
        });

        // Command handler
        command_bus.register::<IncrementCommand>(move |cmd| {
            let event_str = format!("inc:{}", cmd.amount);
            eb_ref.publish(event_str);
            store_clone.append(String::from("incremented"));
            true
        });

        // Query handler
        let proj_q = proj.clone();
        query_bus.register::<GetValueQuery>(move |_| {
            QueryResult::new(proj_q.snapshot().value)
        });

        command_bus.dispatch(&IncrementCommand { amount: 7 });
        command_bus.dispatch(&IncrementCommand { amount: 3 });

        // "inc:7" has 5 chars, "inc:3" has 5 chars → total 10
        let result = query_bus.execute(&GetValueQuery);
        assert_eq!(result.unwrap().downcast::<i64>(), Some(10));
        assert_eq!(store_ref.len(), 2);
    }
}
