use std::array;
use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::mem::{self, MaybeUninit};
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct Producer<'r, T, const N: usize> {
  inner: &'r SpscRing<T, N>,
  // enforce !Sync
  _unsync: PhantomData<Cell<()>>,
}

impl<T, const N: usize> Producer<'_, T, N> {
  pub fn enqueue(&mut self, elem: T) -> Result<(), T> {
    self.inner.enqueue(elem)
  }

  pub fn is_empty(&self) -> bool {
    self.inner.is_empty()
  }

  pub fn is_full(&self) -> bool {
    self.inner.is_full()
  }
}

pub struct Consumer<'r, T, const N: usize> {
  inner: &'r SpscRing<T, N>,
  // enforce !Sync
  _unsync: PhantomData<Cell<()>>,
}

impl<T, const N: usize> Consumer<'_, T, N> {
  pub fn dequeue(&mut self) -> Result<T, Error> {
    self.inner.dequeue()
  }

  pub fn is_empty(&self) -> bool {
    self.inner.is_empty()
  }

  pub fn is_full(&self) -> bool {
    self.inner.is_full()
  }
}

// producer and consumer must be send
const _: () = {
  #[allow(unused)]
  fn check<T: Send, const N: usize>() {
    fn assert<X: Send>() {}
    assert::<Producer<T, N>>();
    assert::<Consumer<T, N>>();
  }
};

// producer and consumer must NOT be sync
const _: () = {
  #[allow(unused)]
  trait AmbiguousIfSync<A> {
    fn check() {}
  }
  impl<X: ?Sized> AmbiguousIfSync<()> for X {}
  impl<X: ?Sized + Sync> AmbiguousIfSync<u8> for X {}
  #[allow(unused)]
  fn check<T: Send, const N: usize>() {
    <Producer<T, N> as AmbiguousIfSync<_>>::check();
    <Consumer<T, N> as AmbiguousIfSync<_>>::check();
  }
};

// producer and consumer must NOT be clone
const _: () = {
  #[allow(unused)]
  trait AmbiguousIfClone<A> {
    fn check() {}
  }
  impl<X: ?Sized> AmbiguousIfClone<()> for X {}
  // clone implies sized
  impl<X: /*?Sized +*/ Clone> AmbiguousIfClone<u8> for X {}
  #[allow(unused)]
  fn check<T: Send, const N: usize>() {
    <Producer<T, N> as AmbiguousIfClone<_>>::check();
    <Consumer<T, N> as AmbiguousIfClone<_>>::check();
  }
};

#[repr(align(64))]
struct Slot<T>(UnsafeCell<MaybeUninit<T>>);

impl<T> Deref for Slot<T> {
  type Target = UnsafeCell<MaybeUninit<T>>;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
  #[error("spsc ring queue is empty")]
  QueueIsEmpty,
}

pub struct SpscRing<T, const N: usize> {
  head: AtomicUsize,
  tail: AtomicUsize,
  ring: [Slot<T>; N],
}

impl<T, const N: usize> SpscRing<T, N> {
  const ASSERT_VALID_CAPACITY: () = assert!(N >= 2, "spsc ring must have size >=2");

  #[must_use]
  pub fn new() -> Self {
    let _ = Self::ASSERT_VALID_CAPACITY;
    Self {
      head: AtomicUsize::new(0),
      tail: AtomicUsize::new(0),
      ring: array::from_fn(|_| Slot(UnsafeCell::new(MaybeUninit::uninit()))),
    }
  }

  pub fn split(&mut self) -> (Producer<'_, T, N>, Consumer<'_, T, N>) {
    let producer = Producer {
      inner: self,
      _unsync: PhantomData,
    };
    let consumer = Consumer {
      inner: self,
      _unsync: PhantomData,
    };
    (producer, consumer)
  }

  // head is owned by the producer
  fn enqueue(&self, elem: T) -> Result<(), T> {
    // producer is the only writer of head
    // - this thread reads it own writes...
    let head = self.head.load(Ordering::Relaxed);
    // synchronize-with consumer
    let tail = self.tail.load(Ordering::Acquire);
    let next_head = (head + 1) % N;
    if next_head != tail {
      // we stomp whatever used to be in that slot with a new entry...
      unsafe {
        *self.ring[head].get() = MaybeUninit::new(elem);
      }
      self.head.store(next_head, Ordering::Release);
      Ok(())
    } else {
      Err(elem)
    }
  }

  // tail is owned by the consumer
  // - wanted signature to be -> Result<T, ()> but clippy got mad
  fn dequeue(&self) -> Result<T, Error> {
    // synchronize-with producer
    let head = self.head.load(Ordering::Acquire);
    // same thing as producer, consumer is just reading its writes on tail...
    let tail = self.tail.load(Ordering::Relaxed);
    if head == tail {
      return Err(Error::QueueIsEmpty);
    }
    let elem = unsafe { (*self.ring[tail].get()).assume_init_read() };
    let next_tail = (tail + 1) % N;
    self.tail.store(next_tail, Ordering::Release);
    Ok(elem)
  }

  // unsynchronized, not for internal use
  fn is_empty(&self) -> bool {
    let head = self.head.load(Ordering::Relaxed);
    let tail = self.tail.load(Ordering::Relaxed);
    head == tail
  }

  // unsychronized, not for internal use
  fn is_full(&self) -> bool {
    let tail = self.tail.load(Ordering::Relaxed);
    let head = self.head.load(Ordering::Relaxed);
    let next_head = (head + 1) % N;
    next_head == tail
  }
}

impl<T, const N: usize> Default for SpscRing<T, N> {
  fn default() -> Self {
    Self::new()
  }
}

impl<T, const N: usize> Drop for SpscRing<T, N> {
  fn drop(&mut self) {
    if mem::needs_drop::<T>() {
      todo!()
    }
  }
}

unsafe impl<T, const N: usize> Send for SpscRing<T, N> where T: Send {}
unsafe impl<T, const N: usize> Sync for SpscRing<T, N> where T: Send {}

// spscring must be send and sync
const _: () = {
  // we only ever send T across threads with enqueue/dequeue, dont ever hand out &T
  // so we dont need T: Sync...
  #[allow(unused)]
  fn check<T: Send, const N: usize>() {
    fn assert<X: Send + Sync>() {}
    assert::<SpscRing<T, N>>();
  }
};
