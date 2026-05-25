use std::array;
use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::mem::{self, MaybeUninit};
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};

// wrapper to enforce single producer constraint
#[cfg_attr(feature = "padded-handles", repr(align(64)))]
pub struct Producer<'r, T, const N: usize> {
  inner: &'r SpscRing<T, N>,
  // we can push until we hit head, so cache the latest goal post, and when we
  // hit it, we can reload to see if its moved...
  cached_tail: usize,
  // enforce !Sync
  _unsync: PhantomData<Cell<()>>,
}

impl<T, const N: usize> Producer<'_, T, N> {
  #[inline(always)]
  pub fn enqueue(&mut self, elem: T) -> Result<(), T> {
    self.inner.enqueue(elem, &mut self.cached_tail)
  }

  #[inline(always)]
  pub fn is_full(&self) -> bool {
    is_full(self.inner)
  }
}

// wrapper to enforce single consumer constraint
#[cfg_attr(feature = "padded-handles", repr(align(64)))]
pub struct Consumer<'r, T, const N: usize> {
  inner: &'r SpscRing<T, N>,
  // same shit as producer
  cached_head: usize,
  // enforce !Sync
  _unsync: PhantomData<Cell<()>>,
}

impl<T, const N: usize> Consumer<'_, T, N> {
  #[inline(always)]
  pub fn dequeue(&mut self) -> Result<T, Error> {
    self.inner.dequeue(&mut self.cached_head)
  }

  #[inline(always)]
  pub fn is_empty(&self) -> bool {
    is_empty(self.inner)
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

// cache-aligned slots are an additive feature...
#[cfg_attr(feature = "padded-slots", repr(align(64)))]
struct Slot<T>(UnsafeCell<MaybeUninit<T>>);

impl<T> Deref for Slot<T> {
  type Target = UnsafeCell<MaybeUninit<T>>;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

#[repr(align(64))]
struct CachePadded<T>(T);

impl<T> Deref for CachePadded<T> {
  type Target = T;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
  #[error("spsc ring queue is empty")]
  QueueIsEmpty,
}

/// Lock-free, single-producer single-consumer ring buffer
/// - contains N-1 available slots, SpscRing<T, 16>
pub struct SpscRing<T, const N: usize> {
  head: CachePadded<AtomicUsize>,
  tail: CachePadded<AtomicUsize>,
  ring: [Slot<T>; N],
}

impl<T, const N: usize> SpscRing<T, N> {
  const ASSERT_VALID_CAPACITY: () = assert!(
    N >= 2 && N.is_power_of_two(),
    "spsc ring must have size >=2 for power of two N"
  );

  #[must_use]
  #[inline]
  pub fn new() -> Self {
    let () = Self::ASSERT_VALID_CAPACITY;
    Self {
      head: CachePadded(AtomicUsize::new(0)),
      tail: CachePadded(AtomicUsize::new(0)),
      ring: array::from_fn(|_| Slot(UnsafeCell::new(MaybeUninit::uninit()))),
    }
  }

  #[inline]
  pub fn split(&mut self) -> (Producer<'_, T, N>, Consumer<'_, T, N>) {
    let producer = Producer {
      inner: self,
      cached_tail: 0,
      _unsync: PhantomData,
    };
    let consumer = Consumer {
      inner: self,
      cached_head: 0,
      _unsync: PhantomData,
    };
    (producer, consumer)
  }

  // head is owned by the producer
  // - cached_tail is refreshed when head catches back around to tail... this means
  //   the queue is full, and we need to check to see if the tail has moved forward
  #[inline]
  fn enqueue(&self, elem: T, cached_tail: &mut usize) -> Result<(), T> {
    // producer owns head, we are reading our own writes...
    let head = self.head.load(Ordering::Relaxed);
    let next_head = step::<N>(head);
    if next_head == *cached_tail {
      // synchronize-with consumer
      *cached_tail = self.tail.load(Ordering::Acquire);
      if next_head == *cached_tail {
        return Err(elem);
      }
    }
    // safety; we stomp whatever used to be in that slot with a new entry, and every
    // slot is initialized...
    unsafe {
      (*self.ring[head & (N - 1)].get()).write(elem);
    }
    self.head.store(next_head, Ordering::Release);
    Ok(())
  }

  // tail is owned by consumer
  // - cached_head is refreshed when tail catches up to head (its empty), to see if
  //   anything else was added in the meantime...
  // - wanted signature to be -> Result<T, ()> but clippy got mad
  #[inline]
  fn dequeue(&self, cached_head: &mut usize) -> Result<T, Error> {
    // consumer owns tail, again we are reading our own writes
    let tail = self.tail.load(Ordering::Relaxed);
    // did we catch up to the head?
    if tail == *cached_head {
      // yup, synchronize-with producer
      *cached_head = self.head.load(Ordering::Acquire);
      // has the head moved?
      if tail == *cached_head {
        // nope still empty
        return Err(Error::QueueIsEmpty);
      }
    }
    // safety; previous tail slot is treated as garbage after we step the tail, so
    // we can claim sole ownership of the contained element
    let elem = unsafe { (*self.ring[tail & (N - 1)].get()).assume_init_read() };
    let next_tail = step::<N>(tail);
    self.tail.store(next_tail, Ordering::Release);
    Ok(elem)
  }
}

// only meaningful when called by consumer (owns tail)
#[inline(always)]
fn is_empty<T, const N: usize>(ring: &SpscRing<T, N>) -> bool {
  // synchronize-with producer
  let head = ring.head.load(Ordering::Acquire);
  let tail = ring.tail.load(Ordering::Relaxed);
  head == tail
}

// only meaningful when called by producer (owns head)
#[inline(always)]
fn is_full<T, const N: usize>(ring: &SpscRing<T, N>) -> bool {
  // synchronize-with consumer
  let tail = ring.tail.load(Ordering::Acquire);
  let head = ring.head.load(Ordering::Relaxed);
  let next_head = step::<N>(head);
  next_head == tail
}

#[inline(always)]
fn step<const N: usize>(i: usize) -> usize {
  (i + 1) & (N - 1)
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
