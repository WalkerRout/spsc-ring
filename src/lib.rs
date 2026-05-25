#![no_std]

use core::cell::{Cell, UnsafeCell};
use core::marker::PhantomData;
use core::mem::{self, MaybeUninit};
use core::ops::{Deref, DerefMut, Index};
use core::sync::atomic::{AtomicUsize, Ordering};

#[cfg(feature = "heap")]
extern crate alloc;

#[cfg(feature = "heap")]
use alloc::{boxed::Box, vec::Vec};

struct ProducerInner<'r, T, const N: usize> {
  ring: &'r SpscRing<T, N>,
  // we can push until we hit head, so cache the latest goal post, and when we
  // hit it, we can reload to see if its moved...
  cached_tail: usize,
  // enforce !Sync
  _unsync: PhantomData<Cell<()>>,
}

// wrapper to enforce single producer constraint
pub struct Producer<'r, T, const N: usize> {
  #[cfg(feature = "padded-handles")]
  inner: CachePadded<ProducerInner<'r, T, N>>,
  #[cfg(not(feature = "padded-handles"))]
  inner: ProducerInner<'r, T, N>,
}

impl<T, const N: usize> Producer<'_, T, N> {
  #[inline(always)]
  pub fn enqueue(&mut self, elem: T) -> Result<(), T> {
    #[cfg(feature = "padded-handles")]
    let inner = &mut *self.inner;
    #[cfg(not(feature = "padded-handles"))]
    let inner = &mut self.inner;
    inner.ring.enqueue(elem, &mut inner.cached_tail)
  }

  #[inline(always)]
  pub fn is_full(&self) -> bool {
    is_full(self.inner.ring)
  }
}

struct ConsumerInner<'r, T, const N: usize> {
  ring: &'r SpscRing<T, N>,
  // same shit as producer
  cached_head: usize,
  // enforce !Sync
  _unsync: PhantomData<Cell<()>>,
}

// wrapper to enforce single consumer constraint
pub struct Consumer<'r, T, const N: usize> {
  #[cfg(feature = "padded-handles")]
  inner: CachePadded<ConsumerInner<'r, T, N>>,
  #[cfg(not(feature = "padded-handles"))]
  inner: ConsumerInner<'r, T, N>,
}

impl<T, const N: usize> Consumer<'_, T, N> {
  #[inline(always)]
  pub fn dequeue(&mut self) -> Result<T, Error> {
    #[cfg(feature = "padded-handles")]
    let inner = &mut *self.inner;
    #[cfg(not(feature = "padded-handles"))]
    let inner = &mut self.inner;
    inner.ring.dequeue(&mut inner.cached_head)
  }

  #[inline(always)]
  pub fn is_empty(&self) -> bool {
    is_empty(self.inner.ring)
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

#[cfg(feature = "padded-slots")]
struct Slot<T>(CachePadded<UnsafeCell<MaybeUninit<T>>>);

#[cfg(not(feature = "padded-slots"))]
struct Slot<T>(UnsafeCell<MaybeUninit<T>>);

impl<T> Slot<T> {
  fn new() -> Self {
    #[cfg(feature = "padded-slots")]
    {
      Self(CachePadded(UnsafeCell::new(MaybeUninit::uninit())))
    }
    #[cfg(not(feature = "padded-slots"))]
    {
      Self(UnsafeCell::new(MaybeUninit::uninit()))
    }
  }
}

impl<T> Deref for Slot<T> {
  type Target = UnsafeCell<MaybeUninit<T>>;

  fn deref(&self) -> &Self::Target {
    // lol
    #[cfg(feature = "padded-slots")]
    {
      &self.0.0
    }
    #[cfg(not(feature = "padded-slots"))]
    {
      &self.0
    }
  }
}

// ripped all of these cfg_attrs directly from crossbeam_utils/cache_padded.rs
// - https://docs.rs/crossbeam-utils/latest/src/crossbeam_utils/cache_padded.rs.html#63
#[cfg_attr(
  any(
    target_arch = "x86_64",
    target_arch = "aarch64",
    target_arch = "arm64ec",
    target_arch = "powerpc64",
  ),
  repr(align(128))
)]
#[cfg_attr(
  any(
    target_arch = "arm",
    target_arch = "mips",
    target_arch = "mips32r6",
    target_arch = "mips64",
    target_arch = "mips64r6",
    // include xtensa for esp32 projects...
    target_arch = "xtensa",
    target_arch = "sparc",
    target_arch = "hexagon",
  ),
  repr(align(32))
)]
#[cfg_attr(target_arch = "m68k", repr(align(16)))]
#[cfg_attr(target_arch = "s390x", repr(align(256)))]
#[cfg_attr(
  not(any(
    target_arch = "x86_64",
    target_arch = "aarch64",
    target_arch = "arm64ec",
    target_arch = "powerpc64",
    target_arch = "arm",
    target_arch = "mips",
    target_arch = "mips32r6",
    target_arch = "mips64",
    target_arch = "mips64r6",
    target_arch = "sparc",
    target_arch = "hexagon",
    target_arch = "m68k",
    target_arch = "s390x",
  )),
  repr(align(64))
)]
struct CachePadded<T>(T);

impl<T> Deref for CachePadded<T> {
  type Target = T;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

impl<T> DerefMut for CachePadded<T> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.0
  }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
  #[error("spsc ring queue is empty")]
  QueueIsEmpty,
}

struct Ring<T, const N: usize> {
  #[cfg(feature = "heap")]
  slots: Box<[Slot<T>; N]>,
  #[cfg(not(feature = "heap"))]
  slots: [Slot<T>; N],
}

impl<T, const N: usize> Ring<T, N> {
  const ASSERT_VALID_CAPACITY: () = assert!(
    N >= 2 && N.is_power_of_two(),
    "ring must have size >=2 for power of two N"
  );

  #[inline]
  fn new() -> Self {
    let () = Self::ASSERT_VALID_CAPACITY;
    // we just box slots on heap when we have access to alloc
    #[cfg(feature = "heap")]
    let slots = {
      (0..N)
        .map(|_| Slot::new())
        .collect::<Vec<_>>()
        .into_boxed_slice()
        .try_into()
        .ok()
        .unwrap()
    };
    // use stack-backed memory without heap feature
    #[cfg(not(feature = "heap"))]
    let slots = {
      use core::array;
      array::from_fn(|_| Slot::new())
    };
    Self { slots }
  }
}

impl<T, const N: usize> Index<usize> for Ring<T, N> {
  type Output = Slot<T>;

  #[inline(always)]
  fn index(&self, i: usize) -> &Slot<T> {
    &self.slots[i & (N - 1)]
  }
}

/// Lock-free, single-producer single-consumer ring buffer
/// - contains N-1 available slots, SpscRing<T, 16>
pub struct SpscRing<T, const N: usize> {
  head: CachePadded<AtomicUsize>,
  tail: CachePadded<AtomicUsize>,
  ring: Ring<T, N>,
}

impl<T, const N: usize> SpscRing<T, N> {
  #[must_use]
  #[inline]
  pub fn new() -> Self {
    Self {
      head: CachePadded(AtomicUsize::new(0)),
      tail: CachePadded(AtomicUsize::new(0)),
      ring: Ring::new(),
    }
  }

  #[inline]
  pub fn split(&mut self) -> (Producer<'_, T, N>, Consumer<'_, T, N>) {
    let pinner = ProducerInner {
      ring: self,
      cached_tail: self.tail.load(Ordering::Relaxed),
      _unsync: PhantomData,
    };
    let cinner = ConsumerInner {
      ring: self,
      cached_head: self.head.load(Ordering::Relaxed),
      _unsync: PhantomData,
    };
    let producer = Producer {
      #[cfg(feature = "padded-handles")]
      inner: CachePadded(pinner),
      #[cfg(not(feature = "padded-handles"))]
      inner: pinner,
    };
    let consumer = Consumer {
      #[cfg(feature = "padded-handles")]
      inner: CachePadded(cinner),
      #[cfg(not(feature = "padded-handles"))]
      inner: cinner,
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
      (*self.ring[head].get()).write(elem);
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
    let elem = unsafe { (*self.ring[tail].get()).assume_init_read() };
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
      // tail is racing to catch up to head
      let mut tail = self.tail.load(Ordering::Relaxed);
      let head = self.head.load(Ordering::Relaxed);
      while tail != head {
        // safety; all elements between tail and head are uniquely owned and live
        unsafe {
          (*self.ring[tail].get()).assume_init_drop();
        }
        tail = step::<N>(tail);
      }
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
