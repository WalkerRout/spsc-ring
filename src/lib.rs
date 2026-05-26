#![no_std]

use core::cell::{Cell, UnsafeCell};
use core::marker::PhantomData;
use core::mem::{self, ManuallyDrop, MaybeUninit};
use core::ops::{Deref, DerefMut, Index};
use core::ptr;
use core::slice;
use core::sync::atomic::{AtomicUsize, Ordering};

#[cfg(feature = "heap")]
extern crate alloc;

#[cfg(feature = "heap")]
use alloc::{boxed::Box, vec::Vec};

struct ProducerInner<'r, T, const N: usize> {
  ring: &'r SpscRing<T, N>,
  // own cursor cached locally; we own head, so this matches self.ring.head after every
  // successful enqueue and we never need to Relaxed-load our own atomic
  cached_head: usize,
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

impl<'r, T, const N: usize> Producer<'r, T, N> {
  #[inline(always)]
  fn inner_mut(&mut self) -> &mut ProducerInner<'r, T, N> {
    #[cfg(feature = "padded-handles")]
    return &mut *self.inner;
    #[cfg(not(feature = "padded-handles"))]
    return &mut self.inner;
  }

  #[inline(always)]
  pub fn enqueue(&mut self, elem: T) -> Result<(), T> {
    let inner = self.inner_mut();
    inner
      .ring
      .enqueue(elem, &mut inner.cached_head, &mut inner.cached_tail)
  }

  // might be cool if we return some struct Enqueued that lets us write into
  // the maybeuninit slots themselves, and then commit those writes into the
  // ring buffer...
  #[inline(always)]
  pub fn enqueue_batch<I>(&mut self, items: I) -> usize
  where
    I: IntoIterator<Item = T>,
  {
    let inner = self.inner_mut();
    inner
      .ring
      .enqueue_batch(items, &mut inner.cached_head, &mut inner.cached_tail)
  }

  #[inline(always)]
  pub fn enqueue_batch_copy(&mut self, items: &[T]) -> usize
  where
    T: Copy,
  {
    let inner = self.inner_mut();
    inner
      .ring
      .enqueue_batch_copy(items, &mut inner.cached_head, &mut inner.cached_tail)
  }

  #[inline(always)]
  pub fn is_full(&self) -> bool {
    is_full(self.inner.ring)
  }
}

struct ConsumerInner<'r, T, const N: usize> {
  ring: &'r SpscRing<T, N>,
  // own cursor cached locally; we own tail, so this matches self.ring.tail after every
  // successful dequeue and we never need to Relaxed-load our own atomic
  cached_tail: usize,
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

impl<'r, T, const N: usize> Consumer<'r, T, N> {
  #[inline(always)]
  fn inner_mut(&mut self) -> &mut ConsumerInner<'r, T, N> {
    #[cfg(feature = "padded-handles")]
    return &mut *self.inner;
    #[cfg(not(feature = "padded-handles"))]
    return &mut self.inner;
  }

  #[inline(always)]
  pub fn dequeue(&mut self) -> Result<T, Error> {
    let inner = self.inner_mut();
    inner
      .ring
      .dequeue(&mut inner.cached_tail, &mut inner.cached_head)
  }

  #[inline(always)]
  pub fn dequeue_batch<'a>(&mut self, dst: &'a mut [MaybeUninit<T>]) -> Dequeued<'a, T> {
    let inner = self.inner_mut();
    let len = inner
      .ring
      .dequeue_batch(dst, &mut inner.cached_tail, &mut inner.cached_head);
    Dequeued { buf: dst, len }
  }

  // wrapper around dequeue_batch for copy types, since we can basically treat them the same
  // way as maybeuninit types...
  #[inline(always)]
  pub fn dequeue_batch_copy<'a>(&mut self, dst: &'a mut [T]) -> Dequeued<'a, T>
  where
    T: Copy,
  {
    // safety; T is copy, so we sorta treat it as plain old data... we can just overwrite it
    // or drop it or whatever and it doesnt care...
    let dst_uninit: &'a mut [MaybeUninit<T>] =
      unsafe { slice::from_raw_parts_mut(dst.as_mut_ptr().cast::<MaybeUninit<T>>(), dst.len()) };
    self.dequeue_batch(dst_uninit)
  }

  #[inline(always)]
  pub fn is_empty(&self) -> bool {
    is_empty(self.inner.ring)
  }
}

pub struct Dequeued<'a, T> {
  // <=buf.len(), represents number of successfully dequeued elements
  len: usize,
  buf: &'a mut [MaybeUninit<T>],
}

impl<'a, T> Dequeued<'a, T> {
  #[inline(always)]
  pub fn len(&self) -> usize {
    self.len
  }

  #[inline(always)]
  pub fn is_empty(&self) -> bool {
    self.len == 0
  }

  #[inline(always)]
  pub fn as_slice(&self) -> &[T] {
    // safety; we exclusively own buf and [0, len) is alive
    unsafe { slice::from_raw_parts(self.buf.as_ptr().cast::<T>(), self.len) }
  }

  #[inline(always)]
  pub fn as_mut_slice(&mut self) -> &mut [T] {
    // safety; we exclusively own buf and [0, len) is alive
    unsafe { slice::from_raw_parts_mut(self.buf.as_mut_ptr().cast::<T>(), self.len) }
  }
}

impl<T> Drop for Dequeued<'_, T> {
  #[inline]
  fn drop(&mut self) {
    for i in 0..self.len {
      // safety; can only be constructed with valid elements 0..len, so these are all live
      unsafe {
        self.buf[i].assume_init_drop();
      }
    }
  }
}

impl<T> Deref for Dequeued<'_, T> {
  type Target = [T];

  #[inline(always)]
  fn deref(&self) -> &Self::Target {
    self.as_slice()
  }
}

impl<T> DerefMut for Dequeued<'_, T> {
  #[inline(always)]
  fn deref_mut(&mut self) -> &mut Self::Target {
    self.as_mut_slice()
  }
}

impl<'a, T> IntoIterator for Dequeued<'a, T> {
  type Item = T;
  type IntoIter = DequeuedIntoIter<'a, T>;

  #[inline(always)]
  fn into_iter(self) -> Self::IntoIter {
    // we steal ownership out of self; we dont want to clear the memory we are taking...
    let this = ManuallyDrop::new(self);
    // cant move non-copy out of &T/&mut T and thats all manuallydrop gives us... so
    // we steal the buffer...
    // safety; buffer is alive for lifetime 'a, and we have sole ownership
    let buf = unsafe { ptr::read(&this.buf) };
    DequeuedIntoIter {
      buf,
      front: 0,
      back: this.len,
    }
  }
}

pub struct DequeuedIntoIter<'a, T> {
  // we store a front and back cause we have enough info for a double ended iterator
  front: usize,
  back: usize,
  buf: &'a mut [MaybeUninit<T>],
}

impl<T> Iterator for DequeuedIntoIter<'_, T> {
  type Item = T;

  #[inline(always)]
  fn next(&mut self) -> Option<Self::Item> {
    if self.front == self.back {
      return None;
    }
    let i = self.front;
    // walk forwards
    self.front += 1;
    // safety; all those elements in [front, back) are valid by construction
    Some(unsafe { self.buf[i].assume_init_read() })
  }

  #[inline(always)]
  fn size_hint(&self) -> (usize, Option<usize>) {
    let len = self.back - self.front;
    (len, Some(len))
  }
}

impl<T> DoubleEndedIterator for DequeuedIntoIter<'_, T> {
  #[inline(always)]
  fn next_back(&mut self) -> Option<Self::Item> {
    if self.front == self.back {
      return None;
    }
    // step backwards
    self.back -= 1;
    // safety; all those elements in [front, back) are valid by construction
    Some(unsafe { self.buf[self.back].assume_init_read() })
  }
}

impl<T> ExactSizeIterator for DequeuedIntoIter<'_, T> {
  #[inline(always)]
  fn len(&self) -> usize {
    self.back - self.front
  }
}

impl<T> Drop for DequeuedIntoIter<'_, T> {
  #[inline]
  fn drop(&mut self) {
    while self.front != self.back {
      // safety; same thing as dequeued::drop, we have exclusive ownership over these
      // valid elements
      unsafe {
        self.buf[self.front].assume_init_drop();
      }
      self.front += 1;
    }
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

#[repr(transparent)]
struct Slot<T> {
  #[cfg(feature = "padded-slots")]
  entry: CachePadded<UnsafeCell<MaybeUninit<T>>>,
  #[cfg(not(feature = "padded-slots"))]
  entry: UnsafeCell<MaybeUninit<T>>,
}

impl<T> Slot<T> {
  const COMPACT: bool = mem::size_of::<Self>() == mem::size_of::<MaybeUninit<T>>();
}

impl<T> Default for Slot<T> {
  fn default() -> Self {
    Self {
      #[cfg(feature = "padded-slots")]
      entry: CachePadded(UnsafeCell::new(MaybeUninit::uninit())),
      #[cfg(not(feature = "padded-slots"))]
      entry: UnsafeCell::new(MaybeUninit::uninit()),
    }
  }
}

impl<T> Deref for Slot<T> {
  type Target = UnsafeCell<MaybeUninit<T>>;

  fn deref(&self) -> &Self::Target {
    #[cfg(feature = "padded-slots")]
    {
      &self.entry.0
    }
    #[cfg(not(feature = "padded-slots"))]
    {
      &self.entry
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

#[repr(transparent)]
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
        .map(|_| Slot::default())
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
      array::from_fn(|_| Slot::default())
    };
    Self { slots }
  }

  #[inline(always)]
  fn chunks(&self, start: usize, n: usize) -> (&[Slot<T>], &[Slot<T>]) {
    let first_len = n.min(N - start);
    let second_len = n - first_len;
    (
      &self.slots[start..start + first_len],
      &self.slots[..second_len],
    )
  }

  // safety; caller has exclusive write access to slots [start..start+items.len())
  #[inline]
  unsafe fn write_copy(&self, start: usize, items: &[T])
  where
    T: Copy,
  {
    let (first, second) = self.chunks(start, items.len());
    if Slot::<T>::COMPACT {
      // safety; slot slices are layout compatible
      // - we do two memcpys... i dont like that, but it works...
      // - todo for specialized targets, we could mmap the same physical pages at adjacent
      //   virtual addresses, and then have the page table transparently handle wrap
      //   around... im feeling lazy, so im saving this for a later date...
      //   - https://fgiesen.wordpress.com/2012/07/21/the-magic-ring-buffer/
      //   - https://andreleite.com/posts/2025/nstl/virtual-memory-ring-buffer/
      //   - https://www.reachablecode.com/2022/11/22/a-doubly-mmapped-contiguous-shared-memory-lock-free-queue/
      unsafe {
        ptr::copy_nonoverlapping(items.as_ptr(), first.as_ptr() as *mut T, first.len());
        ptr::copy_nonoverlapping(
          items.as_ptr().add(first.len()),
          second.as_ptr() as *mut T,
          second.len(),
        );
      }
    } else {
      for (slot, &item) in first.iter().chain(second).zip(items) {
        // safety; no other thread can read this slot while we publish
        unsafe { (*slot.get()).write(item) };
      }
    }
  }

  // safety; caller has exclusive read access to slots [start..start+dst.len())
  // and those slots are alive
  #[inline]
  unsafe fn read_into(&self, start: usize, dst: &mut [MaybeUninit<T>]) {
    let (first, second) = self.chunks(start, dst.len());
    if Slot::<T>::COMPACT {
      // safety; slot slices are layout compatible...
      unsafe {
        ptr::copy_nonoverlapping(
          first.as_ptr() as *const T,
          dst.as_mut_ptr().cast::<T>(),
          first.len(),
        );
        ptr::copy_nonoverlapping(
          second.as_ptr() as *const T,
          dst.as_mut_ptr().add(first.len()).cast::<T>(),
          second.len(),
        );
      }
    } else {
      for (slot, out) in first.iter().chain(second).zip(dst.iter_mut()) {
        // safety; slot is initialized and treated as garbage after being drained
        let elem = unsafe { (*slot.get()).assume_init_read() };
        out.write(elem);
      }
    }
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
pub struct SpscRing<T, const N: usize> {
  // head and tail are monotonic; we only care about the difference between them,
  // not the values themselves... this means we can use whatever numbers we want
  // (modulo N) to act as our representation, s.t. we only ever need to increment
  // and wrap around...
  head: CachePadded<AtomicUsize>,
  tail: CachePadded<AtomicUsize>,
  ring: Ring<T, N>,
}

impl<T, const N: usize> SpscRing<T, N> {
  #[inline]
  #[must_use]
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
      cached_head: self.head.load(Ordering::Relaxed),
      cached_tail: self.tail.load(Ordering::Relaxed),
      _unsync: PhantomData,
    };
    let producer = Producer {
      #[cfg(feature = "padded-handles")]
      inner: CachePadded(pinner),
      #[cfg(not(feature = "padded-handles"))]
      inner: pinner,
    };
    let cinner = ConsumerInner {
      ring: self,
      cached_tail: self.tail.load(Ordering::Relaxed),
      cached_head: self.head.load(Ordering::Relaxed),
      _unsync: PhantomData,
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
  // - cached_head matches self.head after every successful enqueue, so we never need
  //   a Relaxed-load of our own atomic on the hot path
  // - cached_tail is refreshed when producers view of count reaches N
  // - tail only grows, so producers view (head-cached_tail) is always >= the actual
  //   count (head-actual_tail)
  // - we check the count (head-cached_tail) and if its <N, we have room...
  //   - if its ==N, we might not have room and we need to check again...
  //   - it can never be >N, since we check if its ==N before incrementing...
  #[inline]
  fn enqueue(&self, elem: T, cached_head: &mut usize, cached_tail: &mut usize) -> Result<(), T> {
    let head = *cached_head;
    if head.wrapping_sub(*cached_tail) == N {
      // synchronize-with consumer
      *cached_tail = self.tail.load(Ordering::Acquire);
      if head.wrapping_sub(*cached_tail) == N {
        return Err(elem);
      }
    }
    // safety; we stomp whatever used to be in that slot with a new entry, and every
    // slot is initialized...
    unsafe {
      (*self.ring[head].get()).write(elem);
    }
    let new_head = head.wrapping_add(1);
    self.head.store(new_head, Ordering::Release);
    *cached_head = new_head;
    Ok(())
  }

  // tail is owned by consumer
  // - cached_tail matches self.tail after every successful dequeue, so we never need
  //   a Relaxed-load of our own atomic on the hot path
  // - cached_head is refreshed when tail catches up to head (its empty), to see if
  //   anything else was added in the meantime...
  // - wanted signature to be -> Result<T, ()> but clippy got mad
  #[inline]
  fn dequeue(&self, cached_tail: &mut usize, cached_head: &mut usize) -> Result<T, Error> {
    let tail = *cached_tail;
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
    let new_tail = tail.wrapping_add(1);
    self.tail.store(new_tail, Ordering::Release);
    *cached_tail = new_tail;
    Ok(elem)
  }

  #[inline]
  fn enqueue_batch<I>(&self, items: I, cached_head: &mut usize, cached_tail: &mut usize) -> usize
  where
    I: IntoIterator<Item = T>,
  {
    let head = *cached_head;
    let mut room = N - head.wrapping_sub(*cached_tail);
    if room == 0 {
      *cached_tail = self.tail.load(Ordering::Acquire);
      room = N - head.wrapping_sub(*cached_tail);
      if room == 0 {
        return 0;
      }
    }
    let (first, second) = self.ring.chunks(head & (N - 1), room);
    let mut items = items.into_iter();
    let mut n = 0;
    // autovectorizes better with two loops, state flag in chain fought compiler
    'outer: for slots in [first, second] {
      for slot in slots {
        let Some(item) = items.next() else {
          break 'outer;
        };
        // safety; slot is in [head, head+room) which is exclusively ours to write
        unsafe { (*slot.get()).write(item) };
        n += 1;
      }
    }
    if n > 0 {
      let new_head = head.wrapping_add(n);
      self.head.store(new_head, Ordering::Release);
      *cached_head = new_head;
    }
    n
  }

  #[inline]
  fn enqueue_batch_copy(
    &self,
    items: &[T],
    cached_head: &mut usize,
    cached_tail: &mut usize,
  ) -> usize
  where
    T: Copy,
  {
    let head = *cached_head;
    let mut room = N - head.wrapping_sub(*cached_tail);
    if room < items.len() {
      *cached_tail = self.tail.load(Ordering::Acquire);
      room = N - head.wrapping_sub(*cached_tail);
    }
    let n = items.len().min(room);
    if n > 0 {
      // safety; we hold exclusive write access to slots [head, head+n)
      unsafe { self.ring.write_copy(head & (N - 1), &items[..n]) };
      let new_head = head.wrapping_add(n);
      self.head.store(new_head, Ordering::Release);
      *cached_head = new_head;
    }
    n
  }

  #[inline]
  fn dequeue_batch(
    &self,
    dst: &mut [MaybeUninit<T>],
    cached_tail: &mut usize,
    cached_head: &mut usize,
  ) -> usize {
    let tail = *cached_tail;
    let mut available = cached_head.wrapping_sub(tail);
    if available < dst.len() {
      *cached_head = self.head.load(Ordering::Acquire);
      available = cached_head.wrapping_sub(tail);
    }
    let n = dst.len().min(available);
    if n > 0 {
      // safety; slots [tail, tail+n) are initialized and exclusively ours to read
      unsafe { self.ring.read_into(tail & (N - 1), &mut dst[..n]) };
      let new_tail = tail.wrapping_add(n);
      self.tail.store(new_tail, Ordering::Release);
      *cached_tail = new_tail;
    }
    n
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
  head.wrapping_sub(tail) == N
}

impl<T, const N: usize> Default for SpscRing<T, N> {
  fn default() -> Self {
    Self::new()
  }
}

impl<T, const N: usize> Drop for SpscRing<T, N> {
  fn drop(&mut self) {
    if mem::needs_drop::<T>() {
      let tail = self.tail.load(Ordering::Relaxed);
      let head = self.head.load(Ordering::Relaxed);
      let n = head.wrapping_sub(tail);
      let (first, second) = self.ring.chunks(tail & (N - 1), n);
      for slot in first.iter().chain(second) {
        // safety; all elements between tail and head are uniquely owned and live
        unsafe { (*slot.get()).assume_init_drop() };
      }
    }
  }
}

// safety; our public api enforces single producer, single consumer architecture
// and we use atomic operations internally to ensure synchronization between threads
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

#[cfg(test)]
mod tests {
  use super::*;

  mod producer {
    use super::*;

    #[test]
    fn enqueue() {
      let mut ring = SpscRing::<u32, 4>::new();
      let (mut p, _c) = ring.split();
      assert!(p.enqueue(1).is_ok());
      assert!(p.enqueue(2).is_ok());
      assert!(p.enqueue(3).is_ok());
      assert!(p.enqueue(4).is_ok());
    }

    #[test]
    fn enqueue_full_returns_value() {
      let mut ring = SpscRing::<u32, 2>::new();
      let (mut p, _c) = ring.split();
      p.enqueue(10).unwrap();
      p.enqueue(20).unwrap();
      assert_eq!(p.enqueue(30), Err(30));
    }

    #[test]
    fn enqueue_batch_copy_fits() {
      let mut ring = SpscRing::<u32, 8>::new();
      let (mut p, _c) = ring.split();
      assert_eq!(p.enqueue_batch_copy(&[1, 2, 3]), 3);
    }

    #[test]
    fn enqueue_batch_copy_partial() {
      let mut ring = SpscRing::<u32, 4>::new();
      let (mut p, _c) = ring.split();
      assert_eq!(p.enqueue_batch_copy(&[1, 2, 3, 4, 5, 6]), 4);
    }

    #[test]
    fn enqueue_batch_copy_empty() {
      let mut ring = SpscRing::<u32, 4>::new();
      let (mut p, _c) = ring.split();
      assert_eq!(p.enqueue_batch_copy(&[]), 0);
    }

    #[test]
    fn enqueue_batch_copy_wraps_slot_indices() {
      let mut ring = SpscRing::<u32, 4>::new();
      let (mut p, mut c) = ring.split();
      assert_eq!(p.enqueue_batch_copy(&[1, 2, 3]), 3);
      assert_eq!(c.dequeue().unwrap(), 1);
      assert_eq!(c.dequeue().unwrap(), 2);
      assert_eq!(p.enqueue_batch_copy(&[4, 5, 6]), 3);
      assert_eq!(c.dequeue().unwrap(), 3);
      assert_eq!(c.dequeue().unwrap(), 4);
      assert_eq!(c.dequeue().unwrap(), 5);
      assert_eq!(c.dequeue().unwrap(), 6);
    }

    #[test]
    fn enqueue_batch_consumes_iter_when_fits() {
      let mut ring = SpscRing::<u32, 8>::new();
      let (mut p, _c) = ring.split();
      assert_eq!(p.enqueue_batch([1u32, 2, 3]), 3);
    }

    #[test]
    fn enqueue_batch_fills_what_fits() {
      let mut ring = SpscRing::<u32, 4>::new();
      let (mut p, _c) = ring.split();
      assert_eq!(p.enqueue_batch([1u32, 2, 3, 4, 5, 6]), 4);
    }

    #[test]
    fn enqueue_batch_leaves_unconsumed_in_borrowed_iter() {
      let mut ring = SpscRing::<u32, 4>::new();
      let (mut p, _c) = ring.split();
      let mut iter = [1u32, 2, 3, 4, 5, 6, 7].into_iter();
      assert_eq!(p.enqueue_batch(iter.by_ref()), 4);
      assert_eq!(iter.next(), Some(5));
    }

    #[test]
    fn is_full() {
      let mut ring = SpscRing::<u32, 4>::new();
      let (mut p, _c) = ring.split();
      assert!(!p.is_full());
      for i in 0..4 {
        p.enqueue(i).unwrap();
      }
      assert!(p.is_full());
    }
  }

  mod consumer {
    use super::*;

    #[test]
    fn dequeue() {
      let mut ring = SpscRing::<u32, 4>::new();
      let (mut p, mut c) = ring.split();
      p.enqueue(42).unwrap();
      assert_eq!(c.dequeue().unwrap(), 42);
    }

    #[test]
    fn dequeue_empty_returns_err() {
      let mut ring = SpscRing::<u32, 4>::new();
      let (_p, mut c) = ring.split();
      assert!(c.dequeue().is_err());
    }

    #[test]
    fn dequeue_batch() {
      let mut ring = SpscRing::<u32, 8>::new();
      let (mut p, mut c) = ring.split();
      p.enqueue_batch_copy(&[1, 2, 3, 4]);
      let mut buf: [MaybeUninit<u32>; 4] = [MaybeUninit::uninit(); 4];
      let d = c.dequeue_batch(&mut buf);
      assert_eq!(d.len(), 4);
      assert_eq!(d.as_slice(), &[1, 2, 3, 4]);
    }

    #[test]
    fn dequeue_batch_partial() {
      let mut ring = SpscRing::<u32, 8>::new();
      let (mut p, mut c) = ring.split();
      p.enqueue_batch_copy(&[1, 2]);
      let mut buf: [MaybeUninit<u32>; 5] = [MaybeUninit::uninit(); 5];
      let d = c.dequeue_batch(&mut buf);
      assert_eq!(d.len(), 2);
      assert_eq!(d.as_slice(), &[1, 2]);
    }

    #[test]
    fn dequeue_batch_empty_queue() {
      let mut ring = SpscRing::<u32, 4>::new();
      let (_p, mut c) = ring.split();
      let mut buf: [MaybeUninit<u32>; 4] = [MaybeUninit::uninit(); 4];
      let d = c.dequeue_batch(&mut buf);
      assert_eq!(d.len(), 0);
      assert!(d.is_empty());
    }

    #[test]
    fn dequeue_batch_copy() {
      let mut ring = SpscRing::<u32, 8>::new();
      let (mut p, mut c) = ring.split();
      p.enqueue_batch_copy(&[10, 20, 30, 40]);
      let mut buf = [0u32; 4];
      let d = c.dequeue_batch_copy(&mut buf);
      assert_eq!(d.len(), 4);
      assert_eq!(d.as_slice(), &[10, 20, 30, 40]);
    }

    #[test]
    fn dequeue_batch_copy_wraps_slot_indices() {
      let mut ring = SpscRing::<u32, 4>::new();
      let (mut p, mut c) = ring.split();
      p.enqueue_batch_copy(&[1, 2, 3]);
      let mut buf = [0u32; 2];
      {
        let d = c.dequeue_batch_copy(&mut buf);
        assert_eq!(d.len(), 2);
        assert_eq!(d.as_slice(), &[1, 2]);
      }
      p.enqueue_batch_copy(&[4, 5, 6]);
      let mut buf = [0u32; 4];
      let d = c.dequeue_batch_copy(&mut buf);
      assert_eq!(d.len(), 4);
      assert_eq!(d.as_slice(), &[3, 4, 5, 6]);
    }

    #[test]
    fn is_empty() {
      let mut ring = SpscRing::<u32, 4>::new();
      let (mut p, c) = ring.split();
      assert!(c.is_empty());
      p.enqueue(1).unwrap();
      assert!(!c.is_empty());
    }
  }

  mod spsc_ring {
    use super::*;

    #[test]
    fn capacity_n_items() {
      let mut ring = SpscRing::<u32, 4>::new();
      let (mut p, _c) = ring.split();
      for i in 0..4 {
        p.enqueue(i).unwrap();
      }
      assert_eq!(p.enqueue(99), Err(99));
    }

    #[test]
    fn capacity_minimum() {
      let mut ring = SpscRing::<u32, 2>::new();
      let (mut p, mut c) = ring.split();
      p.enqueue(1).unwrap();
      p.enqueue(2).unwrap();
      assert_eq!(p.enqueue(3), Err(3));
      assert_eq!(c.dequeue().unwrap(), 1);
      assert_eq!(c.dequeue().unwrap(), 2);
      assert!(c.dequeue().is_err());
    }

    #[test]
    fn fifo_order() {
      let mut ring = SpscRing::<u32, 4>::new();
      let (mut p, mut c) = ring.split();
      for cycle in 0..100u32 {
        for i in 0..4 {
          p.enqueue(cycle * 10 + i).unwrap();
        }
        for i in 0..4 {
          assert_eq!(c.dequeue().unwrap(), cycle * 10 + i);
        }
      }
    }

    #[test]
    fn drop_walks_unconsumed() {
      static DROPS: AtomicUsize = AtomicUsize::new(0);
      #[derive(Debug)]
      struct DropCounter;
      impl Drop for DropCounter {
        fn drop(&mut self) {
          DROPS.fetch_add(1, Ordering::SeqCst);
        }
      }
      {
        let mut ring = SpscRing::<DropCounter, 4>::new();
        let (mut p, _c) = ring.split();
        p.enqueue(DropCounter).unwrap();
        p.enqueue(DropCounter).unwrap();
        p.enqueue(DropCounter).unwrap();
      }
      assert_eq!(DROPS.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn drop_walks_after_partial_drain() {
      static DROPS: AtomicUsize = AtomicUsize::new(0);
      #[derive(Debug)]
      struct DropCounter;
      impl Drop for DropCounter {
        fn drop(&mut self) {
          DROPS.fetch_add(1, Ordering::SeqCst);
        }
      }
      {
        let mut ring = SpscRing::<DropCounter, 4>::new();
        let (mut p, mut c) = ring.split();
        for _ in 0..4 {
          p.enqueue(DropCounter).unwrap();
        }
        c.dequeue().unwrap();
        c.dequeue().unwrap();
      }
      assert_eq!(DROPS.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn drop_walks_across_slot_wrap() {
      static DROPS: AtomicUsize = AtomicUsize::new(0);
      #[derive(Debug)]
      struct DropCounter;
      impl Drop for DropCounter {
        fn drop(&mut self) {
          DROPS.fetch_add(1, Ordering::SeqCst);
        }
      }
      {
        let mut ring = SpscRing::<DropCounter, 4>::new();
        let (mut p, mut c) = ring.split();
        for _ in 0..4 {
          p.enqueue(DropCounter).unwrap();
        }
        for _ in 0..3 {
          c.dequeue().unwrap();
        }
        for _ in 0..3 {
          p.enqueue(DropCounter).unwrap();
        }
      }
      assert_eq!(DROPS.load(Ordering::SeqCst), 7);
    }
  }

  mod dequeued {
    use super::*;

    #[test]
    fn len_matches_dequeued() {
      let mut ring = SpscRing::<u32, 8>::new();
      let (mut p, mut c) = ring.split();
      p.enqueue_batch_copy(&[1, 2, 3]);
      let mut buf: [MaybeUninit<u32>; 8] = [MaybeUninit::uninit(); 8];
      let d = c.dequeue_batch(&mut buf);
      assert_eq!(d.len(), 3);
    }

    #[test]
    fn as_slice_yields_items_in_order() {
      let mut ring = SpscRing::<u32, 8>::new();
      let (mut p, mut c) = ring.split();
      p.enqueue_batch_copy(&[10, 20, 30, 40]);
      let mut buf: [MaybeUninit<u32>; 4] = [MaybeUninit::uninit(); 4];
      let d = c.dequeue_batch(&mut buf);
      assert_eq!(d.as_slice(), &[10, 20, 30, 40]);
    }

    #[test]
    fn deref_to_slice() {
      let mut ring = SpscRing::<u32, 8>::new();
      let (mut p, mut c) = ring.split();
      p.enqueue_batch_copy(&[1, 2, 3]);
      let mut buf: [MaybeUninit<u32>; 4] = [MaybeUninit::uninit(); 4];
      let d = c.dequeue_batch(&mut buf);
      let sum: u32 = d.iter().sum();
      assert_eq!(sum, 6);
    }

    #[test]
    fn drop_drops_all_items() {
      static DROPS: AtomicUsize = AtomicUsize::new(0);
      #[derive(Debug)]
      struct DropCounter;
      impl Drop for DropCounter {
        fn drop(&mut self) {
          DROPS.fetch_add(1, Ordering::SeqCst);
        }
      }
      let mut ring = SpscRing::<DropCounter, 4>::new();
      let (mut p, mut c) = ring.split();
      for _ in 0..3 {
        p.enqueue(DropCounter).unwrap();
      }
      let mut buf: [MaybeUninit<DropCounter>; 3] = unsafe { MaybeUninit::uninit().assume_init() };
      let d = c.dequeue_batch(&mut buf);
      assert_eq!(d.len(), 3);
      drop(d);
      assert_eq!(DROPS.load(Ordering::SeqCst), 3);
    }
  }

  mod dequeued_into_iter {
    use super::*;

    #[test]
    fn yields_in_order() {
      let mut ring = SpscRing::<u32, 8>::new();
      let (mut p, mut c) = ring.split();
      p.enqueue_batch_copy(&[100, 101, 102]);
      let mut buf: [MaybeUninit<u32>; 3] = [MaybeUninit::uninit(); 3];
      let d = c.dequeue_batch(&mut buf);
      let mut it = d.into_iter();
      assert_eq!(it.next(), Some(100));
      assert_eq!(it.next(), Some(101));
      assert_eq!(it.next(), Some(102));
      assert_eq!(it.next(), None);
    }

    #[test]
    fn next_back_reverses() {
      let mut ring = SpscRing::<u32, 8>::new();
      let (mut p, mut c) = ring.split();
      p.enqueue_batch_copy(&[1, 2, 3]);
      let mut buf: [MaybeUninit<u32>; 3] = [MaybeUninit::uninit(); 3];
      let d = c.dequeue_batch(&mut buf);
      let mut it = d.into_iter();
      assert_eq!(it.next_back(), Some(3));
      assert_eq!(it.next(), Some(1));
      assert_eq!(it.next_back(), Some(2));
      assert_eq!(it.next(), None);
    }

    #[test]
    fn exact_size_iterator_len() {
      let mut ring = SpscRing::<u32, 8>::new();
      let (mut p, mut c) = ring.split();
      p.enqueue_batch_copy(&[1, 2, 3, 4]);
      let mut buf: [MaybeUninit<u32>; 4] = [MaybeUninit::uninit(); 4];
      let d = c.dequeue_batch(&mut buf);
      let mut it = d.into_iter();
      assert_eq!(it.len(), 4);
      it.next();
      assert_eq!(it.len(), 3);
      it.next_back();
      assert_eq!(it.len(), 2);
    }

    #[test]
    fn drop_drops_remaining() {
      static DROPS: AtomicUsize = AtomicUsize::new(0);
      #[derive(Debug)]
      struct DropCounter;
      impl Drop for DropCounter {
        fn drop(&mut self) {
          DROPS.fetch_add(1, Ordering::SeqCst);
        }
      }
      let mut ring = SpscRing::<DropCounter, 4>::new();
      let (mut p, mut c) = ring.split();
      for _ in 0..4 {
        p.enqueue(DropCounter).unwrap();
      }
      let mut buf: [MaybeUninit<DropCounter>; 4] = unsafe { MaybeUninit::uninit().assume_init() };
      let d = c.dequeue_batch(&mut buf);
      let mut it = d.into_iter();
      drop(it.next().unwrap());
      drop(it.next().unwrap());
      drop(it);
      assert_eq!(DROPS.load(Ordering::SeqCst), 4);
    }
  }
}
