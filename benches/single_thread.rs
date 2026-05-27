use std::array;
use std::hint::black_box;
use std::mem::{self, MaybeUninit};
use std::time::{Duration, Instant};

use criterion::measurement::WallTime;
use criterion::{
  AxisScale, Bencher, BenchmarkGroup, BenchmarkId, Criterion, PlotConfiguration, Throughput,
  criterion_group, criterion_main,
};
use lib_spsc_ring::SpscRing;

trait Payload: Copy + 'static {
  const LABEL: &'static str;
  fn seed(i: u64) -> Self;
}

impl Payload for u8 {
  const LABEL: &'static str = "u8";
  fn seed(i: u64) -> Self {
    i as u8
  }
}

impl Payload for u64 {
  const LABEL: &'static str = "u64";
  fn seed(i: u64) -> Self {
    i
  }
}

impl Payload for [u8; 64] {
  const LABEL: &'static str = "bytes64";
  fn seed(i: u64) -> Self {
    [i as u8; 64]
  }
}

impl Payload for [u8; 256] {
  const LABEL: &'static str = "bytes256";
  fn seed(i: u64) -> Self {
    [i as u8; 256]
  }
}

fn enqueue<const N: usize>(b: &mut Bencher<'_>, ring: &mut SpscRing<u64, N>) {
  let (mut producer, mut consumer) = ring.split();
  let cap = N as u64;
  b.iter_custom(|iters| {
    let mut total = Duration::ZERO;
    let mut done = 0u64;
    while done < iters {
      let chunk = cap.min(iters - done);
      let start = Instant::now();
      for _ in 0..chunk {
        let _ = producer.enqueue(0u64);
      }
      total += start.elapsed();
      done += chunk;
      while consumer.dequeue().is_ok() {}
    }
    total
  });
}

fn dequeue<const N: usize>(b: &mut Bencher<'_>, ring: &mut SpscRing<u64, N>) {
  let (mut producer, mut consumer) = ring.split();
  let cap = N as u64;
  b.iter_custom(|iters| {
    let mut total = Duration::ZERO;
    let mut done = 0u64;
    while done < iters {
      while producer.enqueue(0u64).is_ok() {}
      let chunk = cap.min(iters - done);
      let start = Instant::now();
      for _ in 0..chunk {
        let _ = consumer.dequeue();
      }
      total += start.elapsed();
      done += chunk;
    }
    total
  });
}

fn enqueue_batch<const N: usize, const CHUNK: usize>(
  b: &mut Bencher<'_>,
  ring: &mut SpscRing<u64, N>,
) {
  let (mut producer, mut consumer) = ring.split();
  let src = [0u64; CHUNK];
  let mut dst: [MaybeUninit<u64>; CHUNK] = unsafe { MaybeUninit::uninit().assume_init() };
  b.iter_custom(|iters| {
    let mut total = Duration::ZERO;
    for _ in 0..iters {
      let start = Instant::now();
      let n = producer.enqueue_batch_copy(&src);
      total += start.elapsed();
      let _ = consumer.dequeue_batch(&mut dst[..n]);
    }
    total
  });
}

fn dequeue_batch<const N: usize, const CHUNK: usize>(
  b: &mut Bencher<'_>,
  ring: &mut SpscRing<u64, N>,
) {
  let (mut producer, mut consumer) = ring.split();
  let src = [0u64; CHUNK];
  let mut dst: [MaybeUninit<u64>; CHUNK] = unsafe { MaybeUninit::uninit().assume_init() };
  b.iter_custom(|iters| {
    let mut total = Duration::ZERO;
    for _ in 0..iters {
      producer.enqueue_batch_copy(&src);
      let start = Instant::now();
      let d = consumer.dequeue_batch(&mut dst);
      total += start.elapsed();
      black_box(d.as_slice());
    }
    total
  });
}

fn payload<T: Payload, const N: usize, const CHUNK: usize>(
  b: &mut Bencher<'_>,
  ring: &mut SpscRing<T, N>,
) {
  let (mut producer, mut consumer) = ring.split();
  let src: [T; CHUNK] = array::from_fn(|i| T::seed(i as u64));
  let mut dst: [MaybeUninit<T>; CHUNK] = unsafe { MaybeUninit::uninit().assume_init() };
  b.iter_custom(|iters| {
    let mut total = Duration::ZERO;
    for _ in 0..iters {
      let start = Instant::now();
      producer.enqueue_batch_copy(&src);
      let d = consumer.dequeue_batch(&mut dst);
      total += start.elapsed();
      black_box(d.as_slice());
    }
    total
  });
}

fn run_enqueue<const N: usize>(group: &mut BenchmarkGroup<'_, WallTime>) {
  group.bench_function(BenchmarkId::new("stack", N), |b| {
    let mut ring = SpscRing::<u64, N>::new();
    enqueue(b, &mut ring);
  });
  group.bench_function(BenchmarkId::new("heap", N), |b| {
    let mut ring = Box::new(SpscRing::<u64, N>::new());
    enqueue(b, &mut ring);
  });
}

fn run_dequeue<const N: usize>(group: &mut BenchmarkGroup<'_, WallTime>) {
  group.bench_function(BenchmarkId::new("stack", N), |b| {
    let mut ring = SpscRing::<u64, N>::new();
    dequeue(b, &mut ring);
  });
  group.bench_function(BenchmarkId::new("heap", N), |b| {
    let mut ring = Box::new(SpscRing::<u64, N>::new());
    dequeue(b, &mut ring);
  });
}

fn run_enqueue_batch<const N: usize, const CHUNK: usize>(group: &mut BenchmarkGroup<'_, WallTime>) {
  group.throughput(Throughput::Elements(CHUNK as u64));
  group.bench_function(BenchmarkId::from_parameter(CHUNK), |b| {
    let mut ring = Box::new(SpscRing::<u64, N>::new());
    enqueue_batch::<N, CHUNK>(b, &mut ring);
  });
}

fn run_dequeue_batch<const N: usize, const CHUNK: usize>(group: &mut BenchmarkGroup<'_, WallTime>) {
  group.throughput(Throughput::Elements(CHUNK as u64));
  group.bench_function(BenchmarkId::from_parameter(CHUNK), |b| {
    let mut ring = Box::new(SpscRing::<u64, N>::new());
    dequeue_batch::<N, CHUNK>(b, &mut ring);
  });
}

fn run_payload<T: Payload, const N: usize, const CHUNK: usize>(
  group: &mut BenchmarkGroup<'_, WallTime>,
) {
  group.throughput(Throughput::Bytes((mem::size_of::<T>() * CHUNK) as u64));
  group.bench_function(BenchmarkId::from_parameter(T::LABEL), |b| {
    let mut ring = Box::new(SpscRing::<T, N>::new());
    payload::<T, N, CHUNK>(b, &mut ring);
  });
}

fn bench_enqueue(c: &mut Criterion) {
  let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
  let mut group = c.benchmark_group("enqueue");
  group.plot_config(plot_config);
  group.throughput(Throughput::Elements(1));
  run_enqueue::<2>(&mut group);
  run_enqueue::<4>(&mut group);
  run_enqueue::<8>(&mut group);
  run_enqueue::<16>(&mut group);
  run_enqueue::<64>(&mut group);
  run_enqueue::<256>(&mut group);
  run_enqueue::<1024>(&mut group);
  run_enqueue::<4096>(&mut group);
  run_enqueue::<16384>(&mut group);
  run_enqueue::<65536>(&mut group);
  group.finish();
}

fn bench_dequeue(c: &mut Criterion) {
  let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
  let mut group = c.benchmark_group("dequeue");
  group.plot_config(plot_config);
  group.throughput(Throughput::Elements(1));
  run_dequeue::<2>(&mut group);
  run_dequeue::<4>(&mut group);
  run_dequeue::<8>(&mut group);
  run_dequeue::<16>(&mut group);
  run_dequeue::<64>(&mut group);
  run_dequeue::<256>(&mut group);
  run_dequeue::<1024>(&mut group);
  run_dequeue::<4096>(&mut group);
  run_dequeue::<16384>(&mut group);
  run_dequeue::<65536>(&mut group);
  group.finish();
}

fn bench_enqueue_batch(c: &mut Criterion) {
  let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
  let mut group = c.benchmark_group("enqueue_batch");
  group.plot_config(plot_config);
  run_enqueue_batch::<8192, 1>(&mut group);
  run_enqueue_batch::<8192, 8>(&mut group);
  run_enqueue_batch::<8192, 64>(&mut group);
  run_enqueue_batch::<8192, 512>(&mut group);
  run_enqueue_batch::<8192, 4096>(&mut group);
  group.finish();
}

fn bench_dequeue_batch(c: &mut Criterion) {
  let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
  let mut group = c.benchmark_group("dequeue_batch");
  group.plot_config(plot_config);
  run_dequeue_batch::<8192, 1>(&mut group);
  run_dequeue_batch::<8192, 8>(&mut group);
  run_dequeue_batch::<8192, 64>(&mut group);
  run_dequeue_batch::<8192, 512>(&mut group);
  run_dequeue_batch::<8192, 4096>(&mut group);
  group.finish();
}

fn bench_payload(c: &mut Criterion) {
  let mut group = c.benchmark_group("payload");
  run_payload::<u8, 1024, 256>(&mut group);
  run_payload::<u64, 1024, 256>(&mut group);
  run_payload::<[u8; 64], 1024, 256>(&mut group);
  run_payload::<[u8; 256], 1024, 256>(&mut group);
  group.finish();
}

criterion_group!(
  benches,
  bench_enqueue,
  bench_dequeue,
  bench_enqueue_batch,
  bench_dequeue_batch,
  bench_payload
);
criterion_main!(benches);
