use std::hint::{black_box, spin_loop};
use std::mem::MaybeUninit;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use criterion::measurement::WallTime;
use criterion::{
  AxisScale, Bencher, BenchmarkGroup, BenchmarkId, Criterion, PlotConfiguration, Throughput,
  criterion_group, criterion_main,
};
use lib_spsc_ring::SpscRing;

fn stream<const N: usize>(b: &mut Bencher<'_>, ring: &mut SpscRing<u64, N>) {
  b.iter_custom(|iters| {
    {
      let (_producer, mut consumer) = ring.split();
      while consumer.dequeue().is_ok() {}
    }
    let (mut producer, mut consumer) = ring.split();
    let prefill = (N / 2).min(N - 1);
    for i in 0..prefill {
      producer.enqueue(black_box(i as u64)).unwrap();
    }
    let barrier = Arc::new(Barrier::new(3));
    thread::scope(|s| {
      let p_barrier = Arc::clone(&barrier);
      let c_barrier = Arc::clone(&barrier);
      let producer_thread = s.spawn(move || {
        p_barrier.wait();
        for i in 0..iters {
          let mut value = black_box(i);
          loop {
            match producer.enqueue(value) {
              Ok(()) => break,
              Err(v) => {
                value = v;
                spin_loop();
              }
            }
          }
        }
      });
      let consumer_thread = s.spawn(move || {
        c_barrier.wait();
        for _ in 0..iters {
          loop {
            match consumer.dequeue() {
              Ok(value) => {
                black_box(value);
                break;
              }
              Err(_) => spin_loop(),
            }
          }
        }
      });
      barrier.wait();
      let start = Instant::now();
      producer_thread.join().unwrap();
      consumer_thread.join().unwrap();
      start.elapsed()
    })
  });
}

fn stream_batch<const N: usize>(b: &mut Bencher<'_>, ring: &mut SpscRing<u64, N>) {
  const CHUNK: usize = 64;
  b.iter_custom(|iters| {
    let (mut producer, mut consumer) = ring.split();
    let barrier = Arc::new(Barrier::new(3));
    thread::scope(|s| {
      let p_barrier = Arc::clone(&barrier);
      let c_barrier = Arc::clone(&barrier);
      let producer_thread = s.spawn(move || {
        p_barrier.wait();
        let src = [0u64; CHUNK];
        let mut sent = 0u64;
        while sent < iters {
          let want = ((iters - sent) as usize).min(CHUNK);
          let mut off = 0;
          while off < want {
            let n = producer.enqueue_batch_copy(&src[off..want]);
            if n == 0 {
              spin_loop();
            } else {
              off += n;
            }
          }
          sent += want as u64;
        }
      });
      let consumer_thread = s.spawn(move || {
        c_barrier.wait();
        let mut dst: [MaybeUninit<u64>; CHUNK] = unsafe { MaybeUninit::uninit().assume_init() };
        let mut recv = 0u64;
        while recv < iters {
          let d = consumer.dequeue_batch(&mut dst);
          if d.is_empty() {
            spin_loop();
          } else {
            recv += d.len() as u64;
            black_box(d.as_slice());
          }
        }
      });
      barrier.wait();
      let start = Instant::now();
      producer_thread.join().unwrap();
      consumer_thread.join().unwrap();
      start.elapsed()
    })
  });
}

fn round_trip<const N: usize>(
  b: &mut Bencher<'_>,
  req: &mut SpscRing<u64, N>,
  resp: &mut SpscRing<u64, N>,
) {
  b.iter_custom(|iters| {
    let (mut req_tx, mut req_rx) = req.split();
    let (mut resp_tx, mut resp_rx) = resp.split();
    let barrier = Arc::new(Barrier::new(2));
    thread::scope(|s| {
      let e_barrier = Arc::clone(&barrier);
      let echo = s.spawn(move || {
        e_barrier.wait();
        for _ in 0..iters {
          let value = loop {
            match req_rx.dequeue() {
              Ok(value) => break value,
              Err(_) => spin_loop(),
            }
          };
          loop {
            match resp_tx.enqueue(value) {
              Ok(()) => break,
              Err(_) => spin_loop(),
            }
          }
        }
      });
      barrier.wait();
      let start = Instant::now();
      for i in 0..iters {
        let mut value = i;
        loop {
          match req_tx.enqueue(value) {
            Ok(()) => break,
            Err(v) => {
              value = v;
              spin_loop();
            }
          }
        }
        let got = loop {
          match resp_rx.dequeue() {
            Ok(value) => break value,
            Err(_) => spin_loop(),
          }
        };
        black_box(got);
      }
      let elapsed = start.elapsed();
      echo.join().unwrap();
      elapsed
    })
  });
}

fn run_stream<const N: usize>(group: &mut BenchmarkGroup<'_, WallTime>) {
  group.bench_function(BenchmarkId::new("stack", N), |b| {
    let mut ring = SpscRing::<u64, N>::new();
    stream(b, &mut ring);
  });
  group.bench_function(BenchmarkId::new("heap", N), |b| {
    let mut ring = Box::new(SpscRing::<u64, N>::new());
    stream(b, &mut ring);
  });
}

fn run_stream_batch<const N: usize>(group: &mut BenchmarkGroup<'_, WallTime>) {
  group.bench_function(BenchmarkId::from_parameter(N), |b| {
    let mut ring = Box::new(SpscRing::<u64, N>::new());
    stream_batch(b, &mut ring);
  });
}

fn run_round_trip<const N: usize>(group: &mut BenchmarkGroup<'_, WallTime>) {
  group.bench_function(BenchmarkId::from_parameter(N), |b| {
    let mut req = Box::new(SpscRing::<u64, N>::new());
    let mut resp = Box::new(SpscRing::<u64, N>::new());
    round_trip(b, &mut req, &mut resp);
  });
}

fn bench_stream(c: &mut Criterion) {
  let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
  let mut group = c.benchmark_group("stream");
  group.plot_config(plot_config);
  group.sample_size(10);
  group.measurement_time(Duration::from_secs(10));
  group.throughput(Throughput::Elements(1));
  run_stream::<2>(&mut group);
  run_stream::<4>(&mut group);
  run_stream::<8>(&mut group);
  run_stream::<16>(&mut group);
  run_stream::<64>(&mut group);
  run_stream::<256>(&mut group);
  run_stream::<1024>(&mut group);
  run_stream::<4096>(&mut group);
  run_stream::<16384>(&mut group);
  run_stream::<65536>(&mut group);
  group.finish();
}

fn bench_stream_batch(c: &mut Criterion) {
  let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
  let mut group = c.benchmark_group("stream_batch");
  group.plot_config(plot_config);
  group.sample_size(10);
  group.measurement_time(Duration::from_secs(10));
  group.throughput(Throughput::Elements(1));
  run_stream_batch::<64>(&mut group);
  run_stream_batch::<256>(&mut group);
  run_stream_batch::<1024>(&mut group);
  run_stream_batch::<4096>(&mut group);
  run_stream_batch::<16384>(&mut group);
  run_stream_batch::<65536>(&mut group);
  group.finish();
}

fn bench_round_trip(c: &mut Criterion) {
  let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
  let mut group = c.benchmark_group("round_trip");
  group.plot_config(plot_config);
  group.sample_size(10);
  group.measurement_time(Duration::from_secs(10));
  run_round_trip::<2>(&mut group);
  run_round_trip::<4>(&mut group);
  run_round_trip::<16>(&mut group);
  run_round_trip::<64>(&mut group);
  run_round_trip::<256>(&mut group);
  group.finish();
}

criterion_group!(benches, bench_stream, bench_stream_batch, bench_round_trip);
criterion_main!(benches);
