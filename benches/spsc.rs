use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use criterion::measurement::WallTime;
use criterion::{
  AxisScale, BenchmarkGroup, BenchmarkId, Criterion, PlotConfiguration, black_box, criterion_group,
  criterion_main,
};
use lib_spsc_ring::SpscRing;

fn run_enqueue<const N: usize>(group: &mut BenchmarkGroup<'_, WallTime>) {
  let ring: &'static mut SpscRing<u32, N> = Box::leak(Box::new(SpscRing::new()));
  let (mut producer, mut consumer) = ring.split();

  let stop = Arc::new(AtomicBool::new(false));
  let stop_drainer = stop.clone();
  let drainer = thread::spawn(move || {
    while !stop_drainer.load(Ordering::Relaxed) {
      let _ = consumer.dequeue();
    }
  });

  group.bench_function(BenchmarkId::from_parameter(N), |b| {
    b.iter(|| producer.enqueue(black_box(0u32)));
  });

  stop.store(true, Ordering::Relaxed);
  drainer.join().unwrap();
}

fn run_dequeue<const N: usize>(group: &mut BenchmarkGroup<'_, WallTime>) {
  let ring: &'static mut SpscRing<u32, N> = Box::leak(Box::new(SpscRing::new()));
  let (mut producer, mut consumer) = ring.split();

  let stop = Arc::new(AtomicBool::new(false));
  let stop_filler = stop.clone();
  let filler = thread::spawn(move || {
    while !stop_filler.load(Ordering::Relaxed) {
      let _ = producer.enqueue(0u32);
    }
  });

  group.bench_function(BenchmarkId::from_parameter(N), |b| {
    b.iter(|| consumer.dequeue());
  });

  stop.store(true, Ordering::Relaxed);
  filler.join().unwrap();
}

fn bench_enqueue(c: &mut Criterion) {
  let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
  let mut group = c.benchmark_group("enqueue");
  group.plot_config(plot_config);
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

criterion_group!(benches, bench_enqueue, bench_dequeue);
criterion_main!(benches);
