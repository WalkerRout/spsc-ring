use std::time::{Duration, Instant};

use criterion::measurement::WallTime;
use criterion::{
  AxisScale, BenchmarkGroup, BenchmarkId, Criterion, PlotConfiguration, criterion_group,
  criterion_main,
};
use lib_spsc_ring::SpscRing;

fn run_enqueue<const N: usize>(group: &mut BenchmarkGroup<'_, WallTime>) {
  group.bench_function(BenchmarkId::from_parameter(N), |b| {
    let ring: &'static mut SpscRing<u32, N> = Box::leak(Box::new(SpscRing::new()));
    let (mut producer, mut consumer) = ring.split();
    let cap = (N - 1) as u64;

    b.iter_custom(|iters| {
      let mut total = Duration::ZERO;
      let mut done = 0u64;
      while done < iters {
        let chunk = cap.min(iters - done);

        let start = Instant::now();
        for _ in 0..chunk {
          let _ = producer.enqueue(0u32);
        }
        total += start.elapsed();
        done += chunk;

        while consumer.dequeue().is_ok() {}
      }
      total
    });
  });
}

fn run_dequeue<const N: usize>(group: &mut BenchmarkGroup<'_, WallTime>) {
  group.bench_function(BenchmarkId::from_parameter(N), |b| {
    let ring: &'static mut SpscRing<u32, N> = Box::leak(Box::new(SpscRing::new()));
    let (mut producer, mut consumer) = ring.split();
    let cap = (N - 1) as u64;

    b.iter_custom(|iters| {
      let mut total = Duration::ZERO;
      let mut done = 0u64;
      while done < iters {
        while producer.enqueue(0u32).is_ok() {}

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
  });
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
