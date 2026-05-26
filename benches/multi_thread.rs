use std::hint::{black_box, spin_loop};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use criterion::measurement::WallTime;
use criterion::{
  AxisScale, BenchmarkGroup, BenchmarkId, Criterion, PlotConfiguration, criterion_group,
  criterion_main,
};
use lib_spsc_ring::SpscRing;

fn run_stream<const N: usize>(group: &mut BenchmarkGroup<'_, WallTime>) {
  group.bench_function(BenchmarkId::from_parameter(N), |b| {
    b.iter_custom(|iters| {
      let ring: &'static mut SpscRing<u64, N> = Box::leak(Box::new(SpscRing::new()));
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
  });
}

fn bench_stream(c: &mut Criterion) {
  let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);
  let mut group = c.benchmark_group("multi_thread_stream");
  group.plot_config(plot_config);
  group.sample_size(10);
  group.measurement_time(std::time::Duration::from_secs(10));

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

criterion_group!(benches, bench_stream);
criterion_main!(benches);
