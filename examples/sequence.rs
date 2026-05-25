use std::thread;

use lib_spsc_ring::SpscRing;

pub fn main() {
  let mut ring = SpscRing::<u32, 16>::new();
  let (mut producer, mut consumer) = ring.split();

  const COUNT: u32 = 1_000_000;
  thread::scope(|s| {
    // producer
    s.spawn(move || {
      let mut sent = 0u32;
      while sent < COUNT {
        if producer.enqueue(sent).is_ok() {
          sent += 1;
        } else {
          std::hint::spin_loop();
        }
      }
    });
    // consumer
    s.spawn(move || {
      let mut expected = 0u32;
      while expected < COUNT {
        match consumer.dequeue() {
          Ok(v) => {
            assert_eq!(v, expected, "ordering violated");
            expected += 1;
          }
          Err(_) => std::hint::spin_loop(),
        }
      }
      println!("consumed {} items in order", expected);
    });
  });
}
