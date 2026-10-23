//! Verify queue-full admission itself allocates nothing after initialization.
use natslink::{Bytes, Error, Options, Publisher, PublisherOptions, ReportInterval};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! { static ALLOCATIONS: Cell<Option<usize>> = const { Cell::new(None) }; }
struct Counted;
unsafe impl GlobalAlloc for Counted {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|count| {
            if let Some(n) = count.get() {
                count.set(Some(n + 1));
            }
        });
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        System.dealloc(pointer, layout);
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let _ = ALLOCATIONS.try_with(|count| {
            if let Some(n) = count.get() {
                count.set(Some(n + 1));
            }
        });
        System.realloc(pointer, layout, size)
    }
}
#[global_allocator]
static ALLOCATOR: Counted = Counted;

#[tokio::test]
async fn rejecting_a_full_publish_queue_has_no_heap_allocation() {
    let stalled = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let publisher = Publisher::new(PublisherOptions {
        options: Options {
            url: format!("nats://{}", stalled.local_addr().unwrap()),
            topic: "events.allocation".into(),
            report_interval: ReportInterval::Off,
            ..Default::default()
        },
        queue_capacity: 1,
        max_payload: 0,
    })
    .unwrap();
    publisher.start().await.unwrap();
    publisher.publish(Bytes::from_static(b"occupied")).unwrap();
    // No await in the measured loop: the current-thread runtime cannot drain
    // the full command queue or charge unrelated tasks to this allocation count.
    for _ in 0..10 {
        assert!(matches!(
            publisher.publish(Bytes::new()),
            Err(Error::BufferFull(1))
        ));
    }
    ALLOCATIONS.with(|count| count.set(Some(0)));
    for _ in 0..1000 {
        assert!(matches!(
            publisher.publish(Bytes::new()),
            Err(Error::BufferFull(1))
        ));
    }
    let count = ALLOCATIONS.with(|count| count.replace(None).unwrap());
    assert_eq!(
        count, 0,
        "queue-full admission must not clone Client or allocate a subject"
    );
    publisher.stop().await;
}
