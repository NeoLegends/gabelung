[![crates.io](https://img.shields.io/crates/v/gabelung.svg)](https://crates.io/crates/gabelung)

# gabelung

Branch an asynchronous stream into two, pushing all items to both halves.

The resulting branches can be polled independently from each other and will
receive all items from the underlying stream (which must be `Clone`).

As long as both halves are alive, one half will never outpace the other by more
than a fixed number of items.

The goal of this library is to obsolete itself by integration with `futures-rs` (pending [issue](https://github.com/rust-lang/futures-rs/issues/2166)).
It does not depend on executor features and thus is runtime agnostic. It is 
verified to work on both `async_std` and `tokio`.

## Example

```rust
use futures::{stream, prelude::*};

let (mut left, mut right) = gabelung::new(stream::repeat(1u8));

assert_eq!(left.next().await, Some(1u8));
assert_eq!(right.next().await, Some(1u8));
```

## License

MIT
