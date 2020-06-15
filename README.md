[![crates.io](https://img.shields.io/crates/v/gabelung.svg)](https://crates.io/crates/gabelung)

# gabelung

Branch an asynchronous stream into two, pushing all items to both halves.

The resulting branches can be polled independently from each other and will
receive all items from the underlying stream (which must be `Clone`).

As long as both halves are alive, one half will never outpace the other by more
than a fixed number of items.

This library is runtime agnostic. It is verified to work on both `async_std`
and `tokio`.

## Example

```rust
use futures::{stream, prelude::*};

let (mut left, mut right) = gabelung::new(stream::repeat(1u8));

assert_eq!(left.next().await, Some(1u8));
assert_eq!(right.next().await, Some(1u8));
```

## License

MIT
