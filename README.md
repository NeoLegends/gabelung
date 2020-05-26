# gabelung

Branch an asynchronous stream of cloneable items into two halfs that yield
the items in lockstep.

As long as both branches are alive, one can never outpace the other by more than
a fixed number of items.

This library is runtime agnostic. Nonetheless it is tested on both `async_std`
and `tokio`.

## Example

```rust
use futures::{stream, prelude::*};
use gabelung::Branch;

let (mut left, mut right) = Branch::new(stream::repeat(1u8));

assert_eq!(left.next().await, Some(1u8));
assert_eq!(right.next().await, Some(1u8));
```

## License

MIT
