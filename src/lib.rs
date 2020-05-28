//! Branch an asynchronous stream into two, pushing all items to both halves.
//!
//! The resulting branches will can be polled independently from each other and will
//! receive all items from the underlying stream (which must be `Clone`).
//!
//! As long as both halves are alive, one half will never outpace the other by more
//! than a fixed number of items.
//!
//! This library is runtime agnostic. It is verified to work on both `async_std`
//! and `tokio`.
//!
//! # Example
//!
//! ```rust
//! # #[tokio::main]
//! # async fn main() {
//! use futures::{stream, prelude::*};
//!
//! let (mut left, mut right) = gabelung::new(stream::repeat(1u8));
//!
//! assert_eq!(left.next().await, Some(1u8));
//! assert_eq!(right.next().await, Some(1u8));
//! # }
//! ```

#![deny(missing_docs)]

use futures_util::{
    ready,
    stream::{FusedStream, Stream},
};
use parking_lot::Mutex;
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

/// A branch of the forked stream.
///
/// As long as both halves are alive, one half will never outpace the other by more
/// than a fixed number of items.
///
/// See [`fn new(stream)`](fn.new.html) for more information.
#[must_use = "streams do nothing unless polled"]
#[derive(Debug)]
pub struct Branch<S, I> {
    direction: Direction,
    inner: Arc<Mutex<Inner<S, I>>>,
}

#[derive(Debug)]
struct Inner<S, I> {
    left: State<I>,
    right: State<I>,
    stream: S,
}

#[derive(Debug)]
enum State<I> {
    Live(Option<I>, Option<Waker>),
    Dropped,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Direction {
    Left,
    Right,
}

/// Branch the given stream into two.
///
/// This creates two handles which can be polled independently from each other and
/// will receive all items from the underlying stream (which must be `Clone`).
///
/// As long as both halves are alive, one half will never outpace the other by more
/// than a fixed number of items.
///
/// # Example
///
/// ```rust
/// # #[tokio::main]
/// # async fn main() {
/// use futures::{stream, prelude::*};
///
/// let (mut left, mut right) = gabelung::new(stream::repeat(1u8));
///
/// assert_eq!(left.next().await, Some(1u8));
/// assert_eq!(right.next().await, Some(1u8));
/// # }
/// ```
pub fn new<S: Stream>(stream: S) -> (Branch<S, S::Item>, Branch<S, S::Item>) {
    let inner = Arc::new(Mutex::new(Inner {
        left: State::Live(None, None),
        right: State::Live(None, None),
        stream,
    }));

    let left = Branch {
        direction: Direction::Left,
        inner: inner.clone(),
    };
    let right = Branch {
        direction: Direction::Right,
        inner: inner,
    };

    (left, right)
}

impl<S, I> Drop for Branch<S, I> {
    fn drop(&mut self) {
        let mut inner = self.inner.lock();
        let Inner { left, right, .. } = &mut *inner;

        let (own_state, other_state) = match self.direction {
            Direction::Left => (left, right),
            Direction::Right => (right, left),
        };

        *own_state = State::Dropped;

        // Wake up the other half to hand off the responsibility for polling.
        //
        // If this half was the last one to poll the underlying stream and received
        // Poll::Pending, the other half is waiting for us to wake to take the next
        // action. If we're being dropped though, no further action will be taken
        // and the other half needs to be notified about that.
        other_state.wake();
    }
}

impl<S> Stream for Branch<S, S::Item>
where
    S: Stream,
    S::Item: Clone,
{
    type Item = S::Item;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<Self::Item>> {
        let mut inner = self.inner.lock();
        let Inner {
            left,
            right,
            stream,
        } = &mut *inner;

        // SAFETY: From my limited understanding this should be safe. The code never
        // moves the stream, and since it is behind Arc<Mutex> it should also never
        // move on its own.
        let stream = unsafe { Pin::new_unchecked(stream) };
        let (own_branch, other_branch) = match self.direction {
            Direction::Left => (left, right),
            Direction::Right => (right, left),
        };

        own_branch.ensure_waker(cx);

        match own_branch.take_item() {
            Some(it) => {
                // Wake up other branch since we have progressed and it's free to
                // move further now.
                other_branch.wake();

                return Poll::Ready(Some(it));
            }
            None => {
                // Other branch may still have to consume its item, wait for that to
                // happen until progressing any further. The other branch will wake
                // us up.
                if other_branch.has_item() {
                    return Poll::Pending;
                }

                match ready!(stream.poll_next(cx)) {
                    Some(it) => {
                        other_branch.put_item(&it);
                        other_branch.wake();

                        return Poll::Ready(Some(it));
                    }
                    None => {
                        return Poll::Ready(None);
                    }
                }
            }
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let inner = self.inner.lock();
        let own_state = match self.direction {
            Direction::Left => &inner.left,
            Direction::Right => &inner.right,
        };

        let (inner_low, inner_high) = inner.stream.size_hint();
        let adjust_for_self = if own_state.has_item() { 1 } else { 0 };

        (inner_low + adjust_for_self, inner_high.map(|c| c + adjust_for_self))
    }
}

impl<S> FusedStream for Branch<S, S::Item>
where
    S: FusedStream,
    S::Item: Clone,
{
    fn is_terminated(&self) -> bool {
        let inner = self.inner.lock();
        let own_state = match self.direction {
            Direction::Left => &inner.left,
            Direction::Right => &inner.right,
        };

        !own_state.has_item() && inner.stream.is_terminated()
    }
}

// The stream itself is behind the Arc, so it won't move if the branch is moved
impl<S, I> Unpin for Branch<S, I> {}

impl<I> State<I> {
    pub fn ensure_waker(&mut self, cx: &Context<'_>) {
        match self {
            State::Live(_, waker) => {
                if waker.is_none() {
                    *waker = Some(cx.waker().clone());
                }
            }
            State::Dropped => unreachable!("poll on dropped branch half"),
        }
    }

    pub fn has_item(&self) -> bool {
        match self {
            State::Live(Some(_), _) => true,
            _ => false,
        }
    }

    pub fn put_item(&mut self, item: &I)
    where
        I: Clone,
    {
        match self {
            State::Live(it @ None, _) => *it = Some(item.clone()),
            State::Live(Some(_), _) => panic!("overwriting branch item"),
            _ => {}
        }
    }

    pub fn take_item(&mut self) -> Option<I> {
        match self {
            State::Live(it, _) => it.take(),
            _ => None,
        }
    }

    pub fn wake(&self) {
        if let State::Live(_, Some(w)) = self {
            w.wake_by_ref();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::prelude::*;

    fn get_stream() -> (
        Branch<stream::Repeat<()>, ()>,
        Branch<stream::Repeat<()>, ()>,
    ) {
        let base = stream::repeat(());
        crate::new(base)
    }

    fn branch_multiple(cx: &mut Context<'_>) -> Poll<()> {
        let (left, right) = get_stream();
        let (a_l, a_r) = crate::new(left);
        let (b_l, b_r) = crate::new(right);

        let mut a_l = Box::pin(a_l);
        let mut a_r = Box::pin(a_r);
        let mut b_l = Box::pin(b_l);
        let mut b_r = Box::pin(b_r);

        assert_eq!(a_l.as_mut().poll_next(cx), Poll::Ready(Some(())));
        assert_eq!(a_r.as_mut().poll_next(cx), Poll::Ready(Some(())));
        assert_eq!(b_l.as_mut().poll_next(cx), Poll::Ready(Some(())));
        assert_eq!(b_r.as_mut().poll_next(cx), Poll::Ready(Some(())));
        assert_eq!(b_r.as_mut().poll_next(cx), Poll::Ready(Some(())));
        assert_eq!(b_r.as_mut().poll_next(cx), Poll::Pending);

        Poll::Ready(())
    }

    fn drop_one_half(cx: &mut Context<'_>) -> Poll<()> {
        let (left, right) = get_stream();
        let mut left = Box::pin(left);
        drop(right);

        assert_eq!(left.as_mut().poll_next(cx), Poll::Ready(Some(())));
        assert_eq!(left.as_mut().poll_next(cx), Poll::Ready(Some(())));
        assert_eq!(left.as_mut().poll_next(cx), Poll::Ready(Some(())));
        assert_eq!(left.as_mut().poll_next(cx), Poll::Ready(Some(())));
        assert_eq!(left.as_mut().poll_next(cx), Poll::Ready(Some(())));

        Poll::Ready(())
    }

    fn lockstep(cx: &mut Context<'_>) -> Poll<()> {
        let (left, right) = get_stream();
        let mut left = Box::pin(left);
        let mut right = Box::pin(right);

        assert_eq!(left.as_mut().poll_next(cx), Poll::Ready(Some(())));
        assert_eq!(right.as_mut().poll_next(cx), Poll::Ready(Some(())));
        assert_eq!(left.as_mut().poll_next(cx), Poll::Ready(Some(())));
        assert_eq!(right.as_mut().poll_next(cx), Poll::Ready(Some(())));
        assert_eq!(left.as_mut().poll_next(cx), Poll::Ready(Some(())));
        assert_eq!(right.as_mut().poll_next(cx), Poll::Ready(Some(())));
        assert_eq!(left.as_mut().poll_next(cx), Poll::Ready(Some(())));
        assert_eq!(right.as_mut().poll_next(cx), Poll::Ready(Some(())));

        Poll::Ready(())
    }

    fn terminates(cx: &mut Context<'_>) -> Poll<()> {
        let (left, right) = crate::new(stream::once(future::ready(())));

        let mut left = Box::pin(left);
        let mut right = Box::pin(right);

        assert_eq!(left.as_mut().poll_next(cx), Poll::Ready(Some(())));
        assert_eq!(right.as_mut().poll_next(cx), Poll::Ready(Some(())));
        assert_eq!(right.as_mut().poll_next(cx), Poll::Ready(None));
        assert_eq!(left.as_mut().poll_next(cx), Poll::Ready(None));
        assert_eq!(right.as_mut().poll_next(cx), Poll::Ready(None));
        assert_eq!(left.as_mut().poll_next(cx), Poll::Ready(None));

        Poll::Ready(())
    }

    fn waits_for_other(cx: &mut Context<'_>) -> Poll<()> {
        let (left, right) = get_stream();
        let mut left = Box::pin(left);
        let mut right = Box::pin(right);

        assert_eq!(left.as_mut().poll_next(cx), Poll::Ready(Some(())));
        assert_eq!(left.as_mut().poll_next(cx), Poll::Pending);
        assert_eq!(right.as_mut().poll_next(cx), Poll::Ready(Some(())));
        assert_eq!(right.as_mut().poll_next(cx), Poll::Ready(Some(())));
        assert_eq!(right.as_mut().poll_next(cx), Poll::Pending);
        assert_eq!(left.as_mut().poll_next(cx), Poll::Ready(Some(())));
        assert_eq!(left.as_mut().poll_next(cx), Poll::Ready(Some(())));
        assert_eq!(left.as_mut().poll_next(cx), Poll::Pending);

        Poll::Ready(())
    }

    async fn pull_items(s: impl Stream<Item = ()>, amt: usize) {
        s.take(amt).collect().await
    }

    mod a_std {
        use futures::{future, join};

        #[async_std::test]
        async fn branch_multiple() {
            future::poll_fn(super::branch_multiple).await;
        }

        #[async_std::test]
        async fn drop_one_half() {
            future::poll_fn(super::drop_one_half).await;
        }

        #[async_std::test]
        async fn lockstep() {
            future::poll_fn(super::lockstep).await;
        }

        #[async_std::test]
        async fn terminates() {
            future::poll_fn(super::terminates).await;
        }

        #[async_std::test]
        async fn waits_for_other() {
            future::poll_fn(super::waits_for_other).await;
        }

        #[async_std::test]
        async fn wakeup() {
            let (a, b) = super::get_stream();

            let fut_a = async_std::task::spawn(super::pull_items(a, 10));
            let fut_b = async_std::task::spawn(super::pull_items(b, 10));

            join!(fut_a, fut_b);
        }

        #[tokio::test]
        async fn wakeup_after_drop() {
            let (a, b) = super::get_stream();

            let fut_a = async_std::task::spawn(super::pull_items(a, 10));
            let fut_b = async_std::task::spawn(super::pull_items(b, 20));

            join!(fut_a, fut_b);
        }
    }

    mod tk {
        use futures::{future, join};

        #[tokio::test]
        async fn branch_multiple() {
            future::poll_fn(super::branch_multiple).await;
        }

        #[tokio::test]
        async fn drop_one_half() {
            future::poll_fn(super::drop_one_half).await;
        }

        #[tokio::test]
        async fn lockstep() {
            future::poll_fn(super::lockstep).await;
        }

        #[tokio::test]
        async fn terminates() {
            future::poll_fn(super::terminates).await;
        }

        #[tokio::test]
        async fn waits_for_other() {
            future::poll_fn(super::waits_for_other).await;
        }

        #[tokio::test]
        async fn wakeup() {
            let (a, b) = super::get_stream();

            let fut_a = tokio::spawn(super::pull_items(a, 10));
            let fut_b = tokio::spawn(super::pull_items(b, 10));

            let (a_res, b_res) = join!(fut_a, fut_b);
            a_res.and(b_res).expect("failed to spawn");
        }

        #[tokio::test]
        async fn wakeup_after_drop() {
            let (a, b) = super::get_stream();

            let fut_a = tokio::spawn(super::pull_items(a, 10));
            let fut_b = tokio::spawn(super::pull_items(b, 20));

            let (a_res, b_res) = join!(fut_a, fut_b);
            a_res.and(b_res).expect("failed to spawn");
        }
    }
}
