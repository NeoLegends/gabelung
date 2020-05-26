//! Branch an asynchronous stream of cloneable items into two halfs that yield
//! the items in lockstep.
//!
//! As long as both branches are alive, one can never outpace the other by more than
//! a fixed number of items.
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

use futures_util::{ready, stream::Stream};
use parking_lot::Mutex;
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

#[derive(Debug)]
pub struct Branch<S, I> {
    direction: Direction,
    inner: Arc<Mutex<Inner<S, I>>>,
}

#[derive(Debug)]
struct Inner<S, I> {
    left: Option<State<I>>,
    right: Option<State<I>>,
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

pub fn new<S: Stream>(stream: S) -> (Branch<S, S::Item>, Branch<S, S::Item>) {
    let inner = Arc::new(Mutex::new(Inner {
        left: None,
        right: None,
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

        *own_state = Some(State::Dropped);

        // Wake up the other half to hand off the responsibility for polling.
        //
        // If this half was the last one to poll the underlying stream and received
        // Poll::Pending, the other half is waiting for us to wake to take the next
        // action. If we're being dropped though, no further action will be taken
        // and the other half needs to be notified about that.
        if let Some(State::Live(_, Some(waker))) = other_state {
            waker.wake_by_ref();
        }
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

        // SAFETY: This may or may not be safe. I think it's safe but I honestly
        // don't know. Since the stream is behind Arc<Mutex> I think it will never
        // move on its own.
        let stream = unsafe { Pin::new_unchecked(stream) };
        let (own_state, other_state) = match self.direction {
            Direction::Left => (left, right),
            Direction::Right => (right, left),
        };

        loop {
            match own_state.take() {
                Some(State::Live(Some(it), Some(waker))) => {
                    *own_state = Some(State::Live(None, Some(waker)));

                    // Wake up other branch since we have progressed and it's free
                    // to move further now.
                    if let Some(State::Live(_, Some(w))) = &*other_state {
                        w.wake_by_ref();
                    }

                    return Poll::Ready(Some(it));
                }
                Some(State::Live(None, Some(waker))) => {
                    *own_state = Some(State::Live(None, Some(waker)));

                    // Other branch still has to consume its item, wait for that to
                    // happen until progressing any further
                    if let Some(State::Live(Some(_), _)) = &*other_state {
                        return Poll::Pending;
                    }

                    match ready!(stream.poll_next(cx)) {
                        Some(it) => {
                            match other_state {
                                Some(State::Live(item @ None, waker)) => {
                                    *item = Some(it.clone());

                                    // Wake the other half up, if possible. If the
                                    // other half has not been polled yet (and thus
                                    // it's waker is None) it will fetch the item
                                    // on its first .poll_next().
                                    if let Some(w) = waker {
                                        w.wake_by_ref();
                                    }
                                }
                                Some(State::Live(Some(_), _)) => {
                                    // checked for above before polling stream
                                    unreachable!()
                                }
                                Some(State::Dropped) => {}
                                None => {
                                    *other_state = Some(State::Live(
                                        Some(it.clone()),
                                        None,
                                    ))
                                }
                            }

                            return Poll::Ready(Some(it));
                        }
                        None => {
                            return Poll::Ready(None);
                        }
                    }
                }
                Some(State::Live(item, None)) => {
                    *own_state =
                        Some(State::Live(item, Some(cx.waker().clone())));
                }
                None => {
                    *own_state =
                        Some(State::Live(None, Some(cx.waker().clone())));
                }
                Some(State::Dropped) => {
                    unreachable!("poll on dropped branch half");
                }
            }
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.lock().stream.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::prelude::*;

    pub fn get_stream() -> (
        Branch<stream::Repeat<u8>, u8>,
        Branch<stream::Repeat<u8>, u8>,
    ) {
        let base = stream::repeat(0u8);
        crate::new(base)
    }

    pub fn drop_one_half(cx: &mut Context<'_>) -> Poll<()> {
        let (left, right) = get_stream();
        let mut left = Box::pin(left);
        drop(right);

        assert_eq!(left.as_mut().poll_next(cx), Poll::Ready(Some(0)));
        assert_eq!(left.as_mut().poll_next(cx), Poll::Ready(Some(0)));
        assert_eq!(left.as_mut().poll_next(cx), Poll::Ready(Some(0)));
        assert_eq!(left.as_mut().poll_next(cx), Poll::Ready(Some(0)));
        assert_eq!(left.as_mut().poll_next(cx), Poll::Ready(Some(0)));

        Poll::Ready(())
    }

    pub fn lockstep(cx: &mut Context<'_>) -> Poll<()> {
        let (left, right) = get_stream();
        let mut left = Box::pin(left);
        let mut right = Box::pin(right);

        assert_eq!(left.as_mut().poll_next(cx), Poll::Ready(Some(0)));
        assert_eq!(right.as_mut().poll_next(cx), Poll::Ready(Some(0)));
        assert_eq!(left.as_mut().poll_next(cx), Poll::Ready(Some(0)));
        assert_eq!(right.as_mut().poll_next(cx), Poll::Ready(Some(0)));
        assert_eq!(left.as_mut().poll_next(cx), Poll::Ready(Some(0)));
        assert_eq!(right.as_mut().poll_next(cx), Poll::Ready(Some(0)));
        assert_eq!(left.as_mut().poll_next(cx), Poll::Ready(Some(0)));
        assert_eq!(right.as_mut().poll_next(cx), Poll::Ready(Some(0)));

        Poll::Ready(())
    }

    pub fn waits_for_other(cx: &mut Context<'_>) -> Poll<()> {
        let (left, right) = get_stream();
        let mut left = Box::pin(left);
        let mut right = Box::pin(right);

        assert_eq!(left.as_mut().poll_next(cx), Poll::Ready(Some(0)));
        assert_eq!(left.as_mut().poll_next(cx), Poll::Pending);
        assert_eq!(right.as_mut().poll_next(cx), Poll::Ready(Some(0)));
        assert_eq!(right.as_mut().poll_next(cx), Poll::Ready(Some(0)));
        assert_eq!(right.as_mut().poll_next(cx), Poll::Pending);
        assert_eq!(left.as_mut().poll_next(cx), Poll::Ready(Some(0)));
        assert_eq!(left.as_mut().poll_next(cx), Poll::Ready(Some(0)));
        assert_eq!(left.as_mut().poll_next(cx), Poll::Pending);

        Poll::Ready(())
    }

    mod a_std {
        use futures::future;

        #[async_std::test]
        async fn drop_one_half() {
            future::poll_fn(super::drop_one_half).await;
        }

        #[async_std::test]
        async fn lockstep() {
            future::poll_fn(super::lockstep).await;
        }

        #[async_std::test]
        async fn waits_for_other() {
            future::poll_fn(super::waits_for_other).await;
        }
    }

    mod tk {
        use futures::future;

        #[tokio::test]
        async fn drop_one_half() {
            future::poll_fn(super::drop_one_half).await;
        }

        #[tokio::test]
        async fn lockstep() {
            future::poll_fn(super::lockstep).await;
        }

        #[tokio::test]
        async fn waits_for_other() {
            future::poll_fn(super::waits_for_other).await;
        }
    }
}
