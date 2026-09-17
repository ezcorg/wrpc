//! Bridges between wRPC byte streams and component-model `stream<u8>`: a
//! producer the guest reads (fed from a wRPC stream) and a consumer that
//! forwards what the guest writes into a channel, ending it when the guest's
//! end is gone.

use core::pin::Pin;
use core::task::{Context, Poll};

use bytes::Bytes;
use futures::Stream;
use wasmtime::StoreContextMut;
use wasmtime::component::{Destination, Source, StreamConsumer, StreamProducer, StreamResult};

/// A byte stream as wRPC carries it.
pub type BoxStream = Pin<Box<dyn Stream<Item = Bytes> + Send>>;

/// A wRPC byte stream as a wasmtime stream producer: what the guest reads.
pub struct BytesProducer {
    stream: BoxStream,
    pending: Option<Bytes>,
}

impl BytesProducer {
    pub fn new(stream: BoxStream) -> Self {
        Self {
            stream,
            pending: None,
        }
    }

    fn emit<D>(
        &mut self,
        store: StoreContextMut<'_, D>,
        dst: Destination<'_, u8, Bytes>,
        mut data: Bytes,
        cap: usize,
    ) {
        let n = data.len().min(cap);
        if data.len() > n {
            self.pending = Some(data.split_off(n));
        }
        let mut direct = dst.as_direct(store, n);
        if let Some(slice) = direct.remaining().get_mut(..n) {
            slice.copy_from_slice(&data);
        }
        direct.mark_written(n);
    }
}

impl<D: 'static> StreamProducer<D> for BytesProducer {
    type Item = u8;
    type Buffer = Bytes;

    fn poll_produce<'a>(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut store: StoreContextMut<'a, D>,
        mut dst: Destination<'a, u8, Bytes>,
        finish: bool,
    ) -> Poll<wasmtime::Result<StreamResult>> {
        let cap = dst.remaining(&mut store);
        if let Some(pending) = self.pending.take() {
            match cap {
                Some(0) => {
                    self.pending = Some(pending);
                    return Poll::Ready(Ok(StreamResult::Completed));
                }
                Some(cap) => {
                    self.emit(store, dst, pending, cap);
                    return Poll::Ready(Ok(StreamResult::Completed));
                }
                None => {
                    dst.set_buffer(pending);
                    return Poll::Ready(Ok(StreamResult::Completed));
                }
            }
        }
        match self.stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(chunk)) => {
                match cap {
                    Some(0) => {
                        self.pending = Some(chunk);
                    }
                    Some(cap) => self.emit(store, dst, chunk, cap),
                    None => dst.set_buffer(chunk),
                }
                Poll::Ready(Ok(StreamResult::Completed))
            }
            Poll::Ready(None) => Poll::Ready(Ok(StreamResult::Dropped)),
            Poll::Pending if finish => Poll::Ready(Ok(StreamResult::Cancelled)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// A wasmtime stream consumer that forwards to a channel: what the host reads
/// from a guest-produced stream, ending the channel when the guest drops it.
pub struct ChannelConsumer {
    tx: Option<tokio::sync::mpsc::UnboundedSender<Bytes>>,
    /// Fired when the guest's end is gone (the consumer is dropped).
    done: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Drop for ChannelConsumer {
    fn drop(&mut self) {
        if let Some(done) = self.done.take() {
            let _ = done.send(());
        }
    }
}

impl<D> StreamConsumer<D> for ChannelConsumer {
    type Item = u8;

    fn poll_consume(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        store: StoreContextMut<D>,
        source: Source<u8>,
        finish: bool,
    ) -> Poll<wasmtime::Result<StreamResult>> {
        let mut src = source.as_direct(store);
        let buf = src.remaining();
        let n = buf.len();
        if n > 0 {
            let chunk = Bytes::copy_from_slice(buf);
            src.mark_read(n);
            if let Some(tx) = &self.tx
                && tx.send(chunk).is_err()
            {
                self.tx = None;
                return Poll::Ready(Ok(StreamResult::Dropped));
            }
        } else if finish {
            // Nothing left and the stream is closing: the channel ends with us.
            self.tx = None;
            return Poll::Ready(Ok(StreamResult::Dropped));
        }
        Poll::Ready(Ok(StreamResult::Completed))
    }
}

/// Turn a guest stream into a wRPC byte stream: piped through a channel that
/// closes when the guest's side does. The receiver resolves when that happens.
pub fn drain(
    store: impl wasmtime::AsContextMut,
    reader: wasmtime::component::StreamReader<u8>,
) -> wasmtime::Result<(BoxStream, tokio::sync::oneshot::Receiver<()>)> {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    reader.pipe(
        store,
        ChannelConsumer {
            tx: Some(tx),
            done: Some(done_tx),
        },
    )?;
    let stream = futures::stream::poll_fn(move |cx| rx.poll_recv(cx));
    Ok((Box::pin(stream), done_rx))
}
