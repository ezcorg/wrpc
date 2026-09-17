use core::future::Future;
use core::iter::zip;
use core::ops::{BitOrAssign, Shl};
use core::pin::{Pin, pin};

use std::collections::HashSet;

use crate::access::StoreAccess;
use bytes::{BufMut as _, Bytes, BytesMut};
use futures::TryStreamExt as _;
use futures::stream::FuturesUnordered;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWriteExt as _};
use tokio_util::codec::{Encoder, FramedRead};
use tracing::{instrument, trace, warn};
use uuid::Uuid;
use wasm_tokio::cm::AsyncReadValue as _;
use wasm_tokio::{
    AsyncReadCore as _, AsyncReadLeb128 as _, AsyncReadUtf8 as _, CoreNameEncoder,
    CoreVecEncoderBytes, Leb128Encoder, Utf8Codec,
};
use wasmtime::bail;
use wasmtime::component::types::{Case, Field};
use wasmtime::component::{ResourceType, Type, Val};
use wasmtime::error::Context as _;
use wasmtime_wasi::p2::pipe::AsyncReadStream;
use wasmtime_wasi::p2::{DynInputStream, StreamError};
use wrpc_transport::ListDecoderU8;
use wrpc_transport::frame::{Incoming, Outgoing};

use crate::{RemoteResource, WrpcView};

pub struct ValEncoder<'a, T: 'static, A: StoreAccess<T>> {
    pub store: &'a mut A,
    pub _data: core::marker::PhantomData<T>,
    /// The connection scope shared resource handles are minted in.
    pub scope: Option<u64>,
    pub ty: &'a Type,
    pub resources: &'a [ResourceType],
    /// Resource types bridged to wRPC `stream<u8>` (`wasi:io` `input-stream`/
    /// `output-stream`), identified by their possibly-uninstantiated type.
    pub io_streams: &'a [ResourceType],
    pub deferred: Option<
        Box<
            dyn FnOnce(Outgoing) -> Pin<Box<dyn Future<Output = wasmtime::Result<()>> + Send>>
                + Send,
        >,
    >,
}

impl<T: 'static, A: StoreAccess<T>> ValEncoder<'_, T, A> {
    #[must_use]
    pub fn new<'a>(
        store: &'a mut A,
        scope: Option<u64>,
        ty: &'a Type,
        resources: &'a [ResourceType],
        io_streams: &'a [ResourceType],
    ) -> ValEncoder<'a, T, A> {
        ValEncoder {
            store,
            _data: core::marker::PhantomData,
            scope,
            ty,
            resources,
            io_streams,
            deferred: None,
        }
    }

    pub fn with_type<'a>(&'a mut self, ty: &'a Type) -> ValEncoder<'a, T, A> {
        ValEncoder {
            store: &mut *self.store,
            _data: core::marker::PhantomData,
            scope: self.scope,
            ty,
            resources: self.resources,
            io_streams: self.io_streams,
            deferred: None,
        }
    }
}

fn find_enum_discriminant<'a, T>(
    iter: impl IntoIterator<Item = T>,
    names: impl IntoIterator<Item = &'a str>,
    discriminant: &str,
) -> wasmtime::Result<T> {
    zip(iter, names)
        .find_map(|(i, name)| (name == discriminant).then_some(i))
        .context("unknown enum discriminant")
}

fn find_variant_discriminant<'a, T>(
    iter: impl IntoIterator<Item = T>,
    cases: impl IntoIterator<Item = Case<'a>>,
    discriminant: &str,
) -> wasmtime::Result<(T, Option<Type>)> {
    zip(iter, cases)
        .find_map(|(i, Case { name, ty })| (name == discriminant).then_some((i, ty)))
        .context("unknown variant discriminant")
}

#[inline]
fn flag_bits<'a, T: BitOrAssign + Shl<u8, Output = T> + From<u8>>(
    names: impl IntoIterator<Item = &'a str>,
    flags: impl IntoIterator<Item = &'a str>,
) -> T {
    let mut v = T::from(0);
    let flags: HashSet<&str> = flags.into_iter().collect();
    for (i, name) in zip(0u8.., names) {
        if flags.contains(name) {
            v |= T::from(1) << i;
        }
    }
    v
}

async fn write_deferred<I>(w: Outgoing, deferred: I) -> wasmtime::Result<()>
where
    I: IntoIterator,
    I::IntoIter: ExactSizeIterator<
        Item = Option<
            Box<
                dyn FnOnce(Outgoing) -> Pin<Box<dyn Future<Output = wasmtime::Result<()>> + Send>>
                    + Send,
            >,
        >,
    >,
{
    let mut futs: FuturesUnordered<_> = zip(0.., deferred)
        .filter_map(|(i, f)| f.map(|f| (w.index(&[i]), f)))
        .map(|(w, f)| async move {
            let w = w.map_err(wasmtime::Error::from)?;
            f(w).await
        })
        .collect();
    while let Some(()) = futs.try_next().await? {}
    Ok(())
}

impl<T, A> Encoder<&Val> for ValEncoder<'_, T, A>
where
    T: WrpcView + 'static,
    A: StoreAccess<T>,
{
    type Error = wasmtime::Error;

    #[allow(clippy::too_many_lines)]
    #[instrument(level = "trace", skip(self))]
    fn encode(&mut self, v: &Val, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match (v, self.ty) {
            (Val::Bool(v), Type::Bool) => {
                dst.reserve(1);
                dst.put_u8((*v).into());
                Ok(())
            }
            (Val::S8(v), Type::S8) => {
                dst.reserve(1);
                dst.put_i8(*v);
                Ok(())
            }
            (Val::U8(v), Type::U8) => {
                dst.reserve(1);
                dst.put_u8(*v);
                Ok(())
            }
            (Val::S16(v), Type::S16) => Leb128Encoder
                .encode(*v, dst)
                .context("failed to encode s16"),
            (Val::U16(v), Type::U16) => Leb128Encoder
                .encode(*v, dst)
                .context("failed to encode u16"),
            (Val::S32(v), Type::S32) => Leb128Encoder
                .encode(*v, dst)
                .context("failed to encode s32"),
            (Val::U32(v), Type::U32) => Leb128Encoder
                .encode(*v, dst)
                .context("failed to encode u32"),
            (Val::S64(v), Type::S64) => Leb128Encoder
                .encode(*v, dst)
                .context("failed to encode s64"),
            (Val::U64(v), Type::U64) => Leb128Encoder
                .encode(*v, dst)
                .context("failed to encode u64"),
            (Val::Float32(v), Type::Float32) => {
                dst.reserve(4);
                dst.put_f32_le(*v);
                Ok(())
            }
            (Val::Float64(v), Type::Float64) => {
                dst.reserve(8);
                dst.put_f64_le(*v);
                Ok(())
            }
            (Val::Char(v), Type::Char) => {
                Utf8Codec.encode(*v, dst).context("failed to encode char")
            }
            (Val::String(v), Type::String) => CoreNameEncoder
                .encode(v.as_str(), dst)
                .context("failed to encode string"),
            (Val::List(vs), Type::List(ty)) => {
                let ty = ty.ty();
                let n = u32::try_from(vs.len()).context("list length does not fit in u32")?;
                dst.reserve(5 + vs.len());
                Leb128Encoder
                    .encode(n, dst)
                    .context("failed to encode list length")?;
                let mut deferred = Vec::with_capacity(vs.len());
                for v in vs {
                    let mut enc = self.with_type(&ty);
                    enc.encode(v, dst)
                        .context("failed to encode list element")?;
                    deferred.push(enc.deferred);
                }
                if deferred.iter().any(Option::is_some) {
                    self.deferred = Some(Box::new(|w| Box::pin(write_deferred(w, deferred))));
                }
                Ok(())
            }
            (Val::Record(vs), Type::Record(ty)) => {
                dst.reserve(vs.len());
                let mut deferred = Vec::with_capacity(vs.len());
                for ((name, v), Field { ref ty, .. }) in zip(vs, ty.fields()) {
                    let mut enc = self.with_type(ty);
                    enc.encode(v, dst)
                        .with_context(|| format!("failed to encode `{name}` field"))?;
                    deferred.push(enc.deferred);
                }
                if deferred.iter().any(Option::is_some) {
                    self.deferred = Some(Box::new(|w| Box::pin(write_deferred(w, deferred))));
                }
                Ok(())
            }
            (Val::Tuple(vs), Type::Tuple(ty)) => {
                dst.reserve(vs.len());
                let mut deferred = Vec::with_capacity(vs.len());
                for (v, ref ty) in zip(vs, ty.types()) {
                    let mut enc = self.with_type(ty);
                    enc.encode(v, dst)
                        .context("failed to encode tuple element")?;
                    deferred.push(enc.deferred);
                }
                if deferred.iter().any(Option::is_some) {
                    self.deferred = Some(Box::new(|w| Box::pin(write_deferred(w, deferred))));
                }
                Ok(())
            }
            (Val::Variant(discriminant, v), Type::Variant(ty)) => {
                let cases = ty.cases();
                let ty = match cases.len() {
                    ..=0x0000_00ff => {
                        let (discriminant, ty) =
                            find_variant_discriminant(0u8.., cases, discriminant)?;
                        dst.reserve(2 + usize::from(v.is_some()));
                        Leb128Encoder.encode(discriminant, dst)?;
                        ty
                    }
                    0x0000_0100..=0x0000_ffff => {
                        let (discriminant, ty) =
                            find_variant_discriminant(0u16.., cases, discriminant)?;
                        dst.reserve(3 + usize::from(v.is_some()));
                        Leb128Encoder.encode(discriminant, dst)?;
                        ty
                    }
                    0x0001_0000..=0x00ff_ffff => {
                        let (discriminant, ty) =
                            find_variant_discriminant(0u32.., cases, discriminant)?;
                        dst.reserve(4 + usize::from(v.is_some()));
                        Leb128Encoder.encode(discriminant, dst)?;
                        ty
                    }
                    0x0100_0000..=0xffff_ffff => {
                        let (discriminant, ty) =
                            find_variant_discriminant(0u32.., cases, discriminant)?;
                        dst.reserve(5 + usize::from(v.is_some()));
                        Leb128Encoder.encode(discriminant, dst)?;
                        ty
                    }
                    0x1_0000_0000.. => bail!("case count does not fit in u32"),
                };
                if let Some(v) = v {
                    let ty = ty.context("type missing for variant")?;
                    let mut enc = self.with_type(&ty);
                    enc.encode(v, dst)
                        .context("failed to encode variant value")?;
                    if let Some(f) = enc.deferred {
                        self.deferred = Some(f);
                    }
                }
                Ok(())
            }
            (Val::Enum(discriminant), Type::Enum(ty)) => {
                let names = ty.names();
                match names.len() {
                    ..=0x0000_00ff => {
                        let discriminant = find_enum_discriminant(0u8.., names, discriminant)?;
                        dst.reserve(2);
                        Leb128Encoder.encode(discriminant, dst)?;
                    }
                    0x0000_0100..=0x0000_ffff => {
                        let discriminant = find_enum_discriminant(0u16.., names, discriminant)?;
                        dst.reserve(3);
                        Leb128Encoder.encode(discriminant, dst)?;
                    }
                    0x0001_0000..=0x00ff_ffff => {
                        let discriminant = find_enum_discriminant(0u32.., names, discriminant)?;
                        dst.reserve(4);
                        Leb128Encoder.encode(discriminant, dst)?;
                    }
                    0x0100_0000..=0xffff_ffff => {
                        let discriminant = find_enum_discriminant(0u32.., names, discriminant)?;
                        dst.reserve(5);
                        Leb128Encoder.encode(discriminant, dst)?;
                    }
                    0x1_0000_0000.. => bail!("name count does not fit in u32"),
                }
                Ok(())
            }
            (Val::Option(None), Type::Option(_)) => {
                dst.reserve(1);
                dst.put_u8(0);
                Ok(())
            }
            (Val::Option(Some(v)), Type::Option(ty)) => {
                dst.reserve(2);
                dst.put_u8(1);
                let ty = ty.ty();
                let mut enc = self.with_type(&ty);
                enc.encode(v, dst)
                    .context("failed to encode `option::some` value")?;
                if let Some(f) = enc.deferred {
                    self.deferred = Some(f);
                }
                Ok(())
            }
            (Val::Result(v), Type::Result(ty)) => match v {
                Ok(v) => match (v, ty.ok()) {
                    (Some(v), Some(ty)) => {
                        dst.reserve(2);
                        dst.put_u8(0);
                        let mut enc = self.with_type(&ty);
                        enc.encode(v, dst)
                            .context("failed to encode `result::ok` value")?;
                        if let Some(f) = enc.deferred {
                            self.deferred = Some(f);
                        }
                        Ok(())
                    }
                    (Some(_v), None) => bail!("`result::ok` value of unknown type"),
                    (None, Some(_ty)) => bail!("`result::ok` value missing"),
                    (None, None) => {
                        dst.reserve(1);
                        dst.put_u8(0);
                        Ok(())
                    }
                },
                Err(v) => match (v, ty.err()) {
                    (Some(v), Some(ty)) => {
                        dst.reserve(2);
                        dst.put_u8(1);
                        let mut enc = self.with_type(&ty);
                        enc.encode(v, dst)
                            .context("failed to encode `result::err` value")?;
                        if let Some(f) = enc.deferred {
                            self.deferred = Some(f);
                        }
                        Ok(())
                    }
                    (Some(_v), None) => bail!("`result::err` value of unknown type"),
                    (None, Some(_ty)) => bail!("`result::err` value missing"),
                    (None, None) => {
                        dst.reserve(1);
                        dst.put_u8(1);
                        Ok(())
                    }
                },
            },
            (Val::Flags(vs), Type::Flags(ty)) => {
                let names = ty.names();
                let vs = vs.iter().map(String::as_str);
                match names.len() {
                    ..=8 => {
                        dst.reserve(1);
                        dst.put_u8(flag_bits(names, vs));
                    }
                    9..=16 => {
                        dst.reserve(2);
                        dst.put_u16_le(flag_bits(names, vs));
                    }
                    17..=24 => {
                        dst.reserve(3);
                        dst.put_slice(&u32::to_le_bytes(flag_bits(names, vs))[..3]);
                    }
                    25..=32 => {
                        dst.reserve(4);
                        dst.put_u32_le(flag_bits(names, vs));
                    }
                    33..=40 => {
                        dst.reserve(5);
                        dst.put_slice(&u64::to_le_bytes(flag_bits(names, vs))[..5]);
                    }
                    41..=48 => {
                        dst.reserve(6);
                        dst.put_slice(&u64::to_le_bytes(flag_bits(names, vs))[..6]);
                    }
                    49..=56 => {
                        dst.reserve(7);
                        dst.put_slice(&u64::to_le_bytes(flag_bits(names, vs))[..7]);
                    }
                    57..=64 => {
                        dst.reserve(8);
                        dst.put_u64_le(flag_bits(names, vs));
                    }
                    65..=72 => {
                        dst.reserve(9);
                        dst.put_slice(&u128::to_le_bytes(flag_bits(names, vs))[..9]);
                    }
                    73..=80 => {
                        dst.reserve(10);
                        dst.put_slice(&u128::to_le_bytes(flag_bits(names, vs))[..10]);
                    }
                    81..=88 => {
                        dst.reserve(11);
                        dst.put_slice(&u128::to_le_bytes(flag_bits(names, vs))[..11]);
                    }
                    89..=96 => {
                        dst.reserve(12);
                        dst.put_slice(&u128::to_le_bytes(flag_bits(names, vs))[..12]);
                    }
                    97..=104 => {
                        dst.reserve(13);
                        dst.put_slice(&u128::to_le_bytes(flag_bits(names, vs))[..13]);
                    }
                    105..=112 => {
                        dst.reserve(14);
                        dst.put_slice(&u128::to_le_bytes(flag_bits(names, vs))[..14]);
                    }
                    113..=120 => {
                        dst.reserve(15);
                        dst.put_slice(&u128::to_le_bytes(flag_bits(names, vs))[..15]);
                    }
                    121..=128 => {
                        dst.reserve(16);
                        dst.put_u128_le(flag_bits(names, vs));
                    }
                    bits @ 129.. => {
                        let mut cap = bits / 8;
                        if bits % 8 != 0 {
                            cap = cap.saturating_add(1);
                        }
                        let mut buf = vec![0; cap];
                        let flags: HashSet<&str> = vs.into_iter().collect();
                        for (i, name) in names.enumerate() {
                            if flags.contains(name) {
                                buf[i / 8] |= 1 << (i % 8);
                            }
                        }
                        dst.extend_from_slice(&buf);
                    }
                }
                Ok(())
            }
            (Val::Resource(resource), Type::Own(ty) | Type::Borrow(ty)) => {
                if *ty == ResourceType::host::<DynInputStream>() || self.io_streams.contains(ty) {
                    let stream = self.store.with(|mut store| -> wasmtime::Result<_> {
                        let stream = resource
                            .try_into_resource::<DynInputStream>(&mut store)
                            .context("failed to downcast `wasi:io/input-stream`")?;
                        if !stream.owned() {
                            // NOTE: In order to handle this we'd need to know how many bytes the
                            // receiver has read. That means that some kind of callback would be
                            // required from the receiver. This is not trivial and generally should
                            // be a very rare use case.
                            bail!("encoding borrowed `wasi:io/input-stream` not supported yet");
                        }
                        store
                            .data_mut()
                            .wrpc()
                            .table
                            .delete(stream)
                            .context("failed to delete input stream")
                    })?;
                    let mut stream = stream;
                    // Pending: an empty inline chunk; the bytes follow on the
                    // indexed sub-stream, chunked and ended by an empty chunk.
                    dst.reserve(1);
                    dst.put_u8(0x00);
                    self.deferred = Some(Box::new(|w| {
                        Box::pin(async move {
                            let mut w = pin!(w);
                            loop {
                                stream.ready().await;
                                match stream.read(8096) {
                                    Ok(buf) => {
                                        let mut chunk =
                                            BytesMut::with_capacity(buf.len().saturating_add(5));
                                        CoreVecEncoderBytes
                                            .encode(buf, &mut chunk)
                                            .context("failed to encode input stream chunk")?;
                                        w.write_all(&chunk).await?;
                                    }
                                    Err(StreamError::Closed) => {
                                        w.write_all(&[0x00]).await?;
                                    }
                                    Err(err) => return Err(err.into()),
                                }
                            }
                        })
                    }));
                    Ok(())
                } else if resource.ty() == ResourceType::host::<RemoteResource>() {
                    let buf = self.store.with(|mut store| -> wasmtime::Result<Bytes> {
                        let resource = resource
                            .try_into_resource(&mut store)
                            .context("resource type mismatch")?;
                        let table = store.data_mut().wrpc().table;
                        if resource.owned() {
                            let RemoteResource(buf) = table
                                .delete(resource)
                                .context("failed to delete remote resource")?;
                            Ok(buf)
                        } else {
                            let RemoteResource(buf) = table
                                .get(&resource)
                                .context("failed to get remote resource")?;
                            Ok(buf.clone())
                        }
                    })?;
                    CoreVecEncoderBytes
                        .encode(buf, dst)
                        .context("failed to encode resource handle")
                } else if self.resources.contains(ty) {
                    let id = Uuid::now_v7();
                    CoreVecEncoderBytes
                        .encode(id.to_bytes_le().as_slice(), dst)
                        .context("failed to encode resource handle")?;
                    trace!(?id, "store shared resource");
                    let scope = self.scope;
                    self.store.with(|mut store| {
                        store
                            .data_mut()
                            .wrpc()
                            .ctx
                            .shared_resources()
                            .try_insert(scope, id, *resource)
                            .context("failed to store shared resource")
                    })?;
                    Ok(())
                } else {
                    bail!("encoding host resources not supported yet")
                }
            }

            // A component-model `stream<u8>`: the guest's stream is drained into
            // a channel and written after the value, as wRPC carries it.
            (Val::Stream(stream), Type::Stream(ty)) => {
                if ty.ty() != Some(Type::U8) {
                    bail!("only `stream<u8>` is supported");
                }
                let stream = stream.clone();
                let (mut bytes, _done) = self.store.with(|mut store| -> wasmtime::Result<_> {
                    let reader = stream
                        .try_into_stream_reader::<u8>()
                        .context("stream payload type mismatch")?;
                    crate::stream::drain(&mut store, reader)
                })?;
                // Pending: an empty inline chunk; the bytes follow on the
                // indexed sub-stream, chunked and ended by an empty chunk.
                dst.reserve(1);
                dst.put_u8(0x00);
                self.deferred = Some(Box::new(|w| {
                    Box::pin(async move {
                        use futures::StreamExt as _;
                        let mut w = pin!(w);
                        while let Some(buf) = bytes.next().await {
                            let mut chunk = BytesMut::with_capacity(buf.len().saturating_add(5));
                            CoreVecEncoderBytes
                                .encode(buf, &mut chunk)
                                .context("failed to encode stream chunk")?;
                            w.write_all(&chunk).await?;
                        }
                        w.write_all(&[0x00]).await?;
                        w.flush().await?;
                        Ok(())
                    })
                }));
                Ok(())
            }

            (_, Type::Future(..) | Type::ErrorContext) => {
                bail!("futures and error contexts are not supported")
            }
            (_, Type::Map(..)) => {
                bail!("maps not supported")
            }
            (_, Type::FixedLengthList(..)) => {
                bail!("fixed-length lists not supported")
            }
            _ => bail!("value type mismatch"),
        }
    }
}

#[inline]
async fn read_flags(n: usize, r: &mut (impl AsyncRead + Unpin)) -> std::io::Result<u128> {
    let mut buf = 0u128.to_le_bytes();
    r.read_exact(&mut buf[..n]).await?;
    Ok(u128::from_le_bytes(buf))
}

/// Read encoded value of type [`Type`] from an [`AsyncRead`] into a [`Val`]
#[instrument(level = "trace", skip_all, fields(ty, path))]
#[allow(clippy::too_many_arguments)]
pub async fn read_value<T, A>(
    store: &mut A,
    scope: Option<u64>,
    r: &mut Pin<&mut Incoming>,
    resources: &[ResourceType],
    io_streams: &[ResourceType],
    val: &mut Val,
    ty: &Type,
    path: &[usize],
) -> std::io::Result<()>
where
    T: WrpcView + 'static,
    A: StoreAccess<T>,
{
    let owned = matches!(ty, Type::Own(_));
    match ty {
        Type::Bool => {
            let v = r.read_bool().await?;
            *val = Val::Bool(v);
            Ok(())
        }
        Type::S8 => {
            let v = r.read_i8().await?;
            *val = Val::S8(v);
            Ok(())
        }
        Type::U8 => {
            let v = r.read_u8().await?;
            *val = Val::U8(v);
            Ok(())
        }
        Type::S16 => {
            let v = r.read_i16_leb128().await?;
            *val = Val::S16(v);
            Ok(())
        }
        Type::U16 => {
            let v = r.read_u16_leb128().await?;
            *val = Val::U16(v);
            Ok(())
        }
        Type::S32 => {
            let v = r.read_i32_leb128().await?;
            *val = Val::S32(v);
            Ok(())
        }
        Type::U32 => {
            let v = r.read_u32_leb128().await?;
            *val = Val::U32(v);
            Ok(())
        }
        Type::S64 => {
            let v = r.read_i64_leb128().await?;
            *val = Val::S64(v);
            Ok(())
        }
        Type::U64 => {
            let v = r.read_u64_leb128().await?;
            *val = Val::U64(v);
            Ok(())
        }
        Type::Float32 => {
            let v = r.read_f32_le().await?;
            *val = Val::Float32(v);
            Ok(())
        }
        Type::Float64 => {
            let v = r.read_f64_le().await?;
            *val = Val::Float64(v);
            Ok(())
        }
        Type::Char => {
            let v = r.read_char_utf8().await?;
            *val = Val::Char(v);
            Ok(())
        }
        Type::String => {
            let mut s = String::default();
            r.read_core_name(&mut s).await?;
            *val = Val::String(s);
            Ok(())
        }
        Type::List(ty) => {
            let n = r.read_u32_leb128().await?;
            let n = n.try_into().unwrap_or(usize::MAX);
            let mut vs = Vec::with_capacity(n);
            let ty = ty.ty();
            let mut path = path.to_vec();
            for i in 0..n {
                let mut v = Val::Bool(false);
                path.push(i);
                trace!(i, "reading list element value");
                Box::pin(read_value(
                    store, scope, r, resources, io_streams, &mut v, &ty, &path,
                ))
                .await?;
                path.pop();
                vs.push(v);
            }
            *val = Val::List(vs);
            Ok(())
        }
        Type::Record(ty) => {
            let fields = ty.fields();
            let mut vs = Vec::with_capacity(fields.len());
            let mut path = path.to_vec();
            for (i, Field { name, ty }) in fields.enumerate() {
                let mut v = Val::Bool(false);
                path.push(i);
                trace!(i, "reading struct field value");
                Box::pin(read_value(
                    store, scope, r, resources, io_streams, &mut v, &ty, &path,
                ))
                .await?;
                path.pop();
                vs.push((name.to_string(), v));
            }
            *val = Val::Record(vs);
            Ok(())
        }
        Type::Tuple(ty) => {
            let types = ty.types();
            let mut vs = Vec::with_capacity(types.len());
            let mut path = path.to_vec();
            for (i, ty) in types.enumerate() {
                let mut v = Val::Bool(false);
                path.push(i);
                trace!(i, "reading tuple element value");
                Box::pin(read_value(
                    store, scope, r, resources, io_streams, &mut v, &ty, &path,
                ))
                .await?;
                path.pop();
                vs.push(v);
            }
            *val = Val::Tuple(vs);
            Ok(())
        }
        Type::Variant(ty) => {
            let discriminant = r.read_u32_leb128().await?;
            let discriminant = discriminant
                .try_into()
                .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))?;
            let Case { name, ty } = ty.cases().nth(discriminant).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("unknown variant discriminant `{discriminant}`"),
                )
            })?;
            let name = name.to_string();
            if let Some(ty) = ty {
                let mut v = Val::Bool(false);
                trace!(variant = name, "reading nested variant value");
                Box::pin(read_value(
                    store, scope, r, resources, io_streams, &mut v, &ty, path,
                ))
                .await?;
                *val = Val::Variant(name, Some(Box::new(v)));
            } else {
                *val = Val::Variant(name, None);
            }
            Ok(())
        }
        Type::Enum(ty) => {
            let discriminant = r.read_u32_leb128().await?;
            let discriminant = discriminant
                .try_into()
                .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))?;
            let name = ty.names().nth(discriminant).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("unknown enum discriminant `{discriminant}`"),
                )
            })?;
            *val = Val::Enum(name.to_string());
            Ok(())
        }
        Type::Option(ty) => {
            let ok = r.read_option_status().await?;
            if ok {
                let mut v = Val::Bool(false);
                trace!("reading nested `option::some` value");
                Box::pin(read_value(
                    store,
                    scope,
                    r,
                    resources,
                    io_streams,
                    &mut v,
                    &ty.ty(),
                    path,
                ))
                .await?;
                *val = Val::Option(Some(Box::new(v)));
            } else {
                *val = Val::Option(None);
            }
            Ok(())
        }
        Type::Result(ty) => {
            let ok = r.read_result_status().await?;
            if ok {
                if let Some(ty) = ty.ok() {
                    let mut v = Val::Bool(false);
                    trace!("reading nested `result::ok` value");
                    Box::pin(read_value(
                        store, scope, r, resources, io_streams, &mut v, &ty, path,
                    ))
                    .await?;
                    *val = Val::Result(Ok(Some(Box::new(v))));
                } else {
                    *val = Val::Result(Ok(None));
                }
            } else if let Some(ty) = ty.err() {
                let mut v = Val::Bool(false);
                trace!("reading nested `result::err` value");
                Box::pin(read_value(
                    store, scope, r, resources, io_streams, &mut v, &ty, path,
                ))
                .await?;
                *val = Val::Result(Err(Some(Box::new(v))));
            } else {
                *val = Val::Result(Err(None));
            }
            Ok(())
        }
        Type::Flags(ty) => {
            let names = ty.names();
            let flags = match names.len() {
                ..=8 => read_flags(1, r).await?,
                9..=16 => read_flags(2, r).await?,
                17..=24 => read_flags(3, r).await?,
                25..=32 => read_flags(4, r).await?,
                33..=40 => read_flags(5, r).await?,
                41..=48 => read_flags(6, r).await?,
                49..=56 => read_flags(7, r).await?,
                57..=64 => read_flags(8, r).await?,
                65..=72 => read_flags(9, r).await?,
                73..=80 => read_flags(10, r).await?,
                81..=88 => read_flags(11, r).await?,
                89..=96 => read_flags(12, r).await?,
                97..=104 => read_flags(13, r).await?,
                105..=112 => read_flags(14, r).await?,
                113..=120 => read_flags(15, r).await?,
                121..=128 => r.read_u128_le().await?,
                bits @ 129.. => {
                    let mut cap = bits / 8;
                    if bits % 8 != 0 {
                        cap = cap.saturating_add(1);
                    }
                    let mut buf = vec![0; cap];
                    r.read_exact(&mut buf).await?;
                    let mut vs = Vec::with_capacity(
                        buf.iter()
                            .map(|b| b.count_ones())
                            .sum::<u32>()
                            .try_into()
                            .unwrap_or(usize::MAX),
                    );
                    for (i, name) in names.enumerate() {
                        if buf[i / 8] & (1 << (i % 8)) != 0 {
                            vs.push(name.to_string());
                        }
                    }
                    *val = Val::Flags(vs);
                    return Ok(());
                }
            };
            let mut vs = Vec::with_capacity(flags.count_ones().try_into().unwrap_or(usize::MAX));
            for (i, name) in zip(0.., names) {
                if flags & (1 << i) != 0 {
                    vs.push(name.to_string());
                }
            }
            *val = Val::Flags(vs);
            Ok(())
        }
        Type::Own(ty) | Type::Borrow(ty) => {
            if *ty == ResourceType::host::<DynInputStream>() || io_streams.contains(ty) {
                let bytes = read_byte_stream(r, path).await?;
                // The stream must be typed as `DynInputStream` (the host resource type),
                // otherwise the resulting resource handle carries the concrete reader type
                // and fails the guest's `own<input-stream>` type check.
                let stream: DynInputStream =
                    Box::new(AsyncReadStream::new(tokio_util::io::StreamReader::new(
                        futures::StreamExt::map(bytes, Ok::<_, std::io::Error>),
                    )));
                let v = store.with(|mut store| -> std::io::Result<_> {
                    let res =
                        store.data_mut().wrpc().table.push(stream).map_err(|err| {
                            std::io::Error::new(std::io::ErrorKind::OutOfMemory, err)
                        })?;
                    res.try_into_resource_any(store)
                        .map_err(std::io::Error::other)
                })?;
                *val = Val::Resource(v);
                Ok(())
            } else if resources.contains(ty) {
                let mut id = uuid::Bytes::default();
                debug_assert_eq!(id.len(), 16);
                let n = r.read_u8_leb128().await?;
                if usize::from(n) != id.len() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "invalid guest resource handle length {n}, expected {}",
                            id.len()
                        ),
                    ));
                }
                let n = r.read_exact(&mut id).await?;
                if n != id.len() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "invalid amount of guest resource handle bytes read {n}, expected {}",
                            id.len()
                        ),
                    ));
                }

                let id = Uuid::from_bytes_le(id);
                trace!(?id, "lookup shared resource");
                // An `own` parameter hands the resource to the callee: it
                // leaves the table with the call. A `borrow` only looks it up.
                let resource = store
                    .with(|mut store| {
                        let shared = store.data_mut().wrpc().ctx.shared_resources();
                        if owned {
                            shared.remove(scope, &id)
                        } else {
                            shared.get(scope, &id).copied()
                        }
                    })
                    .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotFound))?;
                *val = Val::Resource(resource);
                Ok(())
            } else {
                let n = r.read_u32_leb128().await?;
                let n = usize::try_from(n)
                    .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))?;
                let mut buf = Vec::with_capacity(n);
                r.read_to_end(&mut buf).await?;
                let resource = store.with(|mut store| -> std::io::Result<_> {
                    let table = store.data_mut().wrpc().table;
                    let resource = table
                        .push(RemoteResource(buf.into()))
                        .map_err(|err| std::io::Error::new(std::io::ErrorKind::OutOfMemory, err))?;
                    resource
                        .try_into_resource_any(store)
                        .map_err(std::io::Error::other)
                })?;
                *val = Val::Resource(resource);
                Ok(())
            }
        }
        // A component-model `stream<u8>`: the wRPC sub-stream at this path
        // becomes what the guest reads.
        Type::Stream(ty) => {
            if ty.ty() != Some(Type::U8) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "only `stream<u8>` is supported",
                ));
            }
            let bytes = read_byte_stream(r, path).await?;
            let producer = crate::stream::BytesProducer::new(bytes);
            let v = store.with(|mut store| -> std::io::Result<_> {
                let reader = wasmtime::component::StreamReader::<u8>::new(&mut store, producer)
                    .map_err(std::io::Error::other)?;
                reader
                    .try_into_stream_any(&mut store)
                    .map_err(std::io::Error::other)
            })?;
            *val = Val::Stream(v);
            Ok(())
        }
        Type::Future(..) | Type::ErrorContext => Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "futures and error contexts are not supported",
        )),
        Type::Map(..) => Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "maps not supported",
        )),
        Type::FixedLengthList(..) => Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "fixed-length lists not supported",
        )),
    }
}

/// A wRPC `stream<u8>` value at `path`: an inline length-prefixed chunk, which
/// when non-empty is the whole stream, and when empty means the bytes follow
/// on the indexed sub-stream as chunks ended by an empty one.
async fn read_byte_stream(
    r: &mut Pin<&mut Incoming>,
    path: &[usize],
) -> std::io::Result<crate::stream::BoxStream> {
    use futures::StreamExt as _;
    let n = r.read_u32_leb128().await?;
    if n > 0 {
        let n = usize::try_from(n)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))?;
        let mut buf = vec![0; n];
        r.read_exact(&mut buf).await?;
        return Ok(Box::pin(futures::stream::iter([Bytes::from(buf)])));
    }
    let sub = r.index(path).map_err(std::io::Error::other)?;
    let chunks = FramedRead::new(sub, ListDecoderU8::default())
        .take_while(|item| core::future::ready(item.as_ref().is_ok_and(|c| !c.is_empty())))
        .filter_map(|item| core::future::ready(item.ok().map(Bytes::from)));
    Ok(Box::pin(chunks))
}
