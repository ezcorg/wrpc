#![allow(clippy::type_complexity)] // TODO: https://github.com/bytecodealliance/wrpc/issues/2

use core::any::Any;
use core::borrow::Borrow;
use core::fmt;
use core::future::Future;
use core::iter::zip;
use core::pin::pin;
use core::time::Duration;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::anyhow;
use bytes::{Bytes, BytesMut};
use futures::future::try_join_all;
use tokio::io::AsyncWriteExt as _;
use tokio_util::codec::Encoder;
use tracing::{debug, instrument, trace, warn};
use uuid::Uuid;
use wasmtime::component::{
    Func, Resource, ResourceAny, ResourceTable, ResourceType, Type, Val, types,
};
use wasmtime::error::Context as _;
use wasmtime::{AsContextMut, Engine, bail};
use wrpc_transport::Invoke;
use wrpc_transport::frame::{Incoming, Outgoing};

use crate::bindings::rpc::context::Context;
use crate::bindings::rpc::error::Error;
use crate::bindings::rpc::transport::{IncomingChannel, Invocation, OutgoingChannel};

pub mod access;
pub mod bindings;
mod codec;
pub mod paths;
mod polyfill;
pub mod rpc;
mod serve;
pub mod stream;

pub use access::*;
pub use codec::*;
pub use polyfill::*;
pub use serve::*;

// this returns the RPC name for a wasmtime function name.
// Unfortunately, the [`types::ComponentFunc`] does not include the kind information and we want to
// avoid (re-)parsing the WIT here.
pub fn rpc_func_name(name: &str) -> &str {
    if let Some(name) = name.strip_prefix("[constructor]") {
        name
    } else if let Some(name) = name.strip_prefix("[static]") {
        name
    } else if let Some(name) = name.strip_prefix("[method]") {
        name
    } else {
        name
    }
}

fn rpc_result_type<T: Borrow<Type>>(
    host_resources: &HashMap<Box<str>, HashMap<Box<str>, (ResourceType, ResourceType)>>,
    results_ty: impl IntoIterator<Item = T>,
) -> Option<Option<Type>> {
    let rpc_err_ty = host_resources
        .get("wrpc:rpc/error@0.1.0")
        .and_then(|instance| instance.get("error"));
    let mut results_ty = results_ty.into_iter();
    match (
        rpc_err_ty,
        results_ty.next().as_ref().map(Borrow::borrow),
        results_ty.next(),
    ) {
        (Some((guest_rpc_err_ty, host_rpc_err_ty)), Some(Type::Result(result_ty)), None)
            if *host_rpc_err_ty == ResourceType::host::<Error>()
                && result_ty.err() == Some(Type::Own(*guest_rpc_err_ty)) =>
        {
            Some(result_ty.ok())
        }
        _ => None,
    }
}

pub struct RemoteResource(pub Bytes);

/// A table of shared resources exported by the component. The second field is a
/// capacity cap: once reached, [`SharedResourceTable::try_insert`] refuses new
/// resources (0 = unbounded). It's a backstop against unbounded growth when a client
/// keeps acquiring handles without dropping them — exhaustion then surfaces to the
/// caller as an error rather than growing memory without limit.
///
/// Every handle is **bound to the scope it was minted in**: the transport
/// connection, as the server identifies it per operation. A lookup or removal
/// from any other scope finds nothing, so a handle that leaks out of one
/// connection is inert on every other. Unscoped operations (`None`) only see
/// unscoped handles.
#[derive(Debug, Default)]
pub struct SharedResourceTable {
    entries: HashMap<Uuid, (ResourceAny, Option<u64>)>,
    capacity: usize,
    /// Handles inserted since the last [`Self::take_minted`].
    minted: Vec<Uuid>,
}

impl SharedResourceTable {
    /// A table that refuses more than `capacity` live resources (0 = unbounded).
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            capacity,
            minted: Vec::new(),
        }
    }

    /// Insert an exported resource under `scope`, returning `Err` once at
    /// capacity so the invocation fails cleanly instead of the host growing
    /// memory without bound.
    pub fn try_insert(
        &mut self,
        scope: Option<u64>,
        id: Uuid,
        resource: ResourceAny,
    ) -> std::io::Result<()> {
        if self.capacity != 0
            && self.entries.len() >= self.capacity
            && !self.entries.contains_key(&id)
        {
            return Err(std::io::Error::other(format!(
                "shared resource table at capacity ({} live handles)",
                self.capacity
            )));
        }
        self.entries.insert(id, (resource, scope));
        self.minted.push(id);
        Ok(())
    }

    /// The handles minted since the last call: what a server fronting several
    /// tables records so it can find a handle's table again.
    pub fn take_minted(&mut self) -> Vec<Uuid> {
        core::mem::take(&mut self.minted)
    }

    /// The exported resource for `id`, if it is live **and** was minted in `scope`.
    pub fn get(&self, scope: Option<u64>, id: &Uuid) -> Option<&ResourceAny> {
        match self.entries.get(id) {
            Some((resource, owner)) if *owner == scope => Some(resource),
            _ => None,
        }
    }

    /// Remove and return the exported resource for `id`, if present in `scope`.
    /// The caller is responsible for dropping the returned [`ResourceAny`] via
    /// `ResourceAny::resource_drop[_async]` (which runs the guest destructor) — this
    /// only evicts the table entry.
    pub fn remove(&mut self, scope: Option<u64>, id: &Uuid) -> Option<ResourceAny> {
        self.get(scope, id)?;
        self.entries.remove(id).map(|(resource, _)| resource)
    }

    /// The number of live exported-resource handles in the table, across every scope.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the table holds no live handles.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

pub trait WrpcCtx<T: Invoke>: Send {
    /// Returns context to use for invocation
    fn context(&self) -> T::Context;

    /// Returns an [Invoke] implementation used to satisfy polyfilled imports
    fn client(&self) -> &T;

    /// Returns a table of shared exported resources
    fn shared_resources(&mut self) -> &mut SharedResourceTable;

    /// Optional invocation timeout, component will trap if invocation is not finished within the
    /// returned [Duration]. If this method returns [None], then no timeout will be used.
    fn timeout(&self) -> Option<Duration> {
        None
    }
}

pub struct WrpcCtxView<'a, T: Invoke> {
    pub ctx: &'a mut dyn WrpcCtx<T>,
    pub table: &'a mut ResourceTable,
}

pub trait WrpcView: Send {
    type Invoke: Invoke;

    fn wrpc(&mut self) -> WrpcCtxView<'_, Self::Invoke>;
}

impl<T: WrpcView> WrpcView for &mut T {
    type Invoke = T::Invoke;

    fn wrpc(&mut self) -> WrpcCtxView<'_, Self::Invoke> {
        T::wrpc(self)
    }
}

pub trait WrpcViewExt: WrpcView {
    fn push_invocation(
        &mut self,
        invocation: impl Future<Output = anyhow::Result<(Outgoing, Incoming)>> + Send + 'static,
    ) -> wasmtime::Result<Resource<Invocation>> {
        self.wrpc()
            .table
            .push(Invocation::Future(Box::pin(async move {
                let res = invocation.await;
                Box::new(res) as Box<dyn Any + Send>
            })))
            .context("failed to push invocation to table")
    }

    fn get_invocation_result(
        &mut self,
        invocation: &Resource<Invocation>,
    ) -> wasmtime::Result<Option<&Box<anyhow::Result<(Outgoing, Incoming)>>>> {
        let invocation = self
            .wrpc()
            .table
            .get(invocation)
            .context("failed to get invocation from table")?;
        match invocation {
            Invocation::Future(..) => Ok(None),
            Invocation::Ready(res) => {
                let res = res.downcast_ref().context("invalid invocation type")?;
                Ok(Some(res))
            }
        }
    }

    fn delete_invocation(
        &mut self,
        invocation: Resource<Invocation>,
    ) -> wasmtime::Result<impl Future<Output = anyhow::Result<(Outgoing, Incoming)>>> {
        let invocation = self
            .wrpc()
            .table
            .delete(invocation)
            .context("failed to delete invocation from table")?;
        Ok(async move {
            let res = match invocation {
                Invocation::Future(fut) => fut.await,
                Invocation::Ready(res) => res,
            };
            let res = res
                .downcast()
                .map_err(|_| anyhow!("invalid invocation type"))?;
            *res
        })
    }

    fn push_outgoing_channel(
        &mut self,
        outgoing: Outgoing,
    ) -> wasmtime::Result<Resource<OutgoingChannel>> {
        self.wrpc()
            .table
            .push(OutgoingChannel(Arc::new(std::sync::RwLock::new(Box::new(
                outgoing,
            )))))
            .context("failed to push outgoing channel to table")
    }

    fn delete_outgoing_channel(
        &mut self,
        outgoing: Resource<OutgoingChannel>,
    ) -> wasmtime::Result<Outgoing> {
        let OutgoingChannel(outgoing) = self
            .wrpc()
            .table
            .delete(outgoing)
            .context("failed to delete outgoing channel from table")?;
        let outgoing =
            Arc::into_inner(outgoing).context("outgoing channel has an active stream")?;
        let Ok(outgoing) = outgoing.into_inner() else {
            bail!("lock poisoned");
        };
        let outgoing = outgoing
            .downcast()
            .map_err(|_| wasmtime::Error::msg("invalid outgoing channel type"))?;
        Ok(*outgoing)
    }

    fn push_incoming_channel(
        &mut self,
        incoming: Incoming,
    ) -> wasmtime::Result<Resource<IncomingChannel>> {
        self.wrpc()
            .table
            .push(IncomingChannel(Arc::new(std::sync::RwLock::new(Box::new(
                incoming,
            )))))
            .context("failed to push incoming channel to table")
    }

    fn delete_incoming_channel(
        &mut self,
        incoming: Resource<IncomingChannel>,
    ) -> wasmtime::Result<Incoming> {
        let IncomingChannel(incoming) = self
            .wrpc()
            .table
            .delete(incoming)
            .context("failed to delete incoming channel from table")?;
        let incoming =
            Arc::into_inner(incoming).context("incoming channel has an active stream")?;
        let Ok(incoming) = incoming.into_inner() else {
            bail!("lock poisoned");
        };
        let incoming = incoming
            .downcast()
            .map_err(|_| wasmtime::Error::msg("invalid incoming channel type"))?;
        Ok(*incoming)
    }

    fn push_error(&mut self, error: Error) -> wasmtime::Result<Resource<Error>> {
        self.wrpc()
            .table
            .push(error)
            .context("failed to push error to table")
    }

    fn get_error(&mut self, error: &Resource<Error>) -> wasmtime::Result<&Error> {
        let error = self
            .wrpc()
            .table
            .get(error)
            .context("failed to get error from table")?;
        Ok(error)
    }

    fn get_error_mut(&mut self, error: &Resource<Error>) -> wasmtime::Result<&mut Error> {
        let error = self
            .wrpc()
            .table
            .get_mut(error)
            .context("failed to get error from table")?;
        Ok(error)
    }

    fn delete_error(&mut self, error: Resource<Error>) -> wasmtime::Result<Error> {
        let error = self
            .wrpc()
            .table
            .delete(error)
            .context("failed to delete error from table")?;
        Ok(error)
    }

    fn push_context(
        &mut self,
        cx: <Self::Invoke as Invoke>::Context,
    ) -> wasmtime::Result<Resource<Context>>
    where
        <Self::Invoke as Invoke>::Context: 'static,
    {
        self.wrpc()
            .table
            .push(Context(Box::new(cx)))
            .context("failed to push context to table")
    }

    fn delete_context(
        &mut self,
        cx: Resource<Context>,
    ) -> wasmtime::Result<<Self::Invoke as Invoke>::Context>
    where
        <Self::Invoke as Invoke>::Context: 'static,
    {
        let Context(cx) = self
            .wrpc()
            .table
            .delete(cx)
            .context("failed to delete context from table")?;
        let cx = cx
            .downcast()
            .map_err(|_| wasmtime::Error::msg("invalid context type"))?;
        Ok(*cx)
    }
}

impl<T: WrpcView> WrpcViewExt for T {}

/// Error type returned by [call]
pub enum CallError {
    Decode(wasmtime::Error),
    Encode(wasmtime::Error),
    Table(wasmtime::Error),
    Call(wasmtime::Error),
    TypeMismatch(wasmtime::Error),
    Write(wasmtime::Error),
    Flush(wasmtime::Error),
    Deferred(wasmtime::Error),
    PostReturn(wasmtime::Error),
    Guest(Error),
}

impl core::error::Error for CallError {}

impl fmt::Debug for CallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CallError::Decode(error)
            | CallError::Encode(error)
            | CallError::Table(error)
            | CallError::Call(error)
            | CallError::TypeMismatch(error)
            | CallError::Write(error)
            | CallError::Flush(error)
            | CallError::Deferred(error)
            | CallError::PostReturn(error) => error.fmt(f),
            CallError::Guest(error) => error.fmt(f),
        }
    }
}

impl fmt::Display for CallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CallError::Decode(error)
            | CallError::Encode(error)
            | CallError::Table(error)
            | CallError::Call(error)
            | CallError::TypeMismatch(error)
            | CallError::Write(error)
            | CallError::Flush(error)
            | CallError::Deferred(error)
            | CallError::PostReturn(error) => error.fmt(f),
            CallError::Guest(error) => error.fmt(f),
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn call<C>(
    store: C,
    rx: Incoming,
    tx: Outgoing,
    guest_resources: &[ResourceType],
    host_resources: &HashMap<Box<str>, HashMap<Box<str>, (ResourceType, ResourceType)>>,
    io_streams: &[ResourceType],
    params_ty: impl ExactSizeIterator<Item = &Type>,
    results_ty: &[Type],
    func: Func,
) -> Result<(), CallError>
where
    C: AsContextMut,
    C::Data: WrpcView,
{
    call_observed(
        store,
        rx,
        tx,
        guest_resources,
        host_resources,
        io_streams,
        params_ty,
        results_ty,
        func,
        |_| Ok(()),
    )
    .await
}

/// [`call`] with a look at the decoded parameters before the function runs:
/// `observe` may refuse the call (its error is reported as the call's), which
/// is how a server admits a call against the values it carries.
#[allow(clippy::too_many_arguments)]
pub async fn call_observed<C>(
    mut store: C,
    rx: Incoming,
    tx: Outgoing,
    guest_resources: &[ResourceType],
    host_resources: &HashMap<Box<str>, HashMap<Box<str>, (ResourceType, ResourceType)>>,
    io_streams: &[ResourceType],
    params_ty: impl ExactSizeIterator<Item = &Type>,
    results_ty: &[Type],
    func: Func,
    observe: impl FnOnce(&[Val]) -> wasmtime::Result<()>,
) -> Result<(), CallError>
where
    C: AsContextMut,
    C::Data: WrpcView,
{
    let mut access = Direct(&mut store);
    let mut params = vec![Val::Bool(false); params_ty.len()];
    let mut rx = pin!(rx);
    for (i, (v, ty)) in zip(&mut params, params_ty).enumerate() {
        read_value(
            &mut access,
            None,
            &mut rx,
            guest_resources,
            io_streams,
            v,
            ty,
            &[i],
        )
        .await
        .with_context(|| format!("failed to decode parameter value {i}"))
        .map_err(CallError::Decode)?;
    }
    observe(&params).map_err(CallError::Call)?;
    let mut results = vec![Val::Bool(false); results_ty.len()];
    func.call_async(&mut store, &params, &mut results)
        .await
        .context("failed to call function")
        .map_err(CallError::Call)?;
    let mut access = Direct(&mut store);
    write_results(
        &mut access,
        None,
        tx,
        guest_resources,
        host_resources,
        io_streams,
        results_ty,
        results,
        |_| {},
    )
    .await
}

/// Serve one invocation from inside `Store::run_concurrent`: parameters are
/// decoded through `accessor`, `observe` sees them (and may refuse the call),
/// the function runs as a concurrent task, and the results are encoded back,
/// streams included. `scope` is the connection the handles belong to;
/// `after_encode` runs once the results are encoded and before they are
/// transmitted, so a server can record the handles they carry first.
#[allow(clippy::too_many_arguments)]
pub async fn call_concurrent_observed<T, D>(
    accessor: &wasmtime::component::Accessor<T, D>,
    scope: Option<u64>,
    rx: Incoming,
    tx: Outgoing,
    guest_resources: &[ResourceType],
    host_resources: &HashMap<Box<str>, HashMap<Box<str>, (ResourceType, ResourceType)>>,
    io_streams: &[ResourceType],
    params_ty: impl ExactSizeIterator<Item = &Type>,
    results_ty: &[Type],
    func: Func,
    observe: impl FnOnce(&[Val]) -> wasmtime::Result<()>,
    after_encode: impl FnOnce(&wasmtime::component::Accessor<T, D>),
) -> Result<(), CallError>
where
    T: WrpcView + Send + 'static,
    D: wasmtime::component::HasData + ?Sized,
{
    let mut access = ViaAccessor(accessor);
    let mut params = vec![Val::Bool(false); params_ty.len()];
    let mut rx = pin!(rx);
    for (i, (v, ty)) in zip(&mut params, params_ty).enumerate() {
        read_value(
            &mut access,
            scope,
            &mut rx,
            guest_resources,
            io_streams,
            v,
            ty,
            &[i],
        )
        .await
        .with_context(|| format!("failed to decode parameter value {i}"))
        .map_err(CallError::Decode)?;
    }
    observe(&params).map_err(CallError::Call)?;
    let mut results = vec![Val::Bool(false); results_ty.len()];
    func.call_concurrent(accessor, &params, &mut results)
        .await
        .context("failed to call function")
        .map_err(CallError::Call)?;
    write_results(
        &mut access,
        scope,
        tx,
        guest_resources,
        host_resources,
        io_streams,
        results_ty,
        results,
        |access| after_encode(access.0),
    )
    .await
}

/// Encode `results` and transmit them on `tx`, then run the deferred stream
/// writers (each on its own indexed sub-stream) to completion.
#[allow(clippy::too_many_arguments)]
async fn write_results<T, A>(
    access: &mut A,
    scope: Option<u64>,
    mut tx: Outgoing,
    guest_resources: &[ResourceType],
    host_resources: &HashMap<Box<str>, HashMap<Box<str>, (ResourceType, ResourceType)>>,
    io_streams: &[ResourceType],
    results_ty: &[Type],
    results: Vec<Val>,
    after_encode: impl FnOnce(&mut A),
) -> Result<(), CallError>
where
    T: WrpcView + 'static,
    A: StoreAccess<T>,
{
    let mut buf = BytesMut::default();
    let mut deferred = vec![];
    match (
        &rpc_result_type(host_resources, results_ty),
        results.as_slice(),
    ) {
        (None, results) => {
            for (i, (v, ty)) in zip(results, results_ty).enumerate() {
                let mut enc = ValEncoder::new(access, scope, ty, guest_resources, io_streams);
                enc.encode(v, &mut buf)
                    .with_context(|| format!("failed to encode result value {i}"))
                    .map_err(CallError::Encode)?;
                deferred.push(enc.deferred);
            }
        }
        // `result<_, rpc-eror>`
        (Some(None), [Val::Result(Ok(None))]) => {}
        // `result<T, rpc-eror>`
        (Some(Some(ty)), [Val::Result(Ok(Some(v)))]) => {
            let mut enc = ValEncoder::new(access, scope, ty, guest_resources, io_streams);
            enc.encode(v, &mut buf)
                .context("failed to encode result value 0")
                .map_err(CallError::Encode)?;
            deferred.push(enc.deferred);
        }
        (Some(..), [Val::Result(Err(Some(err)))]) => {
            let Val::Resource(err) = &**err else {
                return Err(CallError::TypeMismatch(wasmtime::Error::msg(
                    "RPC result error value is not a resource",
                )));
            };
            let err = access.with(|mut store| -> Result<Error, CallError> {
                let err = err
                    .try_into_resource(&mut store)
                    .context("RPC result error resource type mismatch")
                    .map_err(CallError::TypeMismatch)?;
                store.data_mut().delete_error(err).map_err(CallError::Table)
            })?;
            return Err(CallError::Guest(err));
        }
        _ => {
            return Err(CallError::TypeMismatch(wasmtime::Error::msg(
                "RPC result type mismatch",
            )));
        }
    }

    // Handles minted into the results exist before the caller can name them.
    after_encode(access);
    debug!("transmitting results");
    tx.write_all(&buf)
        .await
        .context("failed to transmit results")
        .map_err(CallError::Write)?;
    tx.flush()
        .await
        .context("failed to flush outgoing stream")
        .map_err(CallError::Flush)?;
    if let Err(err) = tx.shutdown().await {
        trace!(?err, "failed to shutdown outgoing stream");
    }
    try_join_all(
        zip(0.., deferred)
            .filter_map(|(i, f)| f.map(|f| (tx.index(&[i]), f)))
            .map(|(w, f)| async move {
                let w = w.map_err(wasmtime::Error::from)?;
                f(w).await
            }),
    )
    .await
    .map_err(CallError::Deferred)?;
    Ok(())
}

/// Recursively iterates the component item type and collects all exported resource types
#[instrument(level = "debug", skip_all)]
pub fn collect_item_resource_exports(
    engine: &Engine,
    ty: types::ComponentItem,
    resources: &mut impl Extend<types::ResourceType>,
) {
    match ty {
        types::ComponentItem::ComponentFunc(_)
        | types::ComponentItem::CoreFunc(_)
        | types::ComponentItem::Module(_)
        | types::ComponentItem::Type(_) => {}
        types::ComponentItem::Component(ty) => {
            collect_component_resource_exports(engine, &ty, resources);
        }

        types::ComponentItem::ComponentInstance(ty) => {
            collect_instance_resource_exports(engine, &ty, resources);
        }
        types::ComponentItem::Resource(ty) => {
            debug!(?ty, "collect resource export");
            resources.extend([ty]);
        }
    }
}

/// Recursively iterates the instance type and collects all exported resource types
#[instrument(level = "debug", skip_all)]
pub fn collect_instance_resource_exports(
    engine: &Engine,
    ty: &types::ComponentInstance,
    resources: &mut impl Extend<types::ResourceType>,
) {
    for (name, types::ComponentExtern { ty, .. }) in ty.exports(engine) {
        trace!(name, ?ty, "collect instance item resource exports");
        collect_item_resource_exports(engine, ty, resources);
    }
}

/// Recursively iterates the component type and collects all exported resource types
#[instrument(level = "debug", skip_all)]
pub fn collect_component_resource_exports(
    engine: &Engine,
    ty: &types::Component,
    resources: &mut impl Extend<types::ResourceType>,
) {
    for (name, types::ComponentExtern { ty, .. }) in ty.exports(engine) {
        trace!(name, ?ty, "collect component item resource exports");
        collect_item_resource_exports(engine, ty, resources);
    }
}

/// Iterates the component type and collects all imported resource types
#[instrument(level = "debug", skip_all)]
pub fn collect_component_resource_imports(
    engine: &Engine,
    ty: &types::Component,
    resources: &mut BTreeMap<Box<str>, HashMap<Box<str>, types::ResourceType>>,
) {
    for (name, types::ComponentExtern { ty, .. }) in ty.imports(engine) {
        match ty {
            types::ComponentItem::ComponentFunc(..)
            | types::ComponentItem::CoreFunc(..)
            | types::ComponentItem::Module(..)
            | types::ComponentItem::Type(..)
            | types::ComponentItem::Component(..) => {}
            types::ComponentItem::ComponentInstance(ty) => {
                let instance = name;
                for (name, types::ComponentExtern { ty, .. }) in ty.exports(engine) {
                    if let types::ComponentItem::Resource(ty) = ty {
                        debug!(instance, name, ?ty, "collect instance resource import");
                        if let Some(resources) = resources.get_mut(instance) {
                            resources.insert(name.into(), ty);
                        } else {
                            resources.insert(instance.into(), HashMap::from([(name.into(), ty)]));
                        }
                    }
                }
            }
            types::ComponentItem::Resource(ty) => {
                debug!(name, "collect component resource import");
                if let Some(resources) = resources.get_mut("") {
                    resources.insert(name.into(), ty);
                } else {
                    resources.insert("".into(), HashMap::from([(name.into(), ty)]));
                }
            }
        }
    }
}
