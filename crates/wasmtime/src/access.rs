//! How the codec reaches the store: directly, from a plain `Store` (the
//! synchronous serving path), or through a concurrent [`Accessor`] (a task
//! spawned inside `Store::run_concurrent`, where the store is only ever
//! reachable in short synchronous closures).

use wasmtime::component::{Accessor, HasData};
use wasmtime::{AsContextMut, StoreContextMut};

/// Temporary, synchronous access to a store's context.
pub trait StoreAccess<T: 'static> {
    fn with<R>(&mut self, f: impl FnOnce(StoreContextMut<'_, T>) -> R) -> R;
}

/// A store (or context) held directly.
pub struct Direct<S>(pub S);

impl<S: AsContextMut> StoreAccess<S::Data> for Direct<S> {
    fn with<R>(&mut self, f: impl FnOnce(StoreContextMut<'_, S::Data>) -> R) -> R {
        f(self.0.as_context_mut())
    }
}

/// A store reached through a concurrent accessor.
pub struct ViaAccessor<'a, T: 'static, D: HasData + ?Sized>(pub &'a Accessor<T, D>);

impl<T: 'static, D: HasData + ?Sized> StoreAccess<T> for ViaAccessor<'_, T, D> {
    fn with<R>(&mut self, f: impl FnOnce(StoreContextMut<'_, T>) -> R) -> R {
        self.0.with(|mut access| f(access.as_context_mut()))
    }
}
