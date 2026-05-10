use std::pin::Pin;

use tokio::sync::OnceCell;

use crate::DucklakeResult;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub struct AsyncLazy<T, A = ()> {
    value: OnceCell<T>,
    init: Box<dyn Fn(A) -> BoxFuture<'static, DucklakeResult<T>> + Send + Sync>,
}

impl<T, A> AsyncLazy<T, A> {
    pub fn new<F, Fut>(init: F) -> Self
    where
        F: Fn(A) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = DucklakeResult<T>> + Send + 'static,
    {
        Self {
            value: OnceCell::new(),
            init: Box::new(move |arg| {
                let fut = init(arg);
                Box::pin(fut) as BoxFuture<'static, DucklakeResult<T>>
            }),
        }
    }

    pub async fn get_with_arg(&self, arg: A) -> DucklakeResult<&T> {
        self.value.get_or_try_init(|| (self.init)(arg)).await
    }
}

impl<T> AsyncLazy<T, ()> {
    pub async fn get(&self) -> DucklakeResult<&T> {
        self.get_with_arg(()).await
    }
}
