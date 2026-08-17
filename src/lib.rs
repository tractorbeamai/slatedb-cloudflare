use worker::*;

const COUNTER_BINDING: &str = "COUNTER";
const COUNT_KEY: &str = "count";

#[event(fetch, respond_with_errors)]
pub async fn main(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    Router::new()
        .get("/", |_, _| {
            Response::ok("slatedb-cloudflare worker is running")
        })
        .on_async("/objects/:name", |_, ctx| async move {
            let name = ctx.param("name").map(String::as_str).unwrap_or("default");
            let namespace = ctx.durable_object(COUNTER_BINDING)?;
            let stub = namespace.id_from_name(name)?.get_stub()?;
            stub.fetch_with_str("https://counter.internal/").await
        })
        .run(req, env)
        .await
}
#[durable_object]
pub struct Counter {
    state: State,
}

impl DurableObject for Counter {
    fn new(state: State, _env: Env) -> Self {
        Self { state }
    }

    async fn fetch(&self, _req: Request) -> Result<Response> {
        let storage = self.state.storage();
        let count = storage.get::<u64>(COUNT_KEY).await?.unwrap_or_default() + 1;
        storage.put(COUNT_KEY, count).await?;

        Response::ok(format!("durable object request count: {count}"))
    }
}
