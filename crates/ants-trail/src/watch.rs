//! A watcher's check by a trail: one run of its script as a watcher ([`Run::watching`]), its reads answered by the
//! host as they come, to the script's result.

use ants_proto::{CallAnswer, TrailFunction};
use serde_json::Value;

use crate::protocol::Limits;
use crate::run::{Run, Status};
use crate::runner::Worker;

/// Runs `script` with `args` as a watcher's check in `worker`, answering each of its reads with `read`, and holding
/// what `computing` gives while the worker computes. Returns the script's result, or why it failed.
pub async fn check<R, C, G>(
    worker: &Worker,
    script: &str,
    args: Value,
    read: impl Fn(TrailFunction, Value) -> R,
    computing: impl Fn() -> C,
) -> Result<Value, String>
where
    R: Future<Output = CallAnswer>,
    C: Future<Output = G>,
{
    let mut run = Run::new(script, None, args, Limits::default()).map_err(|e| e.to_string())?.watching();
    loop {
        let progress = {
            let _computing = computing().await;
            run.resume(worker).await.map_err(|e| e.to_string())?
        };
        match run.status() {
            Status::Done { result } => return Ok(result.clone()),
            Status::Failed { error, .. } => return Err(error.clone()),
            Status::Running => {}
        }
        if progress.calls.is_empty() {
            return Err("the script waits on no read".to_owned());
        }
        for call in progress.calls {
            let answer = read(call.function, call.args).await;
            run.answer(call.call, answer).map_err(|e| e.to_string())?;
        }
    }
}
