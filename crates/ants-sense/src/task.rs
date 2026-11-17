//! Personal tasks, their dependency graph and the `sense_task` tool.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Write as _};

use ants_proto::provider::{BoxFuture, ToolCall};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::items::{Item, ItemId, Values};
use crate::sanitize::{Limit, sanitize_body, sanitize_title};
use crate::store::{Items, StoreError, Write};
use crate::{Answer, Sense};

mod lease;

pub use lease::{Lease, fencing_token};

/// The only item kind that can be ready or claimed.
pub const TASK: &str = "task";

/// A task's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Open,
    InProgress,
    Blocked,
    Deferred,
    Closed,
}

impl Status {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::InProgress => "in_progress",
            Self::Blocked => "blocked",
            Self::Deferred => "deferred",
            Self::Closed => "closed",
        }
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// How a task refers to another task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DependencyKind {
    /// The referenced task must close before this task is ready.
    #[serde(rename = "blocks")]
    Blocks,
    /// The referenced task is this task's parent; this does not block readiness.
    #[serde(rename = "parent-child")]
    ParentChild,
    #[serde(rename = "related")]
    Related,
    #[serde(rename = "discovered-from")]
    DiscoveredFrom,
}

/// A relationship from this task to the referenced task.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Dependency {
    pub kind: DependencyKind,
    pub id: ItemId,
}

/// A task's readable values; identity, revision and age live on [`TaskItem::item`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Task {
    pub title: String,
    pub body: String,
    pub status: Status,
    pub priority: u8,
    pub dependencies: Vec<Dependency>,
    pub external_ref: Option<String>,
    pub lease: Option<Lease>,
}

impl Task {
    pub fn of(item: &Item) -> Option<Self> {
        if item.kind != TASK {
            return None;
        }
        let task: Self = serde_json::from_value(Value::Object(item.values.clone())).ok()?;
        let valid_lease = match (&task.lease, task.status) {
            (Some(lease), Status::InProgress) => (2..=item.revision).contains(&lease.claim_revision),
            (None, Status::InProgress) | (Some(_), _) => false,
            (None, _) => true,
        };
        (task.priority <= 4 && !task.title.trim().is_empty() && valid_lease).then_some(task)
    }

    fn values(&self) -> Values {
        serde_json::to_value(self).expect("task values").as_object().expect("task object").clone()
    }
}

/// A task together with its id, revision and provenance.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskItem {
    pub item: Item,
    pub task: Task,
}

impl TaskItem {
    pub fn of(item: Item) -> Option<Self> {
        Some(Self { task: Task::of(&item)?, item })
    }

    pub fn id(&self) -> &ItemId {
        &self.item.id
    }

    /// The task's values together with its id, revision and stamps.
    pub fn json(&self) -> Value {
        let mut values = self.task.values();
        values.insert("id".into(), json!(self.item.id));
        values.insert("revision".into(), json!(self.item.revision));
        values.insert("created".into(), json!(self.item.created));
        values.insert("updated".into(), json!(self.item.updated));
        Value::Object(values)
    }

    /// One brief line, including status, priority, id and revision.
    pub fn text(&self) -> String {
        let mut text = format!(
            "{} P{} {}: {} (revision {})",
            self.id(),
            self.task.priority,
            self.task.status,
            self.task.title,
            self.item.revision
        );
        if let Some(lease) = &self.task.lease {
            let _ = write!(text, "; thread {}, lease until {}", lease.thread, lease.expires_at.to_rfc3339());
        }
        text
    }
}

/// All tasks, in priority order (0 first), then oldest creation first, limited after sorting.
pub async fn list(items: &dyn Items, limit: usize) -> Result<Vec<TaskItem>, StoreError> {
    Ok(Graph::load(items).await?.sorted(false, limit))
}

/// Only open tasks whose blocking prerequisites are closed, in priority then creation order.
pub async fn ready(items: &dyn Items, limit: usize) -> Result<Vec<TaskItem>, StoreError> {
    Ok(Graph::load(items).await?.sorted(true, limit))
}

/// Tasks held by `thread`, including expired leases, in priority then creation order.
pub async fn claimed(
    items: &dyn Items,
    thread: &ants_proto::ThreadId,
    limit: usize,
) -> Result<Vec<TaskItem>, StoreError> {
    Ok(Graph::load(items)
        .await?
        .sorted(false, usize::MAX)
        .into_iter()
        .filter(|task| task.task.lease.as_ref().is_some_and(|lease| &lease.thread == thread))
        .take(limit)
        .collect())
}

struct Graph(BTreeMap<ItemId, TaskItem>);

impl Graph {
    async fn load(items: &dyn Items) -> Result<Self, StoreError> {
        Ok(Self(
            items
                .list(TASK, usize::MAX)
                .await?
                .into_iter()
                .filter_map(TaskItem::of)
                .map(|task| (task.id().clone(), task))
                .collect(),
        ))
    }

    fn unblocked(&self, task: &Task) -> bool {
        task.dependencies
            .iter()
            .filter(|dep| dep.kind == DependencyKind::Blocks)
            .all(|dep| self.0.get(&dep.id).is_some_and(|prerequisite| prerequisite.task.status == Status::Closed))
    }

    fn sorted(self, ready_only: bool, limit: usize) -> Vec<TaskItem> {
        let mut tasks: Vec<_> = self
            .0
            .values()
            .filter(|found| !ready_only || (found.task.status == Status::Open && self.unblocked(&found.task)))
            .cloned()
            .collect();
        tasks.sort_by(|a, b| {
            (a.task.priority, a.item.created.at, a.id()).cmp(&(b.task.priority, b.item.created.at, b.id()))
        });
        tasks.truncate(limit);
        tasks
    }

    fn validate(&self, id: &ItemId, task: &Task) -> Result<(), Answer> {
        let mut seen = BTreeSet::new();
        let mut parents = 0;
        for dep in &task.dependencies {
            if &dep.id == id {
                return Err(invalid("A task cannot refer to itself."));
            }
            if !self.0.contains_key(&dep.id) {
                return Err(invalid(format!("Dependency {} is not a task.", dep.id)));
            }
            if !seen.insert(dep) {
                return Err(invalid(format!("Duplicate dependency {}.", dep.id)));
            }
            if dep.kind == DependencyKind::ParentChild {
                parents += 1;
            }
            if dep.kind == DependencyKind::Related {
                if self.0[&dep.id]
                    .task
                    .dependencies
                    .iter()
                    .any(|other| other.kind == DependencyKind::Related && &other.id == id)
                {
                    return Err(invalid(format!("Related dependency {} exists in the other direction.", dep.id)));
                }
            } else if self.reaches(&dep.id, id) {
                return Err(invalid(format!("Dependency {} would create a cycle.", dep.id)));
            }
        }
        if parents > 1 {
            return Err(invalid("A task has at most one parent."));
        }
        Ok(())
    }

    fn reaches(&self, from: &ItemId, to: &ItemId) -> bool {
        let mut pending = vec![from];
        let mut seen = BTreeSet::new();
        while let Some(id) = pending.pop() {
            if id == to {
                return true;
            }
            if seen.insert(id)
                && let Some(task) = self.0.get(id)
            {
                pending.extend(
                    task.task.dependencies.iter().filter(|dep| dep.kind != DependencyKind::Related).map(|dep| &dep.id),
                );
            }
        }
        false
    }

    /// Guard every graph vertex read during validation: concurrent edge edits cannot each pass on an old graph.
    fn writes(&self, write: Write) -> Vec<Write> {
        let mut writes: Vec<_> = self
            .0
            .values()
            .filter(|task| task.id() != write.id())
            .map(|task| Write::Change {
                id: task.id().clone(),
                values: Values::new(),
                expected: Some(task.item.revision),
            })
            .collect();
        writes.insert(0, write);
        writes
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Action {
    Create,
    Show,
    Update,
    Claim,
    Heartbeat,
    Release,
    Close,
    List,
    #[default]
    Ready,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    #[serde(default)]
    action: Action,
    id: Option<ItemId>,
    revision: Option<u64>,
    token: Option<u64>,
    title: Option<String>,
    body: Option<String>,
    status: Option<Status>,
    priority: Option<u8>,
    dependencies: Option<Vec<Dependency>>,
    external_ref: Option<Value>,
    limit: Option<usize>,
}

impl Request {
    fn parse(call: &ToolCall) -> Result<Self, Answer> {
        let request: Self = serde_json::from_value(Value::Object(call.arguments.clone()))
            .map_err(|e| invalid(format!("Invalid task arguments: {e}.")))?;
        if request.priority.is_some_and(|p| p > 4) {
            return Err(invalid("priority must be from 0 to 4."));
        }
        if request.revision == Some(0) || request.token == Some(0) {
            return Err(invalid("revision and token must be positive integers."));
        }
        if request.limit.is_some_and(|limit| !(1..=100).contains(&limit)) {
            return Err(invalid("limit must be from 1 to 100."));
        }
        if request.dependencies.as_ref().is_some_and(|deps| deps.len() > 100) {
            return Err(invalid("At most 100 dependencies."));
        }
        let editing = request.title.is_some()
            || request.body.is_some()
            || request.status.is_some()
            || request.priority.is_some()
            || request.dependencies.is_some()
            || call.arguments.contains_key("external_ref");
        match request.action {
            Action::Create
                if request.id.is_some()
                    || request.revision.is_some()
                    || request.limit.is_some()
                    || request.token.is_some() =>
            {
                return Err(invalid("create accepts task fields, not id, revision, token or limit."));
            }
            Action::Show
                if editing || request.revision.is_some() || request.limit.is_some() || request.token.is_some() =>
            {
                return Err(invalid("show accepts only id."));
            }
            Action::List | Action::Ready
                if editing || request.id.is_some() || request.revision.is_some() || request.token.is_some() =>
            {
                return Err(invalid("list and ready accept only limit."));
            }
            Action::Update if request.limit.is_some() => return Err(invalid("update does not accept limit.")),
            Action::Claim if editing || request.limit.is_some() || request.token.is_some() => {
                return Err(invalid("claim accepts only id and an optional expected revision."));
            }
            Action::Heartbeat | Action::Release | Action::Close if editing || request.limit.is_some() => {
                return Err(invalid("heartbeat, release and close accept only id, token and an optional revision."));
            }
            _ => {}
        }
        Ok(request)
    }

    fn values(&self, old: Option<&Task>, call: &ToolCall) -> Result<Task, Answer> {
        if matches!(self.status, Some(Status::InProgress | Status::Closed)) {
            return Err(invalid("Use claim for in_progress and close for closed; status cannot bypass a lease."));
        }
        if old.is_some_and(|old| old.lease.is_some() && self.status.is_some_and(|status| status != old.status)) {
            return Err(invalid("Release the lease before changing status, or use close with the claim token."));
        }
        let title = self.title.as_deref().or_else(|| old.map(|old| old.title.as_str())).unwrap_or_default();
        let title = sanitize_title(title.trim(), Limit::Chars(200)).map_err(|e| invalid(e.to_string()))?.text;
        if title.is_empty() {
            return Err(invalid("title is required."));
        }
        let body = self.body.as_deref().or_else(|| old.map(|old| old.body.as_str())).unwrap_or_default();
        let body = sanitize_body(body.trim(), Limit::Bytes(4_096)).map_err(|e| invalid(e.to_string()))?.text;
        let external_ref = if call.arguments.contains_key("external_ref") {
            match self.external_ref.as_ref() {
                None | Some(Value::Null) => None,
                Some(Value::String(reference)) if !reference.trim().is_empty() && reference.len() <= 1_024 => {
                    let reference = sanitize_title(reference.trim(), Limit::Bytes(1_024))
                        .map_err(|error| invalid(error.to_string()))?
                        .text;
                    if reference.trim().is_empty() {
                        return Err(invalid("external_ref must be nonempty text after sanitization."));
                    }
                    Some(reference)
                }
                _ => return Err(invalid("external_ref must be nonempty text of at most 1,024 bytes, or null.")),
            }
        } else {
            old.and_then(|old| old.external_ref.clone())
        };
        Ok(Task {
            title,
            body,
            status: self.status.or_else(|| old.map(|old| old.status)).unwrap_or(Status::Open),
            priority: self.priority.or_else(|| old.map(|old| old.priority)).unwrap_or(2),
            dependencies: self
                .dependencies
                .clone()
                .or_else(|| old.map(|old| old.dependencies.clone()))
                .unwrap_or_default(),
            external_ref,
            lease: old.and_then(|old| old.lease.clone()),
        })
    }
}

/// The `sense_task` argument schema. A blocks dependency names a prerequisite; parent-child names the parent.
pub fn schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "action": {
                "enum": ["create", "show", "update", "list", "ready", "claim", "heartbeat", "release", "close"],
                "default": "ready"
            },
            "id": {"type": "string"},
            "revision": {"type": "integer", "minimum": 1, "description": "Required for update; the revision last read."},
            "token": {
                "type": "integer", "minimum": 1,
                "description": "Claim event sequence; required for heartbeat, release, close and updates while held."
            },
            "title": {"type": "string", "maxLength": 200},
            "body": {"type": "string", "description": "At most 4 KiB."},
            "status": {
                "enum": ["open", "in_progress", "blocked", "deferred", "closed"],
                "description": "create/update accept open, blocked or deferred; claim and close set the other states."
            },
            "priority": {"type": "integer", "minimum": 0, "maximum": 4, "default": 2},
            "dependencies": {
                "type": "array", "maxItems": 100,
                "description": "Replaces all relationships. blocks names a prerequisite; parent-child names the parent.",
                "items": {
                    "type": "object", "additionalProperties": false,
                    "properties": {
                        "kind": {"enum": ["blocks", "parent-child", "related", "discovered-from"]},
                        "id": {"type": "string"}
                    },
                    "required": ["kind", "id"]
                }
            },
            "external_ref": {"type": ["string", "null"], "description": "Team tracker reference; null clears it."},
            "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 20}
        }
    })
}

/// Answer a `sense_task` call.
pub fn task<'a>(sense: &'a Sense, call: &'a ToolCall) -> BoxFuture<'a, Answer> {
    Box::pin(async move { run(sense, call).await.unwrap_or_else(|answer| answer) })
}

async fn run(sense: &Sense, call: &ToolCall) -> Result<Answer, Answer> {
    let request = Request::parse(call)?;
    let items = sense.items(&call.context.ant).await.map_err(store_error)?;
    items.sync().await.map_err(store_error)?;
    let graph = Graph::load(&*items).await.map_err(store_error)?;
    match request.action {
        Action::List | Action::Ready => {
            let tasks = graph.sorted(request.action == Action::Ready, request.limit.unwrap_or(20));
            let text = if tasks.is_empty() {
                "No tasks.".to_owned()
            } else {
                tasks.iter().map(TaskItem::text).collect::<Vec<_>>().join("\n")
            };
            Ok(Answer {
                text,
                json: json!({"tasks": tasks.iter().map(TaskItem::json).collect::<Vec<_>>()}),
                is_error: false,
            })
        }
        Action::Create => {
            let task = request.values(None, call)?;
            let id = ItemId::new();
            graph.validate(&id, &task)?;
            let writes = graph.writes(Write::Create { id, kind: TASK.into(), values: task.values() });
            let item = items.write(&writes, call.context.thread.as_str()).await.map_err(store_error)?.remove(0);
            Ok(shown(TaskItem::of(item).expect("created task")))
        }
        Action::Show | Action::Update | Action::Claim | Action::Heartbeat | Action::Release | Action::Close => {
            let id = request.id.as_ref().ok_or_else(|| invalid("id is required."))?;
            let current = graph.0.get(id).ok_or_else(|| Answer::error("not_found", format!("No task {id}.")))?;
            if request.action == Action::Show {
                return lease::shown(&*items, current.clone()).await;
            }
            if request.revision.is_some_and(|expected| expected != current.item.revision) {
                return Err(conflict(current));
            }
            match request.action {
                Action::Claim => return lease::claim(&*items, &graph, current, call).await,
                Action::Heartbeat | Action::Release | Action::Close => {
                    return lease::change(&*items, current, &request, call).await;
                }
                _ => {}
            }
            let expected = request
                .revision
                .ok_or_else(|| Answer::error("revision_required", "update requires the revision last read.".into()))?;
            if current.task.lease.is_some() || request.token.is_some() {
                lease::authorize(&*items, current, request.token, call).await?;
            }
            let task = request.values(Some(&current.task), call)?;
            graph.validate(id, &task)?;
            let writes =
                graph.writes(Write::Change { id: id.clone(), values: task.values(), expected: Some(expected) });
            let written = if current.task.lease.is_some() {
                items.write_confirmed(&writes, call.context.thread.as_str()).await
            } else {
                items.write(&writes, call.context.thread.as_str()).await
            };
            let item = written.map_err(store_error)?.remove(0);
            lease::shown(&*items, TaskItem::of(item).expect("updated task")).await
        }
    }
}

fn shown(task: TaskItem) -> Answer {
    Answer { text: task.text(), json: json!({"task": task.json()}), is_error: false }
}

fn invalid(message: impl Into<String>) -> Answer {
    Answer::error("invalid", message.into())
}

fn conflict(task: &TaskItem) -> Answer {
    let mut answer = Answer::error(
        "conflict",
        format!("Task {} changed; read revision {} before retrying.", task.id(), task.item.revision),
    );
    answer.json["task"] = task.json();
    answer
}

fn store_error(error: StoreError) -> Answer {
    match error {
        StoreError::Conflict { current, .. } => match TaskItem::of(*current) {
            Some(task) => conflict(&task),
            None => Answer::error("conflict", "A related item changed; read again before retrying.".into()),
        },
        error => Answer::error("unavailable", format!("Tasks are not available: {error}")),
    }
}

#[cfg(test)]
mod tests;
