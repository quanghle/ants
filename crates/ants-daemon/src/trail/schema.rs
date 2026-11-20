//! The JSON a step's reply carries for an agent() call given `schema=`, and its check against that schema: a small
//! subset of JSON Schema (`type`, `properties`, `required`, `items`, `enum` and `additionalProperties: false`), enough
//! to tell an ant what shape to reply in and to name what its reply got wrong.

use serde_json::{Map, Value};

/// The JSON of `text`: all of it, else its first fenced block (```` ```json ````, or a bare ```` ``` ````); refused
/// when nested deeper than a run takes ([`ants_trail::MAX_DEPTH`]).
pub(super) fn reply_json(text: &str) -> Result<Value, String> {
    let value = parsed(text)?;
    ants_trail::check_depth(&value).map_err(|e| format!("the reply's JSON is refused: {e}"))?;
    Ok(value)
}

fn parsed(text: &str) -> Result<Value, String> {
    match serde_json::from_str(text.trim()) {
        Ok(value) => Ok(value),
        Err(e) => match fenced(text) {
            Some(block) => serde_json::from_str(block.trim())
                .map_err(|e| format!("the fenced block of the reply is no JSON ({e})")),
            None => Err(format!("the reply is no JSON ({e})")),
        },
    }
}

/// The text inside the first fenced block of `text`, after its opening line.
fn fenced(text: &str) -> Option<&str> {
    let start = text.find("```")?;
    let after = &text[start + 3..];
    let body = &after[after.find('\n')? + 1..];
    Some(&body[..body.find("```")?])
}

/// Whether `value` matches `schema`; else the first mismatch, naming where it is (`$` for the whole value).
pub(super) fn check(schema: &Value, value: &Value) -> Result<(), String> {
    at(schema, value, "$")
}

fn at(schema: &Value, value: &Value, path: &str) -> Result<(), String> {
    let Some(schema) = schema.as_object() else { return Ok(()) };
    if let Some(types) = schema.get("type") {
        let types: Vec<&str> = match types {
            Value::String(one) => vec![one.as_str()],
            Value::Array(many) => many.iter().filter_map(Value::as_str).collect(),
            _ => Vec::new(),
        };
        if !types.is_empty() && !types.iter().any(|t| is(t, value)) {
            return Err(format!("{path} is {}, not {}", kind(value), types.join(" or ")));
        }
    }
    if let Some(Value::Array(allowed)) = schema.get("enum")
        && !allowed.contains(value)
    {
        let allowed: Vec<String> = allowed.iter().map(Value::to_string).collect();
        return Err(format!("{path} is {value}, not one of {}", allowed.join(", ")));
    }
    if let Value::Object(object) = value {
        object_at(schema, object, path)?;
    }
    if let (Value::Array(items), Some(item)) = (value, schema.get("items")) {
        for (i, value) in items.iter().enumerate() {
            at(item, value, &format!("{path}[{i}]"))?;
        }
    }
    Ok(())
}

fn object_at(schema: &Map<String, Value>, object: &Map<String, Value>, path: &str) -> Result<(), String> {
    let properties = schema.get("properties").and_then(Value::as_object);
    if let Some(Value::Array(required)) = schema.get("required")
        && let Some(missing) = required.iter().filter_map(Value::as_str).find(|key| !object.contains_key(*key))
    {
        return Err(format!("{path} has no {missing:?}"));
    }
    for (key, value) in object {
        match properties.and_then(|properties| properties.get(key)) {
            Some(property) => at(property, value, &format!("{path}.{key}"))?,
            None if schema.get("additionalProperties") == Some(&Value::Bool(false)) => {
                return Err(format!("{path} has {key:?}, which the schema does not take"));
            }
            None => {}
        }
    }
    Ok(())
}

fn is(kind: &str, value: &Value) -> bool {
    match kind {
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "number" => value.is_number(),
        "integer" => value.is_i64() || value.is_u64() || value.as_f64().is_some_and(|f| f.fract() == 0.0),
        "boolean" => value.is_boolean(),
        "null" => value.is_null(),
        _ => true,
    }
}

fn kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{check, reply_json};

    #[test]
    fn reply_json_takes_the_whole_text_or_its_first_fenced_block() {
        assert_eq!(reply_json(" {\"a\": 1} ").unwrap(), json!({"a": 1}));
        let fenced = "Here it is:\n```json\n{\"a\": [1, 2]}\n```\nand more ```{}```";
        assert_eq!(reply_json(fenced).unwrap(), json!({"a": [1, 2]}));
        assert!(reply_json("no JSON here").is_err());
        assert!(reply_json("```json\nnot json\n```").unwrap_err().contains("fenced block"));
    }

    #[test]
    fn reply_json_refuses_json_nested_deeper_than_a_run_takes() {
        let nested = |depth: usize| format!("{}1{}", "[".repeat(depth), "]".repeat(depth));
        assert!(reply_json(&nested(ants_trail::MAX_DEPTH)).is_ok());
        let deeper = reply_json(&nested(ants_trail::MAX_DEPTH + 1)).unwrap_err();
        assert!(deeper.contains("more than 100 deep"), "{deeper}");
    }

    #[test]
    fn check_names_the_first_mismatch_by_its_path() {
        let schema = json!({
            "type": "object",
            "required": ["name", "tags"],
            "additionalProperties": false,
            "properties": {
                "name": {"type": "string"},
                "size": {"type": ["integer", "null"]},
                "tags": {"type": "array", "items": {"enum": ["a", "b"]}},
            },
        });
        assert_eq!(check(&schema, &json!({"name": "x", "size": 3, "tags": ["a"]})), Ok(()));
        assert_eq!(check(&schema, &json!({"name": "x", "size": null, "tags": []})), Ok(()));
        assert_eq!(check(&schema, &json!([])), Err("$ is an array, not object".to_owned()));
        assert_eq!(check(&schema, &json!({"name": "x"})), Err("$ has no \"tags\"".to_owned()));
        assert_eq!(check(&schema, &json!({"name": 1, "tags": []})), Err("$.name is a number, not string".to_owned()));
        assert_eq!(
            check(&schema, &json!({"name": "x", "tags": ["a", "c"]})),
            Err("$.tags[1] is \"c\", not one of \"a\", \"b\"".to_owned())
        );
        assert_eq!(
            check(&schema, &json!({"name": "x", "tags": [], "more": 1})),
            Err("$ has \"more\", which the schema does not take".to_owned())
        );
        assert_eq!(
            check(&schema, &json!({"name": "x", "size": 1.5, "tags": []})).unwrap_err(),
            "$.size is a number, not integer or null"
        );
    }
}
