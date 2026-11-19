//! JSON values in and out of the interpreter: a run's args and a step's answer go in, a step's args and the script's
//! result come out.

use monty_types::{MontyObject, ObjectRef};
use num_bigint::BigInt;
use serde_json::{Map, Number, Value};

/// Most lists and dicts, arrays and objects in JSON, nested in one value a run takes or makes: its args, a call's
/// args, an answer and the result. Far enough under the 127 serde_json reads for the levels a worker's request or
/// response, and the daemon's records, put around such a value.
pub const MAX_DEPTH: usize = 100;
/// Most values visited, so a value sharing its parts many times over cannot blow up.
const MAX_NODES: usize = 1_000_000;

pub(crate) fn to_monty(value: &Value) -> MontyObject {
    match value {
        Value::Null => MontyObject::none(),
        Value::Bool(b) => MontyObject::bool(*b),
        Value::Number(n) => match (n.as_i64(), n.as_f64()) {
            (Some(i), _) => MontyObject::int(i),
            (None, Some(f)) if n.is_f64() => MontyObject::float(f),
            // A u64 past i64::MAX.
            _ => MontyObject::bigint(n.as_u64().map_or_default(BigInt::from)),
        },
        Value::String(s) => MontyObject::string(s.as_str()),
        Value::Array(items) => MontyObject::list(items.iter().map(to_monty)),
        Value::Object(map) => {
            MontyObject::dict(map.iter().map(|(k, v)| (MontyObject::string(k.as_str()), to_monty(v))))
        }
    }
}

/// Converts a value made of None, bools, ints, floats, strings, lists, tuples and dicts with string keys; anything
/// else is an error naming its type.
pub(crate) fn from_monty(value: ObjectRef<'_>) -> Result<Value, String> {
    let mut nodes = 0;
    convert(value, 0, &mut nodes)
}

/// Whether `value` nests at most [`MAX_DEPTH`] arrays and objects; else why not.
pub fn check_depth(value: &Value) -> Result<(), String> {
    fn within(value: &Value, room: usize) -> bool {
        match value {
            Value::Array(items) => room > 0 && items.iter().all(|item| within(item, room - 1)),
            Value::Object(map) => room > 0 && map.values().all(|item| within(item, room - 1)),
            _ => true,
        }
    }
    if within(value, MAX_DEPTH) { Ok(()) } else { Err(too_deep()) }
}

fn too_deep() -> String {
    format!("the value nests lists and dicts more than {MAX_DEPTH} deep")
}

/// `depth` counts the lists and dicts around `value`.
fn convert(value: ObjectRef<'_>, depth: usize, nodes: &mut usize) -> Result<Value, String> {
    *nodes += 1;
    if *nodes > MAX_NODES {
        return Err("the value is too large".to_owned());
    }
    if depth >= MAX_DEPTH && matches!(value.type_name(), "list" | "tuple" | "dict") {
        return Err(too_deep());
    }
    let unsupported = || Err(format!("a {} is not a JSON value", value.type_name()));
    match value.type_name() {
        "NoneType" => Ok(Value::Null),
        "bool" => Ok(Value::Bool(value.as_bool().unwrap_or_default())),
        "int" => match (value.as_int(), value.py_repr().parse::<u64>()) {
            (Some(i), _) => Ok(Value::from(i)),
            (None, Ok(u)) => Ok(Value::from(u)),
            (None, Err(_)) => Err("an int past 64 bits is not a JSON value".to_owned()),
        },
        "float" => value.as_float().and_then(Number::from_f64).map_or_else(unsupported, |n| Ok(Value::Number(n))),
        "str" => Ok(Value::String(value.as_str().unwrap_or_default().to_owned())),
        "list" | "tuple" => value
            .items()
            .unwrap_or_default()
            .into_iter()
            .map(|item| convert(item, depth + 1, nodes))
            .collect::<Result<_, _>>()
            .map(Value::Array),
        "dict" => {
            let mut map = Map::new();
            for (key, item) in value.pairs().unwrap_or_default() {
                let Some(key) = key.as_str() else {
                    return Err(format!("a dict key {} is not a string", key.py_repr()));
                };
                map.insert(key.to_owned(), convert(item, depth + 1, nodes)?);
            }
            Ok(Value::Object(map))
        }
        _ => unsupported(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn json_values_round_trip() {
        let value = json!({"a": [1, -2, 3.5, "x", null, true], "b": {"c": []}, "big": u64::MAX});
        assert_eq!(from_monty(to_monty(&value).as_ref()), Ok(value));
    }

    #[test]
    fn what_json_cannot_hold_is_refused() {
        let set = MontyObject::set([MontyObject::int(1)]);
        let list = MontyObject::list([MontyObject::tuple([MontyObject::int(2)]), set]);
        assert_eq!(from_monty(list.as_ref()), Err("a set is not a JSON value".to_owned()));
        let keyed = MontyObject::dict([(MontyObject::int(1), MontyObject::none())]);
        assert_eq!(from_monty(keyed.as_ref()), Err("a dict key 1 is not a string".to_owned()));
    }

    fn nested(depth: usize) -> Value {
        (0..depth).fold(json!(1), |inner, _| json!([inner]))
    }

    #[test]
    fn values_nest_at_most_max_depth_deep() {
        let deepest = nested(MAX_DEPTH);
        assert_eq!(check_depth(&deepest), Ok(()));
        assert_eq!(from_monty(to_monty(&deepest).as_ref()), Ok(deepest));
        let deeper = nested(MAX_DEPTH + 1);
        assert_eq!(check_depth(&deeper), Err(too_deep()));
        assert_eq!(from_monty(to_monty(&deeper).as_ref()), Err(too_deep()));
        assert_eq!(check_depth(&json!({"a": nested(MAX_DEPTH)})), Err(too_deep()));
    }
}
