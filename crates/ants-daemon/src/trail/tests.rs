use serde_json::json;

use super::schema::{check, reply_json};
use super::steps::AgentArgs;

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

#[test]
fn an_agent_call_s_tip_key_is_one_line() {
    let args = AgentArgs::of(&json!({"prompt": "fix it", "tips": "fix the build"})).unwrap();
    assert_eq!(args.tips.as_deref(), Some("fix the build"));
    for bad in ["", "two\nlines"] {
        let e = AgentArgs::of(&json!({"prompt": "fix it", "tips": bad})).err().expect("refused");
        assert!(e.starts_with("agent() names no tip key as "), "{e}");
    }
}
