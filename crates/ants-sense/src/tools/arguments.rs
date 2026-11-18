//! A tool call's arguments as the deserializer of a tool's request: each argument's error names it, and an argument
//! given as null is taken as not given.

use serde::Deserializer;
use serde::de::value::BorrowedStrDeserializer;
use serde::de::{DeserializeSeed, Error as _, MapAccess, Visitor};
use serde_json::map::Iter;
use serde_json::{Error, Map, Value};

pub(super) struct Arguments<'a>(pub &'a Map<String, Value>);

impl<'de> Deserializer<'de> for Arguments<'de> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_map(Entries { entries: self.0.iter(), value: None })
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf option unit unit_struct
        newtype_struct seq tuple tuple_struct map struct enum identifier ignored_any
    }
}

struct Entries<'a> {
    entries: Iter<'a>,
    value: Option<(&'a str, &'a Value)>,
}

impl<'de> MapAccess<'de> for Entries<'de> {
    type Error = Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>, Error> {
        let Some((name, value)) = self.entries.by_ref().find(|(_, value)| !value.is_null()) else {
            return Ok(None);
        };
        self.value = Some((name, value));
        seed.deserialize(BorrowedStrDeserializer::new(name)).map(Some)
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value, Error> {
        let (name, value) = self.value.take().ok_or_else(|| Error::custom("a value before its name"))?;
        seed.deserialize(value).map_err(|e| Error::custom(format!("{name}: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde::Deserialize;
    use serde_json::json;

    use super::*;

    #[derive(Debug, Default, PartialEq, Deserialize)]
    struct Request {
        limit: Option<u64>,
        #[serde(default)]
        history: bool,
        files: Option<BTreeMap<String, Option<String>>>,
    }

    fn read(arguments: Value) -> Result<Request, Error> {
        let Value::Object(arguments) = arguments else { panic!("arguments are an object") };
        Request::deserialize(Arguments(&arguments))
    }

    #[test]
    fn an_argument_given_as_null_is_not_given_but_a_null_inside_one_stays() {
        assert_eq!(read(json!({"limit": null, "history": null})).unwrap(), Request::default());
        let files = BTreeMap::from([("a.sh".to_owned(), None), ("b.sh".to_owned(), Some("x".to_owned()))]);
        assert_eq!(read(json!({"files": {"a.sh": null, "b.sh": "x"}})).unwrap().files, Some(files));
    }

    #[test]
    fn an_error_names_its_argument() {
        let error = read(json!({"history": true, "limit": "ten"})).unwrap_err().to_string();
        assert!(error.starts_with("limit: invalid type: string \"ten\""), "{error}");
    }
}
