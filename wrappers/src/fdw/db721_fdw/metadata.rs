use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;
use std::str::FromStr;

use serde::{de, Deserialize, Deserializer};

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct BlockStats<T> {
    num: u32,
    min: T,
    max: T,
    #[serde(default)]
    min_len: u32,
    #[serde(default)]
    max_len: u32,
}

pub(crate) type BSMap<T> = HashMap<usize, BlockStats<T>>;

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct BSS<T: for<'a> Deserialize<'a>> {
    #[serde(deserialize_with = "de_int_key")]
    block_stats: BSMap<T>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(tag = "type")]
pub(crate) enum Stats {
    #[serde(rename = "float")]
    Float(BSS<f32>),
    #[serde(rename = "int")]
    Int(BSS<i32>),
    #[serde(rename = "str")]
    Str(BSS<String>),
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct Column {
    #[serde(flatten)]
    block_stats: Stats,
    num_blocks: u32,
    start_offset: u32,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct Metadata {
    #[serde(rename = "Table")]
    table_name: String,
    #[serde(rename = "Columns")]
    columns: HashMap<String, Column>,
    #[serde(rename = "Max Values Per Block")]
    max_vals_per_block: u32,
}

/// Taken from https://github.com/serde-rs/json/issues/560#issuecomment-532054058
fn de_int_key<'de, D, K, V>(deserializer: D) -> Result<HashMap<K, V>, D::Error>
where
    D: Deserializer<'de>,
    K: Eq + Hash + FromStr,
    K::Err: fmt::Display,
    V: Deserialize<'de>,
{
    struct KeySeed<K> {
        k: PhantomData<K>,
    }

    impl<'de, K> de::DeserializeSeed<'de> for KeySeed<K>
    where
        K: FromStr,
        K::Err: fmt::Display,
    {
        type Value = K;

        fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_str(self)
        }
    }

    impl<'de, K> de::Visitor<'de> for KeySeed<K>
    where
        K: FromStr,
        K::Err: fmt::Display,
    {
        type Value = K;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a string")
        }

        fn visit_str<E>(self, string: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            K::from_str(string).map_err(de::Error::custom)
        }
    }

    struct MapVisitor<K, V> {
        k: PhantomData<K>,
        v: PhantomData<V>,
    }

    impl<'de, K, V> de::Visitor<'de> for MapVisitor<K, V>
    where
        K: Eq + Hash + FromStr,
        K::Err: fmt::Display,
        V: Deserialize<'de>,
    {
        type Value = HashMap<K, V>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a map")
        }

        fn visit_map<A>(self, mut input: A) -> Result<Self::Value, A::Error>
        where
            A: de::MapAccess<'de>,
        {
            let mut map = HashMap::new();
            while let Some((k, v)) =
                input.next_entry_seed(KeySeed { k: PhantomData }, PhantomData)?
            {
                map.insert(k, v);
            }
            Ok(map)
        }
    }

    deserializer.deserialize_map(MapVisitor {
        k: PhantomData,
        v: PhantomData,
    })
}
