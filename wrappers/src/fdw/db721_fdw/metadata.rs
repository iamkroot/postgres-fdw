use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;
use std::str::FromStr;

use serde::{de, Deserialize, Deserializer};

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct BlockStats<T> {
    pub(crate) num: u32,
    pub(crate) min: T,
    pub(crate) max: T,
    #[serde(default)]
    pub(crate) min_len: u32,
    #[serde(default)]
    pub(crate) max_len: u32,
}

pub(crate) type BSMap<T> = HashMap<u32, BlockStats<T>>;

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct BSS<T: for<'a> Deserialize<'a>> {
    #[serde(deserialize_with = "de_int_key")]
    pub(crate) block_stats: BSMap<T>,
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
    pub(crate) block_stats: Stats,
    pub(crate) num_blocks: u32,
    pub(crate) start_offset: u32,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct Metadata {
    #[serde(rename = "Table")]
    pub(crate) table_name: String,
    #[serde(rename = "Columns")]
    pub(crate) columns: HashMap<String, Column>,
    #[serde(rename = "Max Values Per Block")]
    pub(crate) max_vals_per_block: u32,
}

impl Metadata {
    pub(crate) fn from_slice(slice: &[u8]) -> serde_json::Result<Self> {
        serde_json::from_slice(slice)
    }
    pub(crate) fn num_rows(&self) -> u64 {
        let Some(col) = self.columns.values().next() else {
            return 0;
        };
        match &col.block_stats {
            Stats::Float(BSS { block_stats }) => block_stats.values().map(|v| v.num as u64).sum(),
            Stats::Int(BSS { block_stats }) => block_stats.values().map(|v| v.num as u64).sum(),
            Stats::Str(BSS { block_stats }) => block_stats.values().map(|v| v.num as u64).sum(),
        }
    }
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
