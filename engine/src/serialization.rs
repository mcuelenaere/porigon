use fst::Map;
use serde::{Serialize, Deserialize, Serializer, Deserializer};

pub struct SerializableMap(Map<Vec<u8>>);

impl Serialize for SerializableMap {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error> where
        S: Serializer {
        serializer.serialize_bytes(self.0.as_ref().as_bytes())
    }
}

impl AsRef<Map<Vec<u8>>> for SerializableMap {
    fn as_ref(&self) -> &Map<Vec<u8>> {
        &self.0
    }
}

impl From<Map<Vec<u8>>> for SerializableMap {
    fn from(m: Map<Vec<u8>>) -> Self {
        Self(m)
    }
}

struct FstMapVisitor;

impl<'de> serde::de::Visitor<'de> for FstMapVisitor {
    type Value = SerializableMap;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("A byte slice, containing a serialized FST map")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
    {
        self.visit_byte_buf(v.to_vec())
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
    {
        Map::new(v).map_err(serde::de::Error::custom).map(SerializableMap)
    }
}

impl<'de> Deserialize<'de> for SerializableMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error> where
        D: Deserializer<'de> {
        deserializer.deserialize_bytes(FstMapVisitor)
    }
}