#[derive(Clone, Copy, Debug)]
pub(crate) enum ParticleType {
    // Server particle types. Unsupported types are commented out.
    Null = 0,
    Integer,
    Float,
    String,
    Blob,
    // TIMESTAMP       = 5,
    Digest = 6,
    // JBLOB  = 7,
    // CSHARP_BLOB     = 8,
    // PYTHON_BLOB     = 9,
    // RUBY_BLOB       = 10,
    // PHP_BLOB        = 11,
    // ERLANG_BLOB     = 12,
    // SEGMENT_POINTER = 13,
    // RTA_LIST        = 14,
    // RTA_DICT        = 15,
    // RTA_APPEND_DICT = 16,
    // RTA_APPEND_LIST = 17,
    // LUA_BLOB        = 18,
    Hll = 18,
    Map,
    List,
    Ldt,
    GeoJson = 23,
}

#[derive(Debug, thiserror::Error)]
#[error("Invalid particle type `{0}`")]
pub struct ParseParticleError(u8);

impl TryFrom<u8> for ParticleType {
    type Error = ParseParticleError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            0 => Self::Null,
            1 => Self::Integer,
            2 => Self::Float,
            3 => Self::String,
            4 => Self::Blob,
            // 5 => ParticleType::TIMESTAMP      ,
            6 => Self::Digest,
            // 7 => ParticleType::JBLOB ,
            // 8 => ParticleType::CSHARP_BLOB    ,
            // 9 => ParticleType::PYTHON_BLOB    ,
            // 10 => ParticleType::RUBY_BLOB      ,
            // 11 => ParticleType::PHP_BLOB       ,
            // 12 => ParticleType::ERLANG_BLOB    ,
            // 13 => ParticleType::SEGMENT_POINTER,
            // 14 => ParticleType::RTA_LIST       ,
            // 15 => ParticleType::RTA_DICT       ,
            // 16 => ParticleType::RTA_APPEND_DICT,
            // 17 => ParticleType::RTA_APPEND_LIST,
            // 18 => ParticleType::LUA_BLOB       ,
            18 => Self::Hll,
            19 => Self::Map,
            20 => Self::List,
            21 => Self::Ldt,
            23 => Self::GeoJson,
            _ => return Err(ParseParticleError(value)),
        })
    }
}
