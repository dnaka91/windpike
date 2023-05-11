#[derive(Clone, Copy, Debug)]
pub(crate) enum ParticleType {
    Null = 0,
    Integer,
    Float,
    String,
    Blob,
    Bool = 17,
    Hll,
    Map,
    List,
    GeoJson = 23,
}

#[derive(Debug, thiserror::Error)]
#[error("invalid particle type `{0}`")]
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
            17 => Self::Bool,
            18 => Self::Hll,
            19 => Self::Map,
            20 => Self::List,
            23 => Self::GeoJson,
            _ => return Err(ParseParticleError(value)),
        })
    }
}
