pub mod normalizer;

#[cfg(not(feature = "unicode"))]
pub type DefaultTextNormalizer = normalizer::LowercaseNormalizer;
#[cfg(feature = "unicode")]
pub type DefaultTextNormalizer = normalizer::unicode::UnicodeNormalizer;
