pub trait TextNormalizer {
    fn normalize(&self, text: &str) -> String;
}

#[derive(Default)]
pub struct NoopNormalizer;

impl TextNormalizer for NoopNormalizer {
    fn normalize(&self, text: &str) -> String {
        text.to_string()
    }
}

#[derive(Default)]
pub struct LowercaseNormalizer;

impl TextNormalizer for LowercaseNormalizer {
    fn normalize(&self, text: &str) -> String {
        text.to_lowercase()
    }
}

#[cfg(feature = "unicode")]
pub mod unicode {
    use super::*;
    use unicode_normalization::UnicodeNormalization;

    #[derive(Default)]
    pub struct UnicodeNormalizer;

    impl TextNormalizer for UnicodeNormalizer {
        fn normalize(&self, text: &str) -> String {
            // This algorithm removes any diacritics and lower cases all characters.
            // https://en.wikipedia.org/wiki/Unicode_equivalence
            // https://towardsdatascience.com/difference-between-nfd-nfc-nfkd-and-nfkc-explained-with-python-code-e2631f96ae6c
            text.nfkd()
                .filter(|c| match *c {
                    // https://en.wikipedia.org/wiki/Combining_character#Unicode_ranges
                    '\u{0300}'..='\u{036F}'
                    | '\u{1AB0}'..='\u{1AFF}'
                    | '\u{1DC0}'..='\u{1DFF}'
                    | '\u{20D0}'..='\u{20FF}'
                    | '\u{FE20}'..='\u{FE2F}' => false,
                    _ => true,
                })
                .flat_map(char::to_lowercase)
                .collect()
        }
    }
}
