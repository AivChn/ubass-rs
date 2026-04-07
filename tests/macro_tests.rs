#![cfg(test)]

use ubass_macros::variants_array;

#[derive(Debug)]
#[variants_array]
enum TestEnum {
    One,
    Two,
    Three,
}

impl PartialEq for TestEnum {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (TestEnum::One, TestEnum::One)
                | (TestEnum::Two, TestEnum::Two)
                | (TestEnum::Three, TestEnum::Three)
        )
    }
}

#[test]
fn test_variants_array() {
    let variants = [TestEnum::One, TestEnum::Two, TestEnum::Three];
    let variants_from_macro = TestEnum::VARIANTS;
    assert_eq!(variants, variants_from_macro);
}
