use super::serialize::*;
use ubass_macros::*;

#[derive(PacketSerialize, PacketDeserialize)]
struct TestUnit();
#[derive(PacketSerialize, PacketDeserialize)]
struct TestTupleByte(u8);
#[derive(PacketSerialize, PacketDeserialize)]
struct TestTupleMultiple(u16, u32, i32);
#[derive(PacketSerialize, PacketDeserialize)]
struct TestTupleComplex(TestUnit, u8, TestTupleByte);
#[derive(PacketSerialize, PacketDeserialize)]
struct TestNamedUnit {}
#[derive(PacketSerialize, PacketDeserialize)]
struct TestNamedByte {
    byte: u8,
}
#[derive(PacketSerialize, PacketDeserialize)]
struct TestNamedMultiple {
    byte: u8,
    word: u16,
    reg: u64,
}
#[derive(PacketSerialize, PacketDeserialize)]
struct TestNamedComplex {
    test_byte: TestNamedByte,
    test_tuple: TestTupleMultiple,
    word: u16,
}
