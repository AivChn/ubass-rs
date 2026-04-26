#![cfg(test)]

use ubass::prelude::*;
use ubass_macros::Serialize;

#[derive(Serialize, Clone, Copy, PartialEq, Debug, Eq, PartialOrd, Ord)]
#[repr(u8)]
#[variants_array]
enum TestEnum {
    One = 1 << 0,
    Two = 1 << 1,
    Three = 1 << 2,
}

#[cfg(test)]
mod flags_test {
    use super::*;
    use ubass::utils::Flags;

    fn test_enum_vec_equals(mut rhs: Vec<TestEnum>, mut lhs: Vec<TestEnum>) -> bool {
        rhs.sort();
        rhs.dedup();
        lhs.sort();
        lhs.dedup();

        rhs == lhs
    }

    #[test]
    fn test_variants_array() {
        let variants = [TestEnum::One, TestEnum::Two, TestEnum::Three];
        let variants_from_macro = TestEnum::VARIANTS;
        assert_eq!(variants, variants_from_macro);
    }

    #[derive(Flags, Clone, Copy, Debug)]
    #[flagtype(TestEnum)]
    struct BitFlagTest(u8);

    #[test]
    fn constructing_bit_flag() {
        let bit_flags = BitFlagTest::construct(&[TestEnum::One, TestEnum::Three]);
        assert_eq!(bit_flags.0, 0b101);
    }

    #[test]
    fn checking_contains() {
        let bit_flags = BitFlagTest::construct(&[TestEnum::Two]);
        assert!(bit_flags.contains(TestEnum::Two));
    }

    #[test]
    fn does_not_contain() {
        let bit_flags = BitFlagTest::construct(&[TestEnum::One]);
        assert!(!bit_flags.contains(TestEnum::Two));
    }

    #[test]
    fn setting_bit_flag() {
        let bit_flags = BitFlagTest::none();
        assert_eq!(bit_flags.set(TestEnum::Two).0, 0b10);
    }

    #[test]
    fn unsetting_bit_flag() {
        let bit_flags = BitFlagTest::construct(&TestEnum::VARIANTS);
        assert_eq!(bit_flags.unset(TestEnum::One).0, 0b110);
    }

    #[test]
    fn deconstructing_bit_flag() {
        let bit_flags = BitFlagTest::construct(&TestEnum::VARIANTS);
        assert!(test_enum_vec_equals(
            bit_flags.deconstruct(),
            Vec::from(TestEnum::VARIANTS)
        ));
    }

    #[test]
    fn setting_and_unsetting_from_constructed_bit_flags() {
        let bit_flags = BitFlagTest::construct(&[TestEnum::One]);
        assert!(bit_flags.contains(TestEnum::One));
        let bit_flags = bit_flags.set(TestEnum::Two);
        assert_eq!(bit_flags.0, 0b11);
        let bit_flags = bit_flags.unset(TestEnum::One);
        assert_eq!(bit_flags.0, 0b10);
    }

    #[test]
    fn setting_a_set_bit() {
        let bit_flags = BitFlagTest::construct(&[TestEnum::One]);
        assert_eq!(bit_flags.set(TestEnum::One).0, 1);
    }

    #[test]
    fn unsetting_an_unset_bit() {
        let bit_flags = BitFlagTest::construct(&[TestEnum::Two]);
        assert_eq!(bit_flags.unset(TestEnum::One).0, 0b10);
    }
}

#[cfg(test)]
mod packet_fields {
    use std::vec;

    use ubass::{
        manager::packets,
        packet_processor::fingerprint::Payload,
        prelude::{
            Timestamp,
            packets::{BatchID, BytePosition, Options, SessionId, Version},
        },
        utils::Flags,
    };

    #[test]
    fn get_payload_from_data() {
        let mut packet = packets::DataPacket {
            version: Version::CURRENT_VERSION,
            opts: Options::none(),
            packet_type: packets::PacketType::Data,
            batch_id: BatchID::new(9),
            fec_info: packets::FECInfo {
                batch_size: 9,
                batch_pos: 2,
                recovery_count: 5,
            },
            session_id: SessionId::new(1888),
            timestamp: Timestamp(120),
            byte_range_start: BytePosition(8),
            payload: vec![1u8, 2, 3, 4, 5].into(),
        };

        assert_eq!(packet.payload(), &mut vec![1, 2, 3, 4, 5]);
    }
}

#[cfg(test)]
mod serialization {
    use super::*;

    #[derive(Debug, Serialize, PartialEq, Eq)]
    struct TestWrapper(u32);

    #[test]
    fn serialize_wrapper_struct() {
        let w = TestWrapper(13);
        let mut buf = [0u8; 4];
        assert!(w.serialize(&mut buf).is_ok());
        assert_eq!(buf, 13u32.to_be_bytes())
    }

    #[test]
    fn deserialize_to_wrapper_struct() {
        let w = TestWrapper::deserialize(&12345u32.to_be_bytes());
        assert_eq!(w, Ok(TestWrapper(12345)));
    }

    #[test]
    fn serialize_modify_deserialize_wrapper() {
        let w1 = TestWrapper(1);
        let mut buf = [0u8; 4];
        assert!(w1.serialize(&mut buf).is_ok());
        let mut modified = buf;
        modified[1] = 2;
        assert_eq!(TestWrapper::deserialize(&buf), Ok(TestWrapper(1)));
        assert_ne!(TestWrapper::deserialize(&modified), Ok(TestWrapper(1)));
    }

    #[test]
    fn bigger_buffer_serialize_wrapper() {
        let w = TestWrapper(13);
        let mut buf = [0u8; 7];
        assert!(w.serialize(&mut buf).is_ok());
        assert_eq!(buf[..4], 13u32.to_be_bytes()[..])
    }

    #[test]
    fn bigger_buffer_deserailize_wrapper() {
        let serialized = 12345u32.to_be_bytes();
        let mut buf = [0u8; 10];
        buf[..4].copy_from_slice(&serialized[..]);
        let w = TestWrapper::deserialize(&buf);
        assert_eq!(w, Ok(TestWrapper(12345)));
    }

    #[test]
    fn buffer_too_small_serialize_wrapper() {
        let w = TestWrapper(13);
        let mut too_small = [0u8; 3];
        assert!(w.serialize(&mut too_small).is_err());
    }

    #[test]
    fn buffer_too_small_deserialize_wrapper() {
        let buf = [0u8, 3, 5];
        assert!(TestWrapper::deserialize(&buf).is_err());
    }

    #[test]
    fn serialize_enum() {
        let v = TestEnum::One;
        let mut buf = [0u8; 1];
        assert!(v.serialize(&mut buf).is_ok());
        assert_eq!(buf[0], TestEnum::One as u8)
    }

    #[test]
    fn deserialize_enum() {
        let buf = [TestEnum::Three as u8];
        assert_eq!(TestEnum::deserialize(&buf), Ok(TestEnum::Three));
    }

    #[test]
    fn serialize_modify_valid() {
        let v = TestEnum::One;
        let mut buf = [0u8; 1];
        assert!(v.serialize(&mut buf).is_ok());
        buf[0] = 2;
        assert_eq!(TestEnum::deserialize(&buf), Ok(TestEnum::Two));
    }

    #[test]
    fn serialize_modify_invalid() {
        let v = TestEnum::One;
        let mut buf = [0; 1];
        assert!(v.serialize(&mut buf).is_ok());
        buf[0] = 15;
        assert!(TestEnum::deserialize(&buf).is_err());
    }

    use packets::Reserved;
    use ubass::prelude::packets::{
        BatchID, BytePosition, DataPacket, MAX_PAYLOAD_LENGTH, Options, PacketType, SessionId,
        Version,
    };

    #[test]
    fn serialize_reserved() {
        let reserved = Reserved::<5>;
        let mut buf = [0; 5];
        assert!(reserved.serialize(&mut buf).is_ok());
        assert_eq!(buf, [0; 5]);
    }

    #[test]
    fn deserialize_reserved() {
        let buf = [0; 3];
        assert_eq!(Reserved::<3>::deserialize(&buf), Ok(Reserved::<3>));
    }

    #[test]
    fn deserialize_garbage_reserved() {
        let buf = [0, 1, 2, 3, 55, 42, 69, 200];
        assert_eq!(Reserved::<8>::deserialize(&buf), Ok(Reserved::<8>));
    }

    #[test]
    fn serialize_too_small_reserved() {
        let reserved = Reserved::<10>;
        let mut buf = [0; 5];
        assert!(reserved.serialize(&mut buf).is_err());
    }

    #[test]
    fn deserialize_too_small_reserved() {
        let buf = [0; 4];
        assert!(Reserved::<8>::deserialize(&buf).is_err());
    }

    fn get_data_packet() -> DataPacket {
        packets::DataPacket {
            version: Version::CURRENT_VERSION,
            opts: Options::none(),
            packet_type: packets::PacketType::Data,
            batch_id: BatchID::new(9),
            fec_info: packets::FECInfo {
                batch_size: 9,
                batch_pos: 2,
                recovery_count: 5,
            },
            session_id: SessionId::new(5),
            timestamp: Timestamp(120),
            byte_range_start: BytePosition(8),
            payload: vec![1u8, 2, 3, 4, 5].into(),
        }
    }

    const SERIALIZED_DATA_PACKET: [u8; 35] = [
        /*version*/ 0,
        1,
        /*opts*/ 0,
        0,
        PacketType::Data as u8,
        /*batch_id*/ 0,
        9,
        /*fec_info*/ 9,
        2,
        5,
        /*session_id*/ 0,
        0,
        0,
        0,
        0,
        0,
        0,
        5,
        /*timestamp*/ 0,
        0,
        0,
        0,
        0,
        0,
        0,
        120,
        /*byte_position*/ 0,
        0,
        0,
        8,
        /*payload*/ 1,
        2,
        3,
        4,
        5,
    ];

    #[test]
    fn serialize_packet() {
        let packet = get_data_packet();
        let mut buf = [0u8; DataPacket::HEADER_SIZE + MAX_PAYLOAD_LENGTH];
        assert!(packet.serialize(&mut buf).is_ok());

        assert_eq!(buf[..SERIALIZED_DATA_PACKET.len()], SERIALIZED_DATA_PACKET);
    }

    #[test]
    fn deserialize_packet() {
        let packet = DataPacket::deserialize(&SERIALIZED_DATA_PACKET);
        assert_eq!(packet, Ok(get_data_packet()));
    }
}

mod dec_macros {
    use ubass::*;

    #[test]
    fn return_on_unwrap_result() {
        let error = Err::<(), ()>(());
        r_unwrap_or_return!(error);
        unreachable!()
    }

    #[test]
    fn unwrap_and_dont_return_result() {
        let ok = Ok::<(), ()>(());
        assert_eq!(r_unwrap_or_return!(ok), ());
    }

    #[test]
    fn return_on_unwrap_option() {
        let none = None::<()>;
        o_unwrap_or_return!(none);
        unreachable!()
    }

    #[test]
    fn unwrap_and_dont_return_option() {
        let some = Some(());
        assert_eq!(o_unwrap_or_return!(some), ());
    }
}
