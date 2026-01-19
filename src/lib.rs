pub mod packet_processor;
pub mod packetizer;
pub mod transport;

// Enum of possible internal protocol erros.
// These errors are specifically:
//  1. completely private - nothing outside the protocol itself can see them
//  2. inward pointing - only errors that cannot be pointed to outside causes (like I/O) are
//     considered internal
#[derive(Debug, Clone)]
pub enum InternalError {
    TaskFailed,
    ChannelFailed,
    ChannelClosed,
}

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
