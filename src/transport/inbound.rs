use crate::prelude::*;

use tokio::{net::UdpSocket, sync::mpsc::Sender};

use super::send_to_processing_layer;
use super::types::{MAX_PACKET_SIZE, ReceivedPacket};

/// Listens for incoming UDP packets and forwards them to the packet processor.
///
/// This function runs an infinite loop, receiving packets and sending them up
/// through the provided channel. It only exits on unrecoverable errors.
///
/// # Arguments
///
/// * `port` - The UDP port to listen on (binds to `0.0.0.0:{port}`)
/// * `sender` - Channel to forward received packets to the packet processor
///
/// # Returns
///
/// * `Ok(())` - Never returns this in practice (loops forever)
/// * `Err(FailedToBind)` - Could not bind to the specified port
/// * `Err(Internal)` - Channel to packet processor closed
///
/// # Note
///
/// This function is designed to be forcefully aborted by the supervisor on shutdown,
/// as it has no mechanism for graceful termination.
pub async fn init(port: u16, sender: Sender<Result<ReceivedPacket>>) -> ErrResult {
    const MAX_ALLOWED_FAILS: u32 = 10;
    let socket = UdpSocket::bind(format!("0.0.0.0:{port}"))
        .await
        .map_err(|_| TransportError::FailedToBind)?;
    let mut fail_count = 0u32;

    loop {
        let mut buffer = vec![0u8; MAX_PACKET_SIZE];

        let stripped_buffer = loop {
            if let Ok(read) = socket.recv(&mut buffer).await {
                break &buffer[..read];
            }

            if fail_count >= MAX_ALLOWED_FAILS {
                return Err(TransportError::RecvFailedTooManyTimes.into());
            }

            fail_count += 1;

            if let Ok(read) = socket.recv(&mut buffer).await {
                fail_count = 0;
                break &buffer[..read];
            }
        };

        let packet = ReceivedPacket::new(stripped_buffer.to_vec());

        send_to_processing_layer(sender.clone(), Ok(packet)).await?;
    }
}
