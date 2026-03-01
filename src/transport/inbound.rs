use std::{net::SocketAddr, vec};
use tokio::{net::UdpSocket, sync::mpsc::Sender};

use super::send_to_processing_layer;
use super::types::*;

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
pub async fn init(
    port: u16,
    sender: Sender<Result<ReceivedPacket, TransportError>>,
) -> Result<(), TransportError> {
    let socket = UdpSocket::bind(format!("0.0.0.0:{port}"))
        .await
        .map_err(|_| TransportError::FailedToBind)?;
    let mut fail_count = 0;

    loop {
        let mut buffer = vec![0u8; MAX_PACKET_SIZE];

        let stripped_buffer = loop {
            match socket.recv(&mut buffer).await {
                Ok(read) => break &buffer[..read],
                Err(_) => {
                    if fail_count >= 25 {
                        return Err(TransportError::RecvFailedTooManyTimes);
                    } else {
                        fail_count += 1;
                    }
                }
            }
            if let Ok(read) = socket.recv(&mut buffer).await {
                fail_count = 0;
                break &buffer[..read];
            }
        };

        let packet = ReceivedPacket::new(stripped_buffer.to_vec());

        if let Err(intern) = send_to_processing_layer(sender.clone(), Ok(packet)).await {
            return Err(TransportError::Internal(intern));
        }
    }
}
/// Encodes a socket address into a session token.
///
/// **Note:** This is a placeholder implementation that only encodes the port,
/// ignoring the IP address. Will be replaced with proper session management.
pub fn get_session_token(addr: SocketAddr) -> u128 {
    (addr.port() as u128) * 12 * 100_000_012
}
