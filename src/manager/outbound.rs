use std::sync::Arc;

use crate::{
    get_state,
    manager::{
        STATE,
        routines::endpoints,
        types::{ManagerFromApi, ManagerToProcessor},
    },
    prelude::*,
    utils::ApiCommand,
};

pub async fn init(
    mut manager_from_api: ManagerFromApi,
    manager_to_processor: ManagerToProcessor,
) -> ErrResult {
    let monitor = Arc::new(HandleMonitor::default());
    monitor.clone().init();

    loop {
        match manager_from_api.recv().await {
            None => {
                return Err(ChannelError::ChannelClosed(Outbound).into());
            }
            Some(ApiCommand::RequestData(one_shot)) => {
                monitor
                    .dispatch(endpoints::request_track(
                        one_shot,
                        manager_to_processor.clone(),
                    ))
                    .await;
            }
            Some(ApiCommand::Close) => {
                monitor.flush().await;
                _ = manager_to_processor
                    .send(PacketProcessingMessage::Close)
                    .await;
                return Ok(());
            }
            Some(ApiCommand::Connect(request)) => {
                // TODO: track in-flight handshakes to detect duplicate connect() calls
                monitor
                    .dispatch(endpoints::connect(request, manager_to_processor.clone()))
                    .await;
            }
            Some(ApiCommand::SendData(request)) => {
                // TODO: buffer framing — split large buffers across multiple DataPackets
                // TODO: FEC controller — assign real batch_id, fec_info, byte_range_start
                // TODO: route session-level errors back via inbound channel
                monitor
                    .dispatch(endpoints::send_data(request, manager_to_processor.clone()))
                    .await;
            }
        }
    }
}
