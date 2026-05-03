use std::sync::Arc;

use crate::{
    get_state,
    manager::{
        STATE, outbound,
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
                return Err(ChannelError::ChannelClosed(Outbound, Layer::Manager).into());
            }
            Some(ApiCommand::RequestData(one_shot)) => {
                monitor.dispatch(endpoints::request_track(
                    one_shot,
                    manager_to_processor.clone(),
                ));
            }
            Some(ApiCommand::Close) => {
                monitor.flush().await;
                _ = manager_to_processor
                    .send(PacketProcessingMessage::Close)
                    .await;
                return Ok(());
            }
            Some(ApiCommand::Connect(request)) => {
                monitor.dispatch(endpoints::connect(request, manager_to_processor.clone()));
            }
            Some(ApiCommand::SendData(request)) => {
                // TODO: FEC controller — assign real batch_id, fec_info, byte_range_start
                monitor.dispatch(endpoints::send_data(request, manager_to_processor.clone()));
            }
            Some(ApiCommand::CloseSession(session_id)) => {
                monitor.dispatch(endpoints::close_session(
                    session_id,
                    manager_to_processor.clone(),
                ));
            }
            Some(ApiCommand::CloseStream(session_id)) => {
                monitor.dispatch(endpoints::close_stream(
                    session_id,
                    manager_to_processor.clone(),
                ));
            }
        }
    }
}
