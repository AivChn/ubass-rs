use std::sync::{Arc, atomic::Ordering};

use crate::{
    get_state,
    manager::{
        STATE, outbound,
        routines::endpoints,
        state,
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
                get_state!().ack.close();
                get_state!().global_handle_monitor.flush().await;
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
            Some(ApiCommand::StreamAction(OneShot {
                data: (session_id, control),
                response,
            })) => {
                monitor.dispatch(endpoints::send_playback_control_packet(
                    session_id,
                    control,
                    response,
                    manager_to_processor.clone(),
                ));
            }
            Some(ApiCommand::SetStreamComplete(session_id, allow_partial)) => {
                monitor.dispatch(endpoints::set_complete_stream(session_id, allow_partial));
            }

            Some(ApiCommand::FindHoles(OneShot {
                data: session_id,
                response,
            })) => {
                monitor.dispatch(endpoints::find_holes(session_id, response));
            }
        }
    }
}
