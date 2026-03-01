pub enum Error {
    Recoverable(ErrorContents),
    Unrecoverable(ErrorContents),
}

#[derive(thiserror::Error, Debug)]
pub enum ErrorContents {
    #[error("Generic: {0}")]
    Generic(String),
    #[error("Channel: {0}")]
    Channel(String),
    #[error("Task: {0}")]
    Task(String),
}
