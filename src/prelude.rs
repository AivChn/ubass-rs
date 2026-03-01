pub use crate::error::Error;
pub use crate::utils::*;
pub use ubass_macros::*;
pub type Result<T> = core::result::Result<T, Error>;
