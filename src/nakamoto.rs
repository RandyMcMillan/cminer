use nakamoto_client::handle::{Error, Handle};
use nakamoto_common::block::Transaction;

pub fn mempool_snapshot<H>(handle: &H) -> Result<Vec<Transaction>, Error>
where
    H: Handle,
{
    handle.mempool()
}
