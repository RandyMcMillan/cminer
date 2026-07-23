use bitcoin::Transaction;

use nakamoto::client::handle::{Error, Handle};

pub fn mempool_snapshot<H>(handle: &H) -> Result<Vec<Transaction>, Error>
where
    H: Handle,
{
    handle.mempool()
}
