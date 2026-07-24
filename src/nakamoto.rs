use std::time::{SystemTime, UNIX_EPOCH};
use std::convert::TryInto;

use bitcoin::blockdata::constants::COIN_VALUE;
use bitcoin::blockdata::script::{Builder, Script};
use bitcoin::blockdata::transaction::{OutPoint, TxIn, TxOut};
use bitcoin::consensus::encode::deserialize;
use bitcoin::hashes::Hash as _;
use bitcoin::{Block, BlockHeader, BlockHash, Transaction, TxMerkleNode};

use nakamoto_client::handle::Handle;
use nakamoto_common::block::Transaction as NakamotoTransaction;
use nakamoto_common::bitcoin::consensus::encode::serialize as common_serialize;
use nakamoto_common::bitcoin::hashes::Hash as _;

use crate::util::Result;

pub fn mempool_snapshot<H>(handle: &H) -> Result<Vec<NakamotoTransaction>>
where
    H: Handle,
{
    Ok(handle.mempool()?)
}

pub fn mempool_snapshot_bitcoin<H>(handle: &H) -> Result<Vec<Transaction>>
where
    H: Handle,
{
    handle
        .mempool()?
        .into_iter()
        .map(|tx| Ok(deserialize(&common_serialize(&tx))?))
        .collect()
}

pub fn build_candidate_block<H>(handle: &H) -> Result<Block>
where
    H: Handle,
{
    let (height, tip) = handle.get_tip()?;
    let txs = mempool_snapshot_bitcoin(handle)?;
    info!(
        "building candidate block: height={}, mempool_txs={}, prev_hash={}",
        height + 1,
        txs.len(),
        tip.block_hash()
    );
    let prev_blockhash = BlockHash::from_slice(tip.block_hash().as_inner())?;
    let version = tip.version;
    let bits = tip.bits;
    let time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as u32;

    Ok(build_candidate_block_for(
        (height + 1).try_into().expect("block height overflow"),
        version,
        prev_blockhash,
        time,
        bits,
        txs,
    ))
}

pub fn build_candidate_block_for(
    height: u32,
    version: i32,
    prev_blockhash: BlockHash,
    time: u32,
    bits: u32,
    txs: Vec<Transaction>,
) -> Block {
    let has_witness = txs.iter().any(|tx| tx.input.iter().any(|input| !input.witness.is_empty()));
    let mut block = Block {
        header: BlockHeader {
            version,
            prev_blockhash,
            merkle_root: TxMerkleNode::default(),
            time,
            bits,
            nonce: 0,
        },
        txdata: {
            let mut txdata = Vec::with_capacity(txs.len() + 1);
            txdata.push(coinbase_transaction(height, subsidy(height), has_witness));
            txdata.extend(txs);
            txdata
        },
    };

    if has_witness {
        let witness_root = block.witness_root();
        let commitment = Block::compute_witness_commitment(&witness_root, &block.txdata[0].input[0].witness[0]);
        block.txdata[0]
            .output
            .push(TxOut {
                value: 0,
                script_pubkey: Script::new_op_return(&{
                    let mut bytes = Vec::with_capacity(36);
                    bytes.extend_from_slice(&[0xaa, 0x21, 0xa9, 0xed]);
                    bytes.extend_from_slice(commitment.as_inner());
                    bytes
                }),
            });
    }

    block.header.merkle_root = block.merkle_root();
    debug_assert!(block.check_merkle_root());
    if has_witness {
        debug_assert!(block.check_witness_commitment());
    }
    info!(
        "candidate block ready: txs={}, witness={}, merkle_root={}",
        block.txdata.len(),
        has_witness,
        block.header.merkle_root
    );
    block
}

fn coinbase_transaction(height: u32, value: u64, has_witness: bool) -> Transaction {
    let script_sig = Builder::new().push_int(height as i64).into_script();
    let witness = if has_witness { vec![vec![0u8; 32]] } else { vec![] };

    Transaction {
        version: 1,
        lock_time: 0,
        input: vec![TxIn {
            previous_output: OutPoint::null(),
            script_sig,
            sequence: u32::MAX,
            witness,
        }],
        output: vec![TxOut {
            value,
            script_pubkey: Builder::new().push_int(1).into_script(),
        }],
    }
}

fn subsidy(height: u32) -> u64 {
    let halvings = height / 210_000;
    if halvings >= 64 {
        0
    } else {
        50 * COIN_VALUE >> halvings
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_candidate_block_with_coinbase_only() {
        let block = build_candidate_block_for(
            1,
            1,
            BlockHash::default(),
            1_700_000_000,
            0x1d00ffff,
            vec![],
        );

        assert_eq!(block.txdata.len(), 1);
        assert!(block.check_merkle_root());
        assert_eq!(block.txdata[0].output[0].value, 50 * COIN_VALUE);
    }
}
