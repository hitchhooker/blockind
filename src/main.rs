use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use subxt::{OnlineClient, PolkadotConfig};
use tokio::sync::mpsc;
use tracing::{info, error};
use tracing_subscriber;
use subxt::config::Header;

type BlockNumber = u32;
type BlockHash = <PolkadotConfig as subxt::Config>::Hash;

const BATCH_SIZE: usize = 100;

#[derive(Debug, thiserror::Error)]
enum IndexerError {
    #[error(transparent)]
    SubxtError(#[from] subxt::Error),
    #[error("Processing error: {0}")]
    ProcessingError(String),
    #[error("Channel send error")]
    ChannelSendError,
    #[error("Task join error: {0}")]
    JoinError(#[from] tokio::task::JoinError),
}

struct BlockIndexer {
    client: OnlineClient<PolkadotConfig>,
    latest_processed_block: Arc<AtomicU32>,
    target_block: BlockNumber,
}

impl BlockIndexer {
    async fn new(target_block: BlockNumber) -> Result<Self, IndexerError> {
        let client = OnlineClient::from_url("wss://dev.rotko.net/rococo-people").await?;
        Ok(Self {
            client,
            latest_processed_block: Arc::new(AtomicU32::new(0)),
            target_block,
        })
    }

    async fn run(&self) -> Result<(), IndexerError> {
        let (tx, mut rx) = mpsc::channel(100);
        let sub_handle = self.start_subscription(tx.clone())?;
        let hist_handle = self.fetch_historical_blocks(tx);

        tokio::select! {
            res = sub_handle => res??,
            res = hist_handle => res?,
            _ = self.process_blocks(&mut rx) => {},
        }

        Ok(())
    }

    fn start_subscription(
        &self,
        tx: mpsc::Sender<(BlockNumber, BlockHash)>,
    ) -> Result<tokio::task::JoinHandle<Result<(), IndexerError>>, IndexerError> {
        let blocks_client = self.client.blocks();
        let latest_block = Arc::clone(&self.latest_processed_block);

        Ok(tokio::spawn(async move {
            let mut block_sub = blocks_client.subscribe_finalized().await?;
            while let Some(block) = block_sub.next().await {
                let block = block?;
                let number = block.header().number;
                let hash = block.header().hash();

                if tx.send((number, hash)).await.is_err() {
                    return Err(IndexerError::ChannelSendError);
                }
                println!("Received block #{}", number);
                //latest_block.fetch_max(number, Ordering::SeqCst);
            }
            Ok(())
        }))
    }

    async fn fetch_historical_blocks(
        &self,
        tx: mpsc::Sender<(BlockNumber, BlockHash)>,
    ) -> Result<(), IndexerError> {
        let blocks_client = self.client.blocks();
        let mut current_block = blocks_client.at_latest().await?;
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        println!("Latest block: {:?}", current_block.header().number);

        loop {
            let number = current_block.header().number;
            if number <= self.target_block || number <= self.latest_processed_block.load(Ordering::SeqCst) {
                break;
            }
            println!("Processing historical block #{}", number);

            batch.push((number, current_block.header().hash()));

            if batch.len() == BATCH_SIZE {
                self.send_batch(&tx, batch).await?;
                batch = Vec::with_capacity(BATCH_SIZE);
            }

            if let parent_hash = current_block.header().parent_hash {
                current_block = blocks_client.at(parent_hash).await?;
            } else {
                break;
            }
        }

        if !batch.is_empty() {
            self.send_batch(&tx, batch).await?;
        }

        Ok(())
    }

    async fn send_batch(
        &self,
        tx: &mpsc::Sender<(BlockNumber, BlockHash)>,
        batch: Vec<(BlockNumber, BlockHash)>,
    ) -> Result<(), IndexerError> {
        for (number, hash) in batch.into_iter().rev() {
            tx.send((number, hash)).await.map_err(|_| IndexerError::ChannelSendError)?;
        }
        Ok(())
    }

    async fn process_blocks(
        &self,
        rx: &mut mpsc::Receiver<(BlockNumber, BlockHash)>,
    ) -> Result<(), IndexerError> {
        while let Some((number, hash)) = rx.recv().await {
            match self.process_block(number, hash).await {
                Ok(_) => info!("Processed block #{}", number),
                Err(e) => error!("Error processing block #{}: {}", number, e),
            }
        }
        Ok(())
    }

    async fn process_block(&self, _number: BlockNumber, hash: BlockHash) -> Result<(), IndexerError> {
        let _block = self.client.blocks().at(hash).await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), IndexerError> {
    tracing_subscriber::fmt::init();
    BlockIndexer::new(100).await?.run().await
}
