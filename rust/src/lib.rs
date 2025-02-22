#![allow(clippy::get_first)]

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use reqwest::{header::CONTENT_TYPE, Client as HttpClient, Method, StatusCode, Url};
use tokio::sync::mpsc;

pub mod evm;
pub mod svm;

#[derive(Debug, Clone, Copy)]
pub struct ClientConfig {
    pub max_num_retries: usize,
    pub retry_backoff_ms: u64,
    pub retry_base_ms: u64,
    pub retry_ceiling_ms: u64,
    pub http_req_timeout_millis: u64,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            max_num_retries: 9,
            retry_backoff_ms: 1000,
            retry_base_ms: 250,
            retry_ceiling_ms: 2000,
            http_req_timeout_millis: 40_000,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StreamConfig {
    pub stop_on_head: bool,
    pub head_poll_interval_millis: u64,
    pub buffer_size: usize,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            stop_on_head: false,
            head_poll_interval_millis: 1_000,
            buffer_size: 10,
        }
    }
}

pub struct Client {
    http_client: HttpClient,
    url: Url,
    max_num_retries: usize,
    retry_backoff_ms: u64,
    retry_base_ms: u64,
    retry_ceiling_ms: u64,
}

static APP_USER_AGENT: &str = concat!("sqd-portal-client-rust/", env!("CARGO_PKG_VERSION"),);

impl Client {
    pub fn new(url: Url, config: ClientConfig) -> Self {
        let http_client = HttpClient::builder()
            .user_agent(APP_USER_AGENT)
            .http1_only()
            .gzip(true)
            .timeout(Duration::from_millis(config.http_req_timeout_millis))
            .build()
            .unwrap();

        Self {
            http_client,
            url,
            max_num_retries: config.max_num_retries,
            retry_backoff_ms: config.retry_backoff_ms,
            retry_base_ms: config.retry_base_ms,
            retry_ceiling_ms: config.retry_ceiling_ms,
        }
    }

    pub async fn svm_arrow_finalized_query(
        &self,
        query: &svm::Query,
    ) -> Result<Option<svm::ArrowResponse>> {
        let query = simd_json::to_vec(query).context("serliaze query")?;
        let query = bytes::Bytes::from(query);

        let response = self.finalized_query(query).await.context("execute query")?;
        let response = match response {
            Some(r) => r,
            None => return Ok(None),
        };

        let mut parser = svm::ArrowResponseParser::default();

        let lines = response.split(|x| *x == b'\n');
        let mut scratch = Vec::new();

        for line in lines {
            if line.is_empty() {
                continue;
            }

            scratch.extend_from_slice(line);
            let tape = simd_json::to_tape(&mut scratch).context("json to tape")?;
            parser.parse_tape(&tape).context("parse tape")?;
            scratch.clear();
        }

        Ok(Some(parser.finish()))
    }

    pub async fn evm_arrow_finalized_query(
        &self,
        query: &evm::Query,
    ) -> Result<Option<evm::ArrowResponse>> {
        let query = simd_json::to_vec(query).context("serialize query")?;
        let query = bytes::Bytes::from(query);

        let response = self.finalized_query(query).await.context("execute query")?;
        let response = match response {
            Some(r) => r,
            None => return Ok(None),
        };

        let mut parser = evm::ArrowResponseParser::default();

        let lines = response.split(|x| *x == b'\n');
        let mut scratch = Vec::new();

        for line in lines {
            if line.is_empty() {
                continue;
            }

            scratch.extend_from_slice(line);
            let tape = simd_json::to_tape(&mut scratch).context("json to tape")?;
            parser.parse_tape(&tape).context("parse tape")?;
            scratch.clear();
        }

        Ok(Some(parser.finish()))
    }

    pub fn evm_arrow_finalized_stream(
        self: Arc<Self>,
        query: evm::Query,
        config: StreamConfig,
    ) -> mpsc::Receiver<Result<evm::ArrowResponse>> {
        let (tx, rx) = mpsc::channel(config.buffer_size);

        let mut query = query;
        // we need this to iterate
        query.fields.block.number = true;

        tokio::spawn(async move {
            loop {
                if let Some(tb) = query.to_block {
                    if tb < query.from_block {
                        break;
                    }
                }

                let res = match self
                    .evm_arrow_finalized_query(&query)
                    .await
                    .context("run query")
                {
                    Ok(r) => r,
                    Err(e) => {
                        tx.send(Err(e)).await.ok();
                        return;
                    }
                };
                let res = match res {
                    Some(r) => r,
                    None => {
                        if config.stop_on_head {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(config.head_poll_interval_millis))
                            .await;
                        continue;
                    }
                };

                let next_block = match res.next_block().context("get next block from response") {
                    Ok(nb) => nb,
                    Err(e) => {
                        tx.send(Err(e)).await.ok();
                        return;
                    }
                };

                query.from_block = next_block;

                if tx.send(Ok(res)).await.is_err() {
                    log::debug!("receiver is closed so quitting stream");
                    return;
                }
            }
        });

        rx
    }

    pub async fn finalized_height(&self) -> Result<u64> {
        let res = self
            .finalized_req(Method::GET, &["finalized-stream", "height"], None)
            .await
            .context("make req")?
            .context("no response data")?;

        let height = std::str::from_utf8(&res).context("check body is utf8")?;
        let height = u64::from_str(height).context("parse height as number")?;

        Ok(height)
    }

    async fn finalized_query(&self, query: bytes::Bytes) -> Result<Option<bytes::Bytes>> {
        self.finalized_req(Method::POST, &["finalized-stream"], Some(query))
            .await
    }

    async fn finalized_req(
        &self,
        method: Method,
        url_segments: &[&str],
        body: Option<bytes::Bytes>,
    ) -> Result<Option<bytes::Bytes>> {
        let mut base = self.retry_base_ms;

        let mut err = anyhow!("");

        for _ in 0..self.max_num_retries + 1 {
            match self
                .finalized_req_impl(method.clone(), url_segments, body.clone())
                .await
            {
                Ok(res) => return Ok(res),
                Err(e) => {
                    log::error!(
                        "failed to get data from server, retrying... The error was: {:?}",
                        e
                    );
                    err = err.context(format!("{:?}", e));
                }
            }

            let base_ms = Duration::from_millis(base);
            let jitter = Duration::from_millis(rand::random::<u64>() % self.retry_backoff_ms);

            tokio::time::sleep(base_ms + jitter).await;

            base = std::cmp::min(base + self.retry_backoff_ms, self.retry_ceiling_ms);
        }

        Err(err)
    }

    async fn finalized_req_impl(
        &self,
        method: Method,
        url_segments: &[&str],
        body: Option<bytes::Bytes>,
    ) -> Result<Option<bytes::Bytes>> {
        let mut url = self.url.clone();
        let mut segments = url.path_segments_mut().ok().context("get path segments")?;
        for s in url_segments {
            segments.push(s);
        }
        std::mem::drop(segments);
        let req = self.http_client.request(method, url);

        let mut req = req.header(CONTENT_TYPE, "application/json");

        if let Some(body) = body {
            req = req.body(body);
        }

        let res = req.send().await.context("execute http req")?;

        let status = res.status();
        if !status.is_success() {
            if status == StatusCode::NO_CONTENT {
                return Ok(None);
            }

            let text = res.text().await.context("read text to see error")?;

            return Err(anyhow!(
                "http response status code {}, err body: {}",
                status,
                text
            ));
        }

        res.bytes()
            .await
            .context("read response body bytes")
            .map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn check_stream_finishes_properly() {
        let url = "https://portal.sqd.dev/datasets/ethereum-mainnet"
            .parse()
            .unwrap();
        let client = Client::new(url, ClientConfig::default());

        let query = evm::Query {
            from_block: 18123123,
            to_block: Some(18123222),
            logs: vec![evm::LogRequest::default()],
            transactions: vec![evm::TransactionRequest::default()],
            include_all_blocks: true,
            fields: evm::Fields {
                transaction: evm::TransactionFields {
                    value: true,
                    ..Default::default()
                },
                ..Default::default()
            },
            // fields: evm::Fields::all(),
            ..Default::default()
        };

        let client = Arc::new(client);

        let mut receiver = client.evm_arrow_finalized_stream(query, StreamConfig::default());

        while let Some(arrow_data) = receiver.recv().await {
            let arrow_data = arrow_data.unwrap();
            let tx_hash = arrow_data
                .transactions
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Decimal256Array>()
                .unwrap();

            for hash in tx_hash.iter().flatten() {
                dbg!(hash.to_string());
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn dummy_stream() {
        let url = "https://portal.sqd.dev/datasets/zksync-mainnet"
            .parse()
            .unwrap();
        let client = Client::new(url, ClientConfig::default());

        let query = evm::Query {
            from_block: 0,
            to_block: None,
            logs: vec![evm::LogRequest::default()],
            transactions: vec![evm::TransactionRequest::default()],
            include_all_blocks: true,
            fields: evm::Fields {
                transaction: evm::TransactionFields {
                    value: true,
                    ..Default::default()
                },
                ..Default::default()
            },
            // fields: evm::Fields::all(),
            ..Default::default()
        };

        let client = Arc::new(client);

        let mut receiver = client.evm_arrow_finalized_stream(query, StreamConfig::default());

        while let Some(arrow_data) = receiver.recv().await {
            let arrow_data = arrow_data.unwrap();
            let tx_hash = arrow_data
                .transactions
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Decimal256Array>()
                .unwrap();

            for hash in tx_hash.iter().flatten() {
                dbg!(hash.to_string());
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn dummy_svm() {
        let url = "https://portal.sqd.dev/datasets/solana-mainnet"
            .parse()
            .unwrap();
        let client = Client::new(url, ClientConfig::default());

        let query = svm::Query {
            from_block: 300123123,
            to_block: Some(300123143),
            fields: svm::Fields {
                transaction: svm::TransactionFields {
                    recent_blockhash: false,
                    ..svm::TransactionFields::all()
                },
                ..svm::Fields::all()
            },
            balances: vec![svm::BalanceRequest::default()],
            include_all_blocks: true,
            instructions: vec![svm::InstructionRequest::default()],
            logs: vec![svm::LogRequest::default()],
            rewards: vec![svm::RewardRequest::default()],
            token_balances: vec![svm::TokenBalanceRequest::default()],
            transactions: vec![svm::TransactionRequest::default()],
            type_: Default::default(),
        };

        // dbg!(&query);

        let arrow_data = client
            .svm_arrow_finalized_query(&query)
            .await
            .unwrap()
            .unwrap();

        let tx_hash = arrow_data
            .transactions
            .column_by_name("block_slot")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();

        for hash in tx_hash.iter().flatten() {
            dbg!(hash.to_string());
        }

        // dbg!(arrow_data);
    }
}
