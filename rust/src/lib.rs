use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use reqwest::{Client as HttpClient, Url};
use serde::Serialize;

pub mod evm;

pub struct ClientConfig {
    pub url: Url,
    pub max_num_retries: usize,
    pub retry_backoff_ms: u64,
    pub retry_base_ms: u64,
    pub retry_ceiling_ms: u64,
    pub http_req_timeout_millis: u64,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            url: "https://portal.sqd.dev/datasets/ethereum-mainnet"
                .parse()
                .unwrap(),
            max_num_retries: 9,
            retry_backoff_ms: 1000,
            retry_base_ms: 250,
            retry_ceiling_ms: 2000,
            http_req_timeout_millis: 20_000,
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

impl Client {
    pub fn new(config: ClientConfig) -> Self {
        let http_client = HttpClient::builder()
            .gzip(true)
            .timeout(Duration::from_millis(config.http_req_timeout_millis))
            .build()
            .unwrap();

        Self {
            http_client,
            url: config.url,
            max_num_retries: config.max_num_retries,
            retry_backoff_ms: config.retry_backoff_ms,
            retry_base_ms: config.retry_base_ms,
            retry_ceiling_ms: config.retry_ceiling_ms,
        }
    }

    pub fn finalized_query<Q: Serialize, R: Response>(query: &Q) -> Result<R> {
        todo!()
    }

    // pub fn finalized_stream(query: &EvmQuery, config: &StreamConfig) -> Result<FinalizedStream<EvmResponse>> {
    //     todo!()
    // }
}

// pub struct StreamConfig {

// }

trait Response {}
