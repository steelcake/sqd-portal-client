use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use reqwest::{Client as HttpClient, Method, Url};
use serde::Serialize;

pub mod evm;

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
    pub fn new(url: Url, config: ClientConfig) -> Self {
        let http_client = HttpClient::builder()
            .gzip(false)
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

    pub async fn evm_arrow_finalized_query(&self, query: &evm::Query) -> Result<evm::ArrowResponse> {
        self.finalized_query(&evm::ArrowResponseParser, query).await
    }

    async fn finalized_query<Q: Serialize, R: ResponseParser>(
        &self,
        parser: &R,
        query: &Q,
    ) -> Result<R::Output> {
        let mut base = self.retry_base_ms;

        let mut err = anyhow!("");

        for _ in 0..self.max_num_retries + 1 {
            match self.finalized_query_impl::<Q, R>(parser, query).await {
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

    async fn finalized_query_impl<Q: Serialize, R: ResponseParser>(
        &self,
        query: &[u8],
    ) -> Result<R::Output> {
        let mut url = self.url.clone();
        let mut segments = url.path_segments_mut().ok().context("get path segments")?;
        segments.push("finalized-stream");
        std::mem::drop(segments);
        let req = self.http_client.request(Method::POST, url);

        let res = req.json(&query).send().await.context("execute http req")?;

        let status = res.status();
        if !status.is_success() {
            let text = res.text().await.context("read text to see error")?;

            return Err(anyhow!(
                "http response status code {}, err body: {}",
                status,
                text
            ));
        }

        let bytes = res.bytes().await.context("read response body bytes")?;
        let mut bytes = bytes.to_vec();
    
        let output = parser.parse(bytes.as_mut_slice()).context("parse json")?;

        Ok(output)
    }

    // pub fn finalized_stream(query: &EvmQuery, config: &StreamConfig) -> Result<FinalizedStream<EvmResponse>> {
    //     todo!()
    // }
}

// pub struct StreamConfig {

// }
//

trait ResponseParser {
    fn parse(&mut self, tape: &simd_json::Tape<'_>) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn dummy() {
        let url = "https://portal.sqd.dev/datasets/ethereum-mainnet"
            .parse()
            .unwrap();
        let client = Client::new(url, ClientConfig::default());

        let query = evm::Query {
            from_block: 21718704,
            logs: vec![evm::LogRequest {
                address: vec!["0xae78736Cd615f374D3085123A210448E74Fc6393".to_lowercase()],
                transaction: true,
                transaction_traces: true,
                ..Default::default()
            }],
            fields: evm::Fields {
                log: evm::LogFields {
                    address: true,
                    ..Default::default()
                },
                block: evm::BlockFields {
                    number: true,
                    timestamp: true,
                    difficulty: true,
                    size: true,
                    gas_limit: true,
                    ..Default::default()
                },
                trace: evm::TraceFields {
                    transaction_index: true,
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        };

        dbg!(&query);

        // let query: evm::Query = serde_json::from_value(serde_json::json!({
        //     "from_block": 20123123,
        //     "transactions": [
        //         {
        //             "from": ""
        //         }
        //     ],
        //     "fields": {
        //         "transaction": {
        //             "from": true,
        //             "to": true,
        //         }
        //     }
        // })).unwrap();

        println!("{}", serde_json::to_string_pretty(&query).unwrap());

        client
            .finalized_query::<_, evm::ArrowResponseParser>(&evm::ArrowResponseParser::default(), &query)
            .await
            .unwrap();
    }
}
