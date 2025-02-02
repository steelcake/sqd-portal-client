use std::str::FromStr;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use reqwest::{header::CONTENT_TYPE, Client as HttpClient, Method, StatusCode, Url};

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

    // pub fn evm_arrow_finalized_stream(self: Arc<Self>, query: evm::Query) -> mpsc::Receiver<Result<evm::ArrowResponse>> {
    //     let (tx, rx) = mpsc::channel(10);

    //     tokio::spawn(async move {
    //         let mut query = query;
    //         // we need this to iterate
    //         query.fields.block.number = true;

    //         let height = match self.finalized_height().await {
    //             Ok(h) => h,
    //             Err(e) => {
    //                 tx.send(Err(e)).await.ok();
    //                 return;
    //             }
    //         };

    //         query.to_block = Some(height);

    //         loop {
    //             match self.evm_arrow_finalized_stream_next_batch(&mut query).await {
    //                 Err(e) => {
    //                     tx.send(Err(e)).await.ok();
    //                     break;
    //                 }
    //                 Ok(res) => match res {
    //                     None => {
    //                         break;
    //                     }
    //                     Some((next_block, batch)) => {
    //                         if let Some(tb) = query.to_block {
    //                             if next_block == tb+1 {
    //                                 done = true;
    //                             }
    //                         }

    //                         if tx.send(Ok(Some(batch))).await.is_err() {
    //                             log::debug!("closing stream since the receiver is dropped");
    //                             break;
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //     });

    //     rx
    // }

    // async fn evm_arrow_finalized_stream_next_batch(&self, query: &evm::Query) -> Result<Option<(u64, evm::ArrowResponse)>> {
    //     let height = self.finalized_height().await.context("get height")?;

    //     let mut query_to_send = query.clone();
    //     query_to_send.to_block = query_to_send.to_block.map(|tb| tb.min(height));

    //     let res = self.evm_arrow_finalized_query(&query_to_send).await.context("run query")?;
    //     let res = match res {
    //         Some(r) => r,
    //         None => return Ok(None),
    //     };

    //     let next_block = res.next_block().context("get next block from response")?;

    //     Ok(Some((next_block, res)))
    // }

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
    async fn dummy() {
        let url = "https://portal.sqd.dev/datasets/zksync-mainnet"
            .parse()
            .unwrap();
        let client = Client::new(url, ClientConfig::default());

        let query = evm::Query {
            from_block: 36963986,
            to_block: Some(36963986),
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

        // dbg!(&query);

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

        // println!("{}", serde_json::to_string_pretty(&query).unwrap());

        let arrow_data = client
            .evm_arrow_finalized_query(&query)
            .await
            .unwrap()
            .unwrap();

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

        // dbg!(arrow_data);
    }
}
