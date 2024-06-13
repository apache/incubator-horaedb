// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use upstream::{
    aws::{AmazonS3, AmazonS3Builder},
    ClientOptions, RetryConfig,
};

use crate::config::AliyunOptions;

fn normalize_endpoint(endpoint: &str, bucket: &str) -> String {
    if endpoint.starts_with("https") {
        format!(
            "https://{}.{}",
            bucket,
            endpoint.replacen("https://", "", 1)
        )
    } else {
        format!("http://{}.{}", bucket, endpoint.replacen("http://", "", 1))
    }
}

pub fn try_new(aliyun_opts: &AliyunOptions) -> upstream::Result<AmazonS3> {
    let cli_opt = ClientOptions::new()
        .with_allow_http(true)
        .with_pool_max_idle_per_host(aliyun_opts.http.pool_max_idle_per_host)
        .with_http2_keep_alive_timeout(aliyun_opts.http.keep_alive_timeout.0)
        .with_http2_keep_alive_while_idle()
        .with_http2_keep_alive_interval(aliyun_opts.http.keep_alive_interval.0)
        .with_timeout(aliyun_opts.http.timeout.0);
    let retry_config = RetryConfig {
        max_retries: aliyun_opts.retry.max_retries,
        retry_timeout: aliyun_opts.retry.retry_timeout.0,
        ..Default::default()
    };

    let endpoint = &aliyun_opts.endpoint;
    let bucket = &aliyun_opts.bucket;
    let endpoint = normalize_endpoint(endpoint, bucket);
    AmazonS3Builder::new()
        .with_virtual_hosted_style_request(true)
        // region is not used when virtual_hosted_style is true,
        // but is required, so dummy is used here
        // https://github.com/apache/arrow-rs/issues/3827
        .with_region("dummy")
        .with_access_key_id(&aliyun_opts.key_id)
        .with_secret_access_key(&aliyun_opts.key_secret)
        .with_endpoint(endpoint)
        .with_bucket_name(bucket)
        .with_client_options(cli_opt)
        .with_proxy_url(&aliyun_opts.proxy)
        .with_retry(retry_config)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_endpoint() {
        let testcase = [
            (
                "https://oss.aliyun.com",
                "test",
                "https://test.oss.aliyun.com",
            ),
            (
                "http://oss.aliyun.com",
                "test",
                "http://test.oss.aliyun.com",
            ),
            ("no-scheme.com", "test", "http://test.no-scheme.com"),
        ];

        for (endpoint, bucket, expected) in testcase {
            let actual = normalize_endpoint(endpoint, bucket);
            assert_eq!(expected, actual);
        }
    }
}
