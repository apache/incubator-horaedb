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

//! Contains all needed macros

/// Define result for given Error type
#[macro_export]
macro_rules! define_result {
    ($t:ty) => {
        pub type Result<T> = std::result::Result<T, $t>;
    };
}

#[macro_export]
macro_rules! hash_map(
    { $($key:expr => $value:expr),+ } => {
        {
            let mut m = ::std::collections::HashMap::new();
            $(
                m.insert($key, $value);
            )+
            m
        }
     };
);

/// Util for working with anyhow + thiserror
/// Works like anyhow's [ensure](https://docs.rs/anyhow/latest/anyhow/macro.ensure.html)
/// But return `Return<T, ErrorFromAnyhow>`
#[macro_export]
macro_rules! ensure {
    ($cond:expr, $msg:literal) => {
        if !$cond {
            return Err(anyhow::anyhow!($msg).into());
        }
    };
    ($cond:expr, $err:expr) => {
        if !$cond {
            return Err($err.into());
        }
    };
    ($cond:expr, $fmt:expr, $($arg:tt)*) => {
        if !$cond {
            return Err(anyhow::anyhow!($fmt, $($arg)*).into());
        }
    };
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_define_result() {
        define_result!(i32);

        fn return_i32_error() -> Result<()> {
            Err(18)
        }

        assert_eq!(Err(18), return_i32_error());
    }

    #[test]
    fn test_hash_map() {
        let m = hash_map! { 1 => "hello", 2 => "world" };

        assert_eq!(2, m.len());
        assert_eq!("hello", *m.get(&1).unwrap());
        assert_eq!("world", *m.get(&2).unwrap());
    }
}
