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

use chrono::{Datelike, Timelike};

use crate::{
    array::{
        Array, DictionaryKey, DictionaryPrimitive, Offset, Primitive, PrimitiveArray,
        TryFromIterator, Utf8Array, Utf8Primitive,
    },
    datatypes::{DataType, IntervalUnit, TimeUnit},
    types::NativeType,
};
use crate::{error::Result, temporal_conversions::EPOCH_DAYS_FROM_CE};

use super::cast;

/// Cast numeric types to Utf8
pub fn cast_string_to_numeric<O: Offset, T>(
    from: &dyn Array,
    to: &DataType,
) -> Result<Box<dyn Array>>
where
    T: NativeType + lexical_core::FromLexical,
{
    let from = from.as_any().downcast_ref::<Utf8Array<O>>().unwrap();

    let iter = from
        .iter()
        .map(|x| x.and_then::<T, _>(|x| lexical_core::parse(x.as_bytes()).ok()));

    let array = Primitive::<T>::from_trusted_len_iter(iter).to(to.clone());

    Ok(Box::new(array))
}

pub fn to_i32<O: Offset>(array: &Utf8Array<O>, to_type: &DataType) -> PrimitiveArray<i32> {
    let converter = match to_type {
        DataType::Int32 => |x: &str| lexical_core::parse::<i32>(x.as_bytes()).ok(),
        DataType::Date32 => |x: &str| {
            x.parse::<chrono::NaiveDate>()
                .ok()
                .map(|x| x.num_days_from_ce() - EPOCH_DAYS_FROM_CE)
        },
        DataType::Time32(TimeUnit::Second) => |x: &str| {
            x.parse::<chrono::NaiveTime>()
                .ok()
                .map(|x| x.num_seconds_from_midnight() as i32)
        },
        DataType::Time32(TimeUnit::Millisecond) => |x: &str| {
            x.parse::<chrono::NaiveTime>()
                .ok()
                .map(|x| x.num_seconds_from_midnight() as i32 * 1000)
        },
        DataType::Interval(IntervalUnit::YearMonth) => {
            |x: &str| lexical_core::parse::<i32>(x.as_bytes()).ok()
        }
        _ => unreachable!(),
    };
    let iter = array.iter().map(|x| x.and_then(converter));
    Primitive::<i32>::from_trusted_len_iter(iter).to(to_type.clone())
}

pub fn to_i64<O: Offset>(array: &Utf8Array<O>, to_type: &DataType) -> PrimitiveArray<i64> {
    let converter = match to_type {
        DataType::Int64 | DataType::Duration(_) => {
            |x: &str| lexical_core::parse::<i64>(x.as_bytes()).ok()
        }
        DataType::Date64 => |x: &str| {
            x.parse::<chrono::NaiveDateTime>()
                .ok()
                .map(|x| x.timestamp_millis())
        },
        DataType::Time64(TimeUnit::Microsecond) => |x: &str| {
            x.parse::<chrono::NaiveTime>().ok().map(|x| {
                x.num_seconds_from_midnight() as i64 * 1_000_000 + x.nanosecond() as i64 / 1000
            })
        },
        DataType::Time64(TimeUnit::Nanosecond) => |x: &str| {
            x.parse::<chrono::NaiveTime>().ok().map(|x| {
                x.num_seconds_from_midnight() as i64 * 1_000_000_000 + x.nanosecond() as i64
            })
        },
        DataType::Timestamp(TimeUnit::Nanosecond, None) => |x: &str| {
            x.parse::<chrono::NaiveDateTime>()
                .ok()
                .map(|x| x.timestamp_nanos())
        },
        DataType::Timestamp(TimeUnit::Microsecond, None) => |x: &str| {
            x.parse::<chrono::NaiveDateTime>()
                .ok()
                .map(|x| x.timestamp_nanos() * 1000)
        },
        DataType::Timestamp(TimeUnit::Millisecond, None) => |x: &str| {
            x.parse::<chrono::NaiveDateTime>()
                .ok()
                .map(|x| x.timestamp_millis())
        },
        DataType::Timestamp(TimeUnit::Second, None) => |x: &str| {
            x.parse::<chrono::NaiveDateTime>()
                .ok()
                .map(|x| x.timestamp_millis() / 1000)
        },
        _ => unreachable!(),
    };

    let iter = array.iter().map(|x| x.and_then(converter));
    Primitive::<i64>::from_trusted_len_iter(iter).to(to_type.clone())
}

// Packs the data as a StringDictionaryArray, if possible, with the
// key types of K
pub fn string_to_dictionary<O: Offset, K: DictionaryKey>(
    array: &dyn Array,
) -> Result<Box<dyn Array>> {
    let to = if O::is_large() {
        DataType::LargeUtf8
    } else {
        DataType::Utf8
    };

    let values = cast(array, &to)?;
    let values = values.as_any().downcast_ref::<Utf8Array<O>>().unwrap();

    let iter = values.iter().map(Result::Ok);
    let primitive = DictionaryPrimitive::<K, Utf8Primitive<i32>, _>::try_from_iter(iter)?;

    let array = primitive.to(DataType::Dictionary(Box::new(K::DATA_TYPE), Box::new(to)));

    Ok(Box::new(array))
}
