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

use std::hash::Hash;

use crate::{
    array::{
        Array, BooleanArray, DictionaryKey, DictionaryPrimitive, Offset, Primitive, PrimitiveArray,
        TryFromIterator, Utf8Array,
    },
    bitmap::Bitmap,
    datatypes::{DataType, IntervalUnit, TimeUnit},
    temporal_conversions,
    types::NativeType,
};
use crate::{error::Result, util::lexical_to_string};

use super::cast;

/// Cast numeric types to Utf8
pub fn cast_numeric_to_string<T, O>(array: &dyn Array) -> Result<Box<dyn Array>>
where
    O: Offset,
    T: NativeType + lexical_core::ToLexical,
{
    let array = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();

    let iter = array.iter().map(|x| x.map(|x| lexical_to_string(*x)));

    let array = Utf8Array::<O>::from_trusted_len_iter(iter);

    Ok(Box::new(array))
}

pub fn i32_to_string<O: Offset>(array: &PrimitiveArray<i32>) -> Utf8Array<O> {
    let converter = match array.data_type() {
        DataType::Interval(IntervalUnit::YearMonth) | DataType::Int32 => lexical_to_string,
        DataType::Date32 => |x| temporal_conversions::date32_to_date(x).to_string(),
        DataType::Time32(TimeUnit::Second) => {
            |x| temporal_conversions::time32s_to_time(x).to_string()
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            |x| temporal_conversions::time32ms_to_time(x).to_string()
        }
        _ => unreachable!(),
    };

    let iter = array.iter().map(|x| x.copied().map(converter));
    Utf8Array::<O>::from_trusted_len_iter(iter)
}

pub fn i64_to_string<O: Offset>(array: &PrimitiveArray<i64>) -> Utf8Array<O> {
    let converter = match array.data_type() {
        DataType::Int64 | DataType::Duration(_) => lexical_to_string,
        DataType::Date64 => |x| temporal_conversions::date64_to_date(x).to_string(),
        DataType::Time64(TimeUnit::Microsecond) => {
            |x| temporal_conversions::time64us_to_time(x).to_string()
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            |x| temporal_conversions::time64ns_to_time(x).to_string()
        }
        DataType::Timestamp(TimeUnit::Second, None) => {
            |x| temporal_conversions::timestamp_s_to_datetime(x).to_string()
        }
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            |x| temporal_conversions::timestamp_ms_to_datetime(x).to_string()
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            |x| temporal_conversions::timestamp_us_to_datetime(x).to_string()
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            |x| temporal_conversions::timestamp_ns_to_datetime(x).to_string()
        }
        _ => unreachable!(),
    };

    let iter = array.iter().map(|x| x.copied().map(converter));
    Utf8Array::<O>::from_trusted_len_iter(iter)
}

/*

impl std::fmt::Display for PrimitiveArray<i128> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.data_type() {
            DataType::Decimal(_, scale) => {
                let new_lines = false;
                let head = &format!("{}", self.data_type());
                // The number 999.99 has a precision of 5 and scale of 2
                let iter = self.iter().map(|x| {
                    x.copied().map(|x| {
                        let base = x / 10i128.pow(*scale as u32);
                        let decimals = x - base * 10i128.pow(*scale as u32);
                        format!("{}.{}", base, decimals)
                    })
                });
                display_fmt(iter, head, f, new_lines)
            }
            _ => unreachable!(),
        }
    }
}

impl std::fmt::Display for PrimitiveArray<days_ms> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let new_lines = false;
        let head = &format!("{}", self.data_type());
        let iter = self.iter().map(|x| {
            x.copied()
                .map(|x| format!("{}d{}ms", x.days(), x.milliseconds()))
        });
        display_fmt(iter, head, f, new_lines)
    }
}
*/

/// Convert Array into a PrimitiveArray of type, and apply numeric cast
pub fn cast_numeric_arrays<I, O>(from: &dyn Array, to_type: &DataType) -> Result<Box<dyn Array>>
where
    I: NativeType + num::NumCast,
    O: NativeType + num::NumCast,
{
    let from = from.as_any().downcast_ref::<PrimitiveArray<I>>().unwrap();
    Ok(Box::new(cast_typed_primitive::<I, O>(from, to_type)))
}

/// Cast PrimitiveArray to PrimitiveArray
pub fn cast_typed_primitive<I, O>(from: &PrimitiveArray<I>, to_type: &DataType) -> PrimitiveArray<O>
where
    I: NativeType + num::NumCast,
    O: NativeType + num::NumCast,
{
    let from = from.as_any().downcast_ref::<PrimitiveArray<I>>().unwrap();

    let iter = from
        .iter()
        .map(|v| v.and_then(|x| num::cast::cast::<I, O>(*x)));
    Primitive::<O>::from_trusted_len_iter(iter).to(to_type.clone())
}

/// Cast an array by changing its data type to the desired type
pub fn cast_array_data<T>(from: &dyn Array, to_type: &DataType) -> Result<Box<dyn Array>>
where
    T: NativeType,
{
    let from = from.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();

    Ok(Box::new(PrimitiveArray::<T>::from_data(
        to_type.clone(),
        from.values_buffer().clone(),
        from.validity().clone(),
    )))
}

/// Cast numeric types to Boolean
///
/// Any zero value returns `false` while non-zero returns `true`
pub fn cast_numeric_to_bool<T>(array: &dyn Array) -> Result<Box<dyn Array>>
where
    T: NativeType,
{
    let array = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();

    let iter = array.values().iter().map(|v| *v != T::default());
    let values = Bitmap::from_trusted_len_iter(iter);

    let array = BooleanArray::from_data(values, array.validity().clone());

    Ok(Box::new(array))
}

pub fn primitive_to_dictionary<T: NativeType + Eq + Hash, K: DictionaryKey>(
    array: &dyn Array,
    to: &DataType,
) -> Result<Box<dyn Array>> {
    let values = cast(array, to)?;
    let values = values.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();

    let iter = values.iter().map(|x| x.copied()).map(Result::Ok);
    let primitive = DictionaryPrimitive::<K, Primitive<T>, _>::try_from_iter(iter)?;

    let array = primitive.to(DataType::Dictionary(
        Box::new(K::DATA_TYPE),
        Box::new(values.data_type().clone()),
    ));

    Ok(Box::new(array))
}
