use parquet2::{
    encoding::{hybrid_rle, Encoding},
    metadata::{ColumnChunkMetaData, ColumnDescriptor},
    read::{BinaryPageDict, Page, PageHeader, StreamingIterator},
    serialization::read::levels,
};

use crate::{
    array::{Offset, Utf8Array},
    bitmap::{utils::BitmapIter, MutableBitmap},
    buffer::MutableBuffer,
    error::{ArrowError, Result},
};

use super::utils;

/// Assumptions: No rep levels
#[allow(clippy::too_many_arguments)]
pub(crate) fn read_dict_buffer<O: Offset>(
    validity_buffer: &[u8],
    indices_buffer: &[u8],
    length: u32,
    dict: &BinaryPageDict,
    offsets: &mut MutableBuffer<O>,
    values: &mut MutableBuffer<u8>,
    validity: &mut MutableBitmap,
) {
    let length = length as usize;
    let dict_values = dict.values();
    let dict_offsets = dict.offsets();
    let mut last_offset = *offsets.as_mut_slice().last().unwrap();

    // SPEC: Data page format: the bit width used to encode the entry ids stored as 1 byte (max bit width = 32),
    // SPEC: followed by the values encoded using RLE/Bit packed described above (with the given bit width).
    let bit_width = indices_buffer[0];
    let indices_buffer = &indices_buffer[1..];

    let non_null_indices_len = (indices_buffer.len() * 8 / bit_width as usize) as u32;
    let mut indices =
        levels::rle_decode(&indices_buffer, bit_width as u32, non_null_indices_len).into_iter();

    let validity_iterator = hybrid_rle::Decoder::new(&validity_buffer, 1);

    for run in validity_iterator {
        match run {
            hybrid_rle::HybridEncoded::Bitpacked(packed) => {
                let remaining = length - values.len();
                let len = core::cmp::min(packed.len() * 8, remaining);
                for is_valid in BitmapIter::new(packed, 0, len) {
                    validity.push(is_valid);
                    if is_valid {
                        let index = indices.next().unwrap() as usize;
                        let dict_offset_i = dict_offsets[index] as usize;
                        let dict_offset_ip1 = dict_offsets[index + 1] as usize;
                        let length = dict_offset_ip1 - dict_offset_i;
                        last_offset += O::from_usize(length).unwrap();
                        values.extend_from_slice(&dict_values[dict_offset_i..dict_offset_ip1]);
                    };
                    offsets.push(last_offset);
                }
            }
            hybrid_rle::HybridEncoded::Rle(value, additional) => {
                let is_set = value[0] == 1;
                validity.extend_constant(additional, is_set);
                if is_set {
                    (0..additional).for_each(|_| {
                        let index = indices.next().unwrap() as usize;
                        let dict_offset_i = dict_offsets[index] as usize;
                        let dict_offset_ip1 = dict_offsets[index + 1] as usize;
                        let length = dict_offset_ip1 - dict_offset_i;
                        last_offset += O::from_usize(length).unwrap();
                        offsets.push(last_offset);
                        values.extend_from_slice(&dict_values[dict_offset_i..dict_offset_ip1]);
                    })
                } else {
                    offsets.extend_constant(additional, last_offset)
                }
            }
        }
    }
}

pub(crate) fn read_optional<O: Offset>(
    validity_buffer: &[u8],
    values_buffer: &[u8],
    length: u32,
    offsets: &mut MutableBuffer<O>,
    values: &mut MutableBuffer<u8>,
    validity: &mut MutableBitmap,
) {
    let length = length as usize;
    let mut last_offset = *offsets.as_mut_slice().last().unwrap();

    // values_buffer: first 4 bytes are len, remaining is values
    let mut values_iterator = utils::BinaryIter::new(values_buffer);

    let validity_iterator = hybrid_rle::Decoder::new(&validity_buffer, 1);

    for run in validity_iterator {
        match run {
            hybrid_rle::HybridEncoded::Bitpacked(packed) => {
                // the pack may contain more items than needed.
                let remaining = length - values.len();
                let len = core::cmp::min(packed.len() * 8, remaining);
                for is_valid in BitmapIter::new(packed, 0, len) {
                    validity.push(is_valid);
                    if is_valid {
                        let value = values_iterator.next().unwrap();
                        last_offset += O::from_usize(value.len()).unwrap();
                        values.extend_from_slice(value);
                    }
                    offsets.push(last_offset);
                }
            }
            hybrid_rle::HybridEncoded::Rle(value, additional) => {
                let is_set = value[0] == 1;
                validity.extend_constant(additional, is_set);
                if is_set {
                    (0..additional).for_each(|_| {
                        let value = values_iterator.next().unwrap();
                        last_offset += O::from_usize(value.len()).unwrap();
                        offsets.push(last_offset);
                        values.extend_from_slice(value)
                    })
                } else {
                    offsets.extend_constant(additional, last_offset)
                }
            }
        }
    }
}

pub(crate) fn read_required<O: Offset>(
    buffer: &[u8],
    _length: u32,
    offsets: &mut MutableBuffer<O>,
    values: &mut MutableBuffer<u8>,
) {
    let mut last_offset = *offsets.as_mut_slice().last().unwrap();

    let values_iterator = utils::BinaryIter::new(buffer);

    for value in values_iterator {
        last_offset += O::from_usize(value.len()).unwrap();
        values.extend_from_slice(value);
        offsets.push(last_offset);
    }
}

pub fn iter_to_array<O, I, E>(mut iter: I, metadata: &ColumnChunkMetaData) -> Result<Utf8Array<O>>
where
    ArrowError: From<E>,
    E: Clone,
    O: Offset,
    I: StreamingIterator<Item = core::result::Result<Page, E>>,
{
    let capacity = metadata.num_values() as usize;
    let mut values = MutableBuffer::<u8>::with_capacity(0);
    let mut offsets = MutableBuffer::<O>::with_capacity(1 + capacity);
    offsets.push(O::default());
    let mut validity = MutableBitmap::with_capacity(capacity);
    while let Some(page) = iter.next() {
        extend_from_page(
            page.as_ref().map_err(|x| x.clone())?,
            metadata.descriptor(),
            &mut offsets,
            &mut values,
            &mut validity,
        )?
    }

    Ok(Utf8Array::from_data(
        offsets.into(),
        values.into(),
        validity.into(),
    ))
}

pub(crate) fn extend_from_page<O: Offset>(
    page: &Page,
    descriptor: &ColumnDescriptor,
    offsets: &mut MutableBuffer<O>,
    values: &mut MutableBuffer<u8>,
    validity: &mut MutableBitmap,
) -> Result<()> {
    assert_eq!(descriptor.max_rep_level(), 0);
    assert!(descriptor.max_def_level() <= 1);
    let is_optional = descriptor.max_def_level() == 1;
    match page.header() {
        PageHeader::V1(header) => {
            assert_eq!(header.definition_level_encoding, Encoding::Rle);

            match (&page.encoding(), page.dictionary_page(), is_optional) {
                (Encoding::PlainDictionary, Some(dict), true) => {
                    // split in two buffers: def_levels and data
                    let (validity_buffer, values_buffer) = utils::split_buffer_v1(page.buffer());
                    read_dict_buffer::<O>(
                        validity_buffer,
                        values_buffer,
                        page.num_values() as u32,
                        dict.as_any().downcast_ref().unwrap(),
                        offsets,
                        values,
                        validity,
                    )
                }
                (Encoding::Plain, None, true) => {
                    // split in two buffers: def_levels and data
                    let (validity_buffer, values_buffer) = utils::split_buffer_v1(page.buffer());
                    read_optional::<O>(
                        validity_buffer,
                        values_buffer,
                        page.num_values() as u32,
                        offsets,
                        values,
                        validity,
                    )
                }
                (Encoding::Plain, None, false) => {
                    read_required::<O>(page.buffer(), page.num_values() as u32, offsets, values)
                }
                _ => {
                    return Err(utils::not_implemented(
                        &page.encoding(),
                        is_optional,
                        page.dictionary_page().is_some(),
                        "V1",
                        "Binary",
                    ))
                }
            }
        }
        PageHeader::V2(header) => {
            let def_level_buffer_length = header.definition_levels_byte_length as usize;

            match (&page.encoding(), page.dictionary_page(), is_optional) {
                (Encoding::PlainDictionary, Some(dict), true) => {
                    let (validity_buffer, values_buffer) =
                        utils::split_buffer_v2(page.buffer(), def_level_buffer_length);
                    read_dict_buffer::<O>(
                        validity_buffer,
                        values_buffer,
                        page.num_values() as u32,
                        dict.as_any().downcast_ref().unwrap(),
                        offsets,
                        values,
                        validity,
                    )
                }
                (Encoding::Plain, None, true) => {
                    let (validity_buffer, values_buffer) =
                        utils::split_buffer_v2(page.buffer(), def_level_buffer_length);
                    read_optional::<O>(
                        validity_buffer,
                        values_buffer,
                        page.num_values() as u32,
                        offsets,
                        values,
                        validity,
                    )
                }
                (Encoding::Plain, None, false) => {
                    read_required::<O>(page.buffer(), page.num_values() as u32, offsets, values)
                }
                _ => {
                    return Err(utils::not_implemented(
                        &page.encoding(),
                        is_optional,
                        page.dictionary_page().is_some(),
                        "V2",
                        "Binary",
                    ))
                }
            }
        }
    };
    Ok(())
}
