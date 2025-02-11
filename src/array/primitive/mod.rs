use crate::{bitmap::Bitmap, buffer::Buffer, datatypes::*, error::ArrowError, types::NativeType};

use super::Array;
use alloc::boxed::Box;
/// A [`PrimitiveArray`] is arrow's equivalent to `Vec<Option<T: NativeType>>`, i.e.
/// an array designed for highly performant operations on optionally nullable slots,
/// backed by a physical type of a physical byte-width, such as `i32` or `f64`.
/// The size of this struct is `O(1)` as all data is stored behind an [`alloc::sync::Arc`].
#[derive(Debug, Clone)]
pub struct PrimitiveArray<T: NativeType> {
    data_type: DataType,
    values: Buffer<T>,
    validity: Option<Bitmap>,
    offset: usize,
}

impl<T: NativeType> PrimitiveArray<T> {
    /// Returns a new empty [`PrimitiveArray`].
    pub fn new_empty(data_type: DataType) -> Self {
        Self::from_data(data_type, Buffer::new(), None)
    }

    /// Returns a new [`PrimitiveArray`] whose all slots are null / `None`.
    #[inline]
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        Self::from_data(
            data_type,
            Buffer::new_zeroed(length),
            Some(Bitmap::new_zeroed(length)),
        )
    }

    /// The canonical method to create a [`PrimitiveArray`] out of low-end APIs.
    /// # Panics
    /// This function panics iff:
    /// * `data_type` is not supported by the physical type
    /// * The validity is not `None` and its length is different from the `values`'s length
    pub fn from_data(data_type: DataType, values: Buffer<T>, validity: Option<Bitmap>) -> Self {
        if !T::is_valid(&data_type) {
            Err(ArrowError::InvalidArgumentError(format!(
                "Type {} does not support logical type {}",
                core::any::type_name::<T>(),
                data_type
            )))
            .unwrap()
        }
        if let Some(ref validity) = validity {
            assert_eq!(values.len(), validity.len());
        }
        Self {
            data_type,
            values,
            validity,
            offset: 0,
        }
    }

    /// Returns a slice of this [`PrimitiveArray`].
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase two ref counts.
    /// # Panic
    /// This function panics iff `offset + length >= self.len()`.
    #[inline]
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let validity = self.validity.clone().map(|x| x.slice(offset, length));
        Self {
            data_type: self.data_type.clone(),
            values: self.values.clone().slice(offset, length),
            validity,
            offset: self.offset + offset,
        }
    }

    /// The values [`Buffer`]. Often used to clone the buffer.
    #[inline]
    pub fn values_buffer(&self) -> &Buffer<T> {
        &self.values
    }

    /// The values as a slice.
    #[inline]
    pub fn values(&self) -> &[T] {
        self.values.as_slice()
    }

    /// Safe method to retrieve the value at slot `i`.
    /// Equivalent to `self.values()[i]`.
    #[inline]
    pub fn value(&self, i: usize) -> T {
        self.values()[i]
    }
}

impl<T: NativeType> Array for PrimitiveArray<T> {
    #[inline]
    fn as_any(&self) -> &dyn core::any::Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.values.len()
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn validity(&self) -> &Option<Bitmap> {
        &self.validity
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn Array> {
        Box::new(self.slice(offset, length))
    }
}

mod display;
mod ffi;
mod from;
mod from_natural;
pub use from::Primitive;
mod iterator;
pub use iterator::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::Int32Array;
    use core::iter::FromIterator;

    #[test]
    fn basics() {
        let data = vec![Some(1), None, Some(10)];

        let array = Int32Array::from_iter(data);

        assert_eq!(array.value(0), 1);
        assert_eq!(array.value(1), 0);
        assert_eq!(array.value(2), 10);
        assert_eq!(array.values(), &[1, 0, 10]);
        assert_eq!(
            array.validity(),
            &Some(Bitmap::from_u8_slice(&[0b00000101], 3))
        );
        assert_eq!(array.is_valid(0), true);
        assert_eq!(array.is_valid(1), false);
        assert_eq!(array.is_valid(2), true);

        let array2 = PrimitiveArray::<i32>::from_data(
            DataType::Int32,
            array.values_buffer().clone(),
            array.validity().clone(),
        );
        assert_eq!(array, array2);

        let array = array.slice(1, 2);
        assert_eq!(array.value(0), 0);
        assert_eq!(array.value(1), 10);
        assert_eq!(array.values(), &[0, 10]);
    }

    #[test]
    fn empty() {
        let array = PrimitiveArray::<i32>::new_empty(DataType::Int32);
        assert_eq!(array.values().len(), 0);
        assert_eq!(array.validity(), &None);
    }
}
