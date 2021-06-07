
#[cfg(target_endian = "little")]
#[allow(dead_code)]
#[inline]
pub fn is_native_little_endian() -> bool {
    true
}

#[cfg(target_endian = "big")]
#[allow(dead_code)]
#[inline]
pub fn is_native_little_endian() -> bool {
    false
}
