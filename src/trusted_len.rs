use core::slice::Iter;

/// An iterator of known, fixed size.
/// A trait denoting Rusts' unstable [TrustedLen](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
/// This is re-defined here and implemented for some iterators until `core::iter::TrustedLen`
/// is stabilized.
pub unsafe trait TrustedLen: Iterator {}

unsafe impl<T> TrustedLen for Iter<'_, T> {}

unsafe impl<B, I: TrustedLen, T: FnMut(I::Item) -> B> TrustedLen for core::iter::Map<I, T> {}

unsafe impl<'a, I, T: 'a> TrustedLen for core::iter::Copied<I>
where
    I: TrustedLen<Item = &'a T>,
    T: Copy,
{
}

unsafe impl<I> TrustedLen for core::iter::Enumerate<I> where I: TrustedLen {}

unsafe impl<A, B> TrustedLen for core::iter::Zip<A, B>
where
    A: TrustedLen,
    B: TrustedLen,
{
}

unsafe impl<T> TrustedLen for core::slice::Windows<'_, T> {}

unsafe impl<A, B> TrustedLen for core::iter::Chain<A, B>
where
    A: TrustedLen,
    B: TrustedLen<Item = A::Item>,
{
}

unsafe impl<T> TrustedLen for core::iter::Once<T> {}

unsafe impl<T> TrustedLen for alloc::vec::IntoIter<T> {}

unsafe impl<A: Clone> TrustedLen for core::iter::Repeat<A> {}
unsafe impl<A, F: FnMut() -> A> TrustedLen for core::iter::RepeatWith<F> {}
unsafe impl<A: TrustedLen> TrustedLen for core::iter::Take<A> {}
