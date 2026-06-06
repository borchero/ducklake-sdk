use indexmap::IndexMap;
use itertools::{EitherOrBoth, Itertools};

pub fn iter_index_map_diff<'a, K, V>(
    lhs: &'a IndexMap<K, V>,
    rhs: &'a IndexMap<K, V>,
) -> impl Iterator<Item = EitherOrBoth<(&'a K, &'a V)>>
where
    K: Ord,
{
    let iter_lhs = lhs.iter().sorted_by_key(|(k, _)| *k);
    let iter_rhs = rhs.iter().sorted_by_key(|(k, _)| *k);
    iter_lhs.merge_join_by(iter_rhs, |(k1, _), (k2, _)| k1.cmp(k2))
}

pub fn iter_vec_diff<'a, T, K>(
    lhs: &'a [T],
    rhs: &'a [T],
    key_fn: impl Fn(&T) -> K + 'a,
) -> impl Iterator<Item = EitherOrBoth<&'a T>>
where
    K: Ord,
{
    let iter_lhs = lhs.iter().sorted_by_key(|v| key_fn(v));
    let iter_rhs = rhs.iter().sorted_by_key(|v| key_fn(v));
    iter_lhs.merge_join_by(iter_rhs, move |v1, v2| key_fn(v1).cmp(&key_fn(v2)))
}
