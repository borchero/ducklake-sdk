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
