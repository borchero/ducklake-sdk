/// For many types in this module, we implement transformations to translate between user
/// representation ("logical" representation), the catalog representation, and the native DuckLake
/// representation ("physical" representation).
///
/// In total, we require four transformations:
///
//  1) From the DuckLake representation to the catalog representation ("deserialize I")
//  2) From the catalog representation to the user representation ("deserialize II")
//  3) From the user representation to the catalog representation ("serialize I")
//  4) From the catalog representation to the DuckLake representation ("serialize II")
//
// Additional notes:
//  - Due to the similarity of the catalog and user representation, transformations (2) and (3)
//    can be implemented with the native `From` trait.
//  - Transformation (2) is implemented as "from reference" because this transformation is used
//    for catalog lookups and is not used to move values out of the catalog.
//  - Transformation (4) is changelog-driven as there is no requirement to directly translate
//    a full catalog to a DuckLake representation. Hence, the transform is implemented during
//    commit.
mod columns;
mod partition;
mod schema;
mod state;
mod table;

pub(super) use columns::{CatalogColumn, CatalogColumns};
pub(super) use partition::CatalogTablePartition;
pub(super) use schema::CatalogSchema;
pub(super) use state::CatalogState;
pub(super) use table::CatalogTable;

use super::ArenaIdx;
