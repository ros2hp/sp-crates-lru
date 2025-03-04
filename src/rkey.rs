use super::*;

use crate::cache::{Cache, CacheValue};
use crate::node::RNode;
use std::fmt::Debug;

// Reverse_SK is the SK value for the Child of form R#<parent-node-type>#:<parent-edge-attribute-sn>
type ReverseSK = String;

#[derive(Eq, PartialEq, Hash, Debug, Clone, PartialOrd, Ord)]
pub struct RKey(pub Uuid, pub ReverseSK);

impl RKey {
    pub fn new(n: Uuid, reverse_sk: ReverseSK) -> RKey {
        RKey(n, reverse_sk)
    }

    pub async fn add_reverse_edge(
        &self,
        task: usize,
        dyn_client: &DynamoClient,
        table_name: &str, //
        mut cache: Cache<RKey, RNode>, //
        target: &Uuid,
        id: usize,
    ) {
        //println!("{} ------------------------------------------------ {:?}",task, self);
        //println!("{} RKEY add_reverse_edge: about to get  {:?} ",task, self);

        match cache.clone().get(&self, task).await {
            CacheValue::New(node) => {
                println!("{} RKEY add_reverse_edge: New  1 {:?} ", task, self);
                let mut node_guard = node.lock().await;

                node_guard
                    .load_ovb_metadata(dyn_client, table_name, self, task)
                    .await;
                node_guard.add_reverse_edge(target.clone(), id as u32);
            }

            CacheValue::Existing(node) => {
                println!("{} RKEY add_reverse_edge: Existing  1 {:?} ", task, self);
                let mut node_guard = node.lock().await;

                node_guard.add_reverse_edge(target.clone(), id as u32);
            }
        }
    }
}
