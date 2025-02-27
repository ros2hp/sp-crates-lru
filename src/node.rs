use super::*;

use crate::types;
use crate::rkey::RKey;

use cache::{Persistence,NewValue};
//use cache::event_stats::Waits;
//use cache::Waits;
use cache::event_stats;       // use x::? where ? can be type, mod, crate

use std::fmt::Debug;

use aws_sdk_dynamodb::config::http::HttpResponse;
use aws_sdk_dynamodb::operation::update_item::{UpdateItemError, UpdateItemOutput};
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client as DynamoClient;

use tokio::sync::Mutex; 

use aws_smithy_runtime_api::client::result::SdkError;

use uuid::Uuid;


#[derive(Clone, Debug)]
pub struct RNode {
    pub node: Uuid,     // child or associated OvB Uuid
    pub rvs_sk: String, // child or associated OvB batch SK
    pub init_cnt: u32,  // edge count at node initialisation (new or db sourced)
    // accumlate edge data into these Vec's
    pub target_uid: Vec<AttributeValue>,
    pub target_id: Vec<AttributeValue>,
    // metadata that describes how to populate target* into db attributes when persisted
    pub ovb: Vec<Uuid>,  // Uuid of OvB
    pub obid: Vec<u32>,  // current batch id in each OvB
    //pub oblen: Vec<u32>, // count of items in current batch in current batch in each OvB block
    pub oid: Vec<u32>,
    pub ocur: Option<u8>, // current Ovb in use
    pub obcnt: usize, // edge count in current batch of current OvB
    //
    //
}




impl RNode {
    pub fn new() -> RNode {
        RNode {
            node: Uuid::nil(),
            rvs_sk: String::new(), //
            init_cnt: 0,           // edge cnt at initialisation (e.g as read from database)
            target_uid: vec![],
            target_id: vec![], //
            //
            ovb: vec![],
            obid: vec![],
            //oblen: vec![],
            obcnt: 0,
            oid: vec![],
            ocur: None, //
        }
    }


    pub async fn load_ovb_metadata(
        &mut self,
        dyn_client: &DynamoClient,
        table_name: &str,
        rkey: &RKey,
        task: usize,
    ) {
        let projection =  types::CNT.to_string() 
                          + "," + types::OVB
                          + "," + types::OVB_BID 
                          + "," + types::OVB_CUR 
                          + "," + types::OVB_CNT;
        let result = dyn_client
            .get_item()
            .table_name(table_name)
            .key(
                types::PK,
                AttributeValue::B(Blob::new(rkey.0.clone().as_bytes())),
            )
            .key(types::SK, AttributeValue::S(rkey.1.clone()))
            //.projection_expression((&*LOAD_PROJ).clone())
            .projection_expression(projection)
            .send()
            .await;

        if let Err(err) = result {
            panic!(
                "get node type: no item found: expected a type value for node. Error: {}",
                err
            )
        }
        let ri: RNode = match result.unwrap().item {
            None =>  return,
            Some(v) => v.into(), //  {(RNode as From)::from(v)} , //  uses trait: Into<RNode> { fn into(self)-> RNode}
        };
        // update self with db data
        self.init_cnt = ri.init_cnt;
        //
        self.ovb = ri.ovb;   
        self.obid = ri.obid; 
        self.obcnt = ri.obcnt; 
        //self.oblen = ri.oblen; 
        self.oid = ri.oid;
        self.ocur = ri.ocur;
        
        if self.ovb.len() > 0 {
            println!("load_ovb_metadata: ovb.len {}. for {:?}",self.ovb.len(), rkey);
        }
    }

    pub fn add_reverse_edge(&mut self, target_uid: Uuid, target_id: u32) {
        //self.cnt += 1; // redundant, use container_uuid.len() and add it to db cnt attribute.
        // accumulate edges into these Vec's. Distribute the data across Dynamodb attributes (aka OvB batches) when persisting to database.
        self.target_uid
            .push(AttributeValue::B(Blob::new(target_uid.as_bytes())));
        self.target_id
            .push(AttributeValue::N(target_id.to_string()));
        
    }
    //
}


//struct NodeMutex<'A>(MutexGuard<'A, RNode>);
// struct NodeMutex<'A, K: InUse>(MutexGuard<'A, K>);

// impl<'A, RNode: InUse> Drop for NodeMutex<'A, RNode> {//MutexGuard<'_, RNode> {

//     fn drop(&mut self) {
//         self.0.unlock();
//         mem::drop(self);
//     }

// }

// impl Drop for RNode {
//     fn drop(&mut self) {
//         println!("DROP RNODE {:?}",self.uuid);
//     }
// }

    // impl CacheApply for RNode {

    //     fn new_entry(&mut self) {
    //         self.load_ovb_metadata(self.dyn_client, self.table_name).await;
    //         self.add_reverse_edge(target.clone(), id as u32);
    //     }
    
    //     fn existing_entry(&mut self) {
    //         self.add_reverse_edge(target.clone(), id as u32);
    //     }
    // }


// Populate reverse cache with return values from Dynamodb.
// note: not interested in TARGET* attributes only OvB* attributes (metadata about TARGET*)
impl From<HashMap<String, AttributeValue>> for RNode {
    //    HashMap.into() -> RNode

    fn from(mut value: HashMap<String, AttributeValue>) -> Self {
        let mut edge = RNode::new();

        for (k, v) in value.drain() {
            match k.as_str() {
                types::PK => edge.node = types::as_uuid(v).unwrap(),
                types::SK => edge.rvs_sk = types::as_string(v).unwrap(),
                //
                types::CNT => edge.init_cnt = types::as_u32_2(v).unwrap(),
                //
                types::OVB => edge.ovb = types::as_luuid(v).unwrap(),
                types::OVB_CNT => edge.obcnt = types::as_u32_2(v).unwrap() as usize,
                types::OVB_BID => edge.obid = types::as_lu32(v).unwrap(),
                //types::OVB_ID => edge.oid = types::as_lu32(v).unwrap(),
                types::OVB_CUR => edge.ocur = types::as_u8_2(v),
                _ => panic!(
                    "unexpected attribute in HashMap for RNode: [{}]",
                    k.as_str()
                ),
            }
        }
        //println!("NODE....ovb.len {}  oid len {}  init_cnt {} ocur {:?} for {:?}",edge.ovb.len(), edge.obid.len(), edge.init_cnt, edge.ocur, edge.node);
        edge
    }
}


impl NewValue<RKey,RNode> for RNode {

    fn new_with_key(rkey: &RKey) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(RNode{
            node: rkey.0.clone(),
            rvs_sk: rkey.1.clone(), //
            init_cnt: 0,
            target_uid: vec![],     // target_uid.len() total edges added in current sp session
            target_id: vec![], //
            //
            ovb: vec![],
            obcnt:0,
            //oblen: vec![],
            obid: vec![],
            oid: vec![],
            ocur: None, //
        }))
    }
}

impl Persistence<RKey, Dynamo> for RNode {

    async fn persist(
        &mut self,
        task : usize, 
        db: Dynamo,
        waits : event_stats::Waits,
        persist_completed_send_ch: tokio::sync::mpsc::Sender<(RKey, usize)>,
    )
{
            // at this point, cache is source-of-truth updated with db values if edge exists.
            // use db cache values to decide nature of updates to db
            // Note for LIST_APPEND Dynamodb will scan the entire attribute value before appending, so List should be relatively small < 10000.
            let table_name: String = db.table_name; 
            let dyn_client = db.conn;
            let mut target_uid: Vec<AttributeValue>;
            let mut target_id: Vec<AttributeValue>;
            let mut update_expression: &str;
            
            //let mut node = arc_node.lock().await;
            let rkey = RKey::new(self.node,self.rvs_sk.clone());
            let init_cnt =self.init_cnt as usize;
            let edge_cnt =self.target_uid.len() + init_cnt;
            
            if init_cnt <= crate::EMBEDDED_CHILD_NODES {
            
                //println!("*PERSIST  ..init_cnt < EMBEDDED. {:?}", rkey);
            
                if self.target_uid.len() <= crate::EMBEDDED_CHILD_NODES - init_cnt {
                    // consume all of self.target*
                    target_uid = mem::take(&mut self.target_uid);
                    target_id = mem::take(&mut self.target_id);
                } else {
                    // consume portion of self.target*
                    target_uid = self
                        .target_uid
                        .split_off(crate::EMBEDDED_CHILD_NODES - init_cnt);
                    std::mem::swap(&mut target_uid, &mut self.target_uid);
                    target_id = self
                        .target_id
                        .split_off(crate::EMBEDDED_CHILD_NODES - init_cnt);
                    std::mem::swap(&mut target_id, &mut self.target_id);
                }
        
                if init_cnt == 0 {
                    // no data in db
                    update_expression = "SET #cnt = :cnt, #target = :tuid, #id = :id";
                } else {
                    // append to existing data
                   update_expression = "SET #target=list_append(#target, :tuid), #id=list_append(#id,:id), #cnt = :cnt";
                }
                let before = Instant::now();
                //update edge_item
                let result = dyn_client
                    .update_item()
                    .table_name(table_name.clone())
                    .key(
                        types::PK,
                        AttributeValue::B(Blob::new(rkey.0.clone().as_bytes())),
                    )
                    .key(types::SK, AttributeValue::S(rkey.1.clone()))
                    .update_expression(update_expression)
                    // reverse edge
                    .expression_attribute_names("#cnt", types::CNT)
                    .expression_attribute_values(":cnt", AttributeValue::N(edge_cnt.to_string()))
                    .expression_attribute_names("#target", types::TARGET_UID)
                    .expression_attribute_values(":tuid", AttributeValue::L(target_uid))
                    .expression_attribute_names("#id", types::TARGET_ID)
                    .expression_attribute_values(":id", AttributeValue::L(target_id))
                    //.return_values(ReturnValue::AllNew)
                    .send()
                    .await;
                    waits.record(event_stats::Event::PersistEmbedded, Instant::now().duration_since(before)).await;        
               
                    handle_result(&rkey, result);
        
            }
            // consume the target_* fields by moving them into overflow batches and persisting the batch
            // note if node has been loaded from db must drive off ovb meta data which gives state of current 
            // population of overflwo batches
        
            println!("*PERSIST  self.target_uid.len()  {}    {:?}",self.target_uid.len(),rkey);
            while self.target_uid.len() > 0 {
        
                ////println!("PERSIST  logic target_uid > 0 value {}  {:?}", self.target_uid.len(), rkey );
            
                let mut target_uid: Vec<AttributeValue> = vec![];
                let mut target_id: Vec<AttributeValue> = vec![];
                let mut sk_w_bid : String;
                let event : event_stats::Event ;
        
                match self.ocur {
                    None => {
                        // first OvB
                        self.obcnt=crate::OV_MAX_BATCH_SIZE;  // force zero free space - see later.
                        self.ocur = Some(0);
                        continue;
                        }
                    Some(mut ocur) => {
        
                        let batch_freespace = crate::OV_MAX_BATCH_SIZE - self.obcnt;
                        if batch_freespace > 0 {
                        
                            // consume last of self.target*
                            if self.target_uid.len() <= batch_freespace {
                            // consume all of self.target*
                                target_uid = mem::take(&mut self.target_uid);
                                target_id = mem::take(&mut self.target_id);
                                self.obcnt += target_uid.len();
                                
                            } else {
                                
                                // consume portion of self.target*
                                target_uid = self
                                    .target_uid
                                    .split_off(batch_freespace);
                                std::mem::swap(&mut target_uid, &mut self.target_uid);
                                target_id = self.target_id.split_off(batch_freespace);
                                std::mem::swap(&mut target_id, &mut self.target_id);
                                self.obcnt=crate::OV_MAX_BATCH_SIZE;
        
                            }                                        
                            update_expression = "SET #target=list_append(#target, :tuid), #id=list_append(#id, :id)";  
                            event = event_stats::Event::PersistOvbAppend;
                            sk_w_bid = rkey.1.clone();
                            sk_w_bid.push('%');
                            sk_w_bid.push_str(&self.obid[ocur as usize].to_string());
                      
                        } else {
                        
                            // create a new batch optionally in a new OvB
                            if self.ovb.len() < crate::MAX_OV_BLOCKS {
                                // create a new OvB
                                self.ovb.push(Uuid::new_v4());
                                self.obid.push(1);
                                self.obcnt = 0;
                                self.ocur = Some(self.ovb.len() as u8 - 1);
                             
                            } else {
        
                                // change current ovb (ie. block)
                                ocur+=1;
                                if ocur as usize == crate::MAX_OV_BLOCKS {
                                        ocur = 0;
                                }
                                self.ocur = Some(ocur);
                                //println!("PERSIST   33 self.ocur, ocur {}  {}", self.ocur.unwrap(), ocur);
                                self.obid[ocur as usize] += 1;
                                self.obcnt = 0;
                            }                     
                            if self.target_uid.len() <= crate::OV_MAX_BATCH_SIZE {
        
                                // consume remaining self.target*
                                target_uid = mem::take(&mut self.target_uid);
                                target_id = mem::take(&mut self.target_id);
                                self.obcnt += target_uid.len();
                            
                            } else {
        
                                // consume leading portion of self.target*
                                target_uid = self.target_uid.split_off(crate::OV_MAX_BATCH_SIZE);
                                std::mem::swap(&mut target_uid, &mut self.target_uid);
                                target_id = self.target_id.split_off(crate::OV_MAX_BATCH_SIZE);
                                std::mem::swap(&mut target_id, &mut self.target_id);
                                self.obcnt=crate::OV_MAX_BATCH_SIZE;
                            }
                            // ================
                            // add OvB batches
                            // ================
                            sk_w_bid = rkey.1.clone();
                            sk_w_bid.push('%');
                            sk_w_bid.push_str(&self.obid[ocur as usize].to_string());
            
                            update_expression = "SET #target = :tuid, #id = :id";
                            event = event_stats::Event::PersistOvbSet;
                        }
                        // ================
                        // add OvB batches
                        // ================   
                        let before = Instant::now();
                        let result = dyn_client
                            .update_item()
                            .table_name(table_name.clone())
                            .key(
                                types::PK,
                                AttributeValue::B(Blob::new(
                                    self.ovb[self.ocur.unwrap() as usize].as_bytes(),
                                )),
                            )
                            .key(types::SK, AttributeValue::S(sk_w_bid.clone()))
                            .update_expression(update_expression)
                            // reverse edge
                            .expression_attribute_names("#target", types::TARGET_UID)
                            .expression_attribute_values(":tuid", AttributeValue::L(target_uid))
                            .expression_attribute_names("#id", types::TARGET_ID)
                            .expression_attribute_values(":id", AttributeValue::L(target_id))
                            //.return_values(ReturnValue::AllNew)
                            .send()
                            .await;
                        waits.record(event, Instant::now().duration_since(before)).await;        
          
                        handle_result(&rkey, result);
                        //println!("PERSIST : batch written.....{:?}",rkey);
                    }
                }
            } // end while
            // update OvB meta on edge predicate only if OvB are used.
            if self.ovb.len() > 0 {
                update_expression = "SET  #cnt = :cnt, #ovb = :ovb, #obid = :obid, #obcnt = :obcnt, #ocur = :ocur";
        
                let ocur = match self.ocur {
                    None => 0,
                    Some(v) => v,
                };
                let before = Instant::now();
                let result = dyn_client
                    .update_item()
                    .table_name(table_name.clone())
                    .key(types::PK, AttributeValue::B(Blob::new(rkey.0.clone())))
                    .key(types::SK, AttributeValue::S(rkey.1.clone()))
                    .update_expression(update_expression)
                    // OvB metadata
                    .expression_attribute_names("#cnt", types::CNT)
                    .expression_attribute_values(":cnt", AttributeValue::N(edge_cnt.to_string()))
                    .expression_attribute_names("#ovb", types::OVB)
                    .expression_attribute_values(":ovb", types::uuid_to_av_lb(&self.ovb))
                    .expression_attribute_names("#obid", types::OVB_BID)
                    .expression_attribute_values(":obid", types::u32_to_av_ln(&self.obid))
                    .expression_attribute_names("#obcnt", types::OVB_CNT)
                    .expression_attribute_values(":obcnt", AttributeValue::N(self.obcnt.to_string()))
                    .expression_attribute_names("#ocur", types::OVB_CUR)
                    .expression_attribute_values(":ocur", AttributeValue::N(ocur.to_string()))
                    //.return_values(ReturnValue::AllNew)
                    .send()
                    .await;
                    waits.record(event_stats::Event::PersistMeta, Instant::now().duration_since(before)).await;        
             
                handle_result(&rkey, result);
                
            }
        
            // send task completed msg to persist service
            if let Err(err) = persist_completed_send_ch.send((rkey.clone(),task) ).await {
                println!(
                    "Sending completed persist msg to waiting client failed: {}",
                    err
                );
            }
            println!("{} *PERSIST  Exit    {:?}",task, rkey);
            ()
    }
}
        

    fn handle_result(rkey: &RKey, result: Result<UpdateItemOutput, SdkError<UpdateItemError, HttpResponse>>) {
        match result {
            Ok(_out) => {
                ////println!("PERSIST  PRESIST Service: Persist successful update...")
            }
            Err(err) => match err {
                SdkError::ConstructionFailure(_cf) => {
                    //println!("PERSIST   Persist Service: Persist  update error ConstructionFailure...")
                }
                SdkError::TimeoutError(_te) => {
                    //println!("PERSIST   Persist Service: Persist  update error TimeoutError")
                }
                SdkError::DispatchFailure(_df) => {
                    //println!("PERSIST   Persist Service: Persist  update error...DispatchFailure")
                }
                SdkError::ResponseError(_re) => {
                    //println!("PERSIST   Persist Service: Persist  update error ResponseError")
                }
                SdkError::ServiceError(_se) => {
                    panic!(" Persist Service: Persist  update error ServiceError {:?}",rkey);
                }
                _ => {}
            },
        }
    }
