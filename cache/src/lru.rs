use super::*;
//extern crate event_stats;

use crate::Cache;
use event_stats::{Waits,Event};

use std::hash::Hash;
use std::cmp::Eq;
use std::fmt::Debug;
use std::collections::HashMap;
use std::sync::Arc;

use tokio::time::{sleep, Duration, Instant};
use tokio::sync::Mutex;

pub enum LruAction {
    Attach,
    Move_to_head,
}

// lru is used only to drive lru_entry eviction.
// the lru_entry cache is separate
#[derive(Clone,Debug)]
struct Entry<K: Debug>{
    pub key: K,
    //
    pub next: Option<Arc<Mutex<Entry<K>>>>,  // TODO: as LRU is a service and Entry is local to service remove Mutex and Arc. NO - DOESN'T WORK.
    pub prev: Option<Arc<Mutex<Entry<K>>>>,
}

impl<K: Debug> Entry<K> {
    fn new(k : K) -> Entry<K> {
        Entry{key: k
            ,next: None
            ,prev: None
        }
    }
}


// impl Drop for Entry {
//         fn drop(&mut self) {
//         //println!("\nDROP LRU Entry {:?}\n",self.key);
//     }
// }


#[derive(Clone, Debug)]
pub(crate) struct LRU<K : Debug,V> {
    capacity: usize,
    cnt : usize,
    // pointer to Entry value in the LRU linked list for a K
    lookup : HashMap<K,Arc<Mutex<Entry<K>>>>,
    //
    persist_submit_ch: tokio::sync::mpsc::Sender<(usize, K, Arc<Mutex<V>>)>,
    // record stat waits
    waits : Waits,
    //
    head: Option<Arc<Mutex<Entry<K>>>>,
    tail: Option<Arc<Mutex<Entry<K>>>>,
}
    // 371 |       let lru_server = tokio::task::spawn( async move { 
    //     |  __________________________________________^
    // 372 | |         loop {
    // 373 | |             tokio::select! {
    // 374 | |                 //biased;         // removes random number generation - normal processing will determine order so select! can follow it.
    // ...   |
    // 438 | |         }               
    // 439 | |     }); 
    //     | |_____^ future created by async block is not `Send`
    // head: Option<Rc<RefCell<Entry>>>,
    // tail: Option<Rc<RefCell<Entry>>>,


// implement attach & move_to_head as trait methods.
// // Makes more sense however for these methods to be part of the LRU itself - just epxeriementing with traits.
// pub trait LRU {
//     fn attach(
//         &mut self, // , cache_guard:  &mut std::sync::MutexGuard<'_, Cache::<K,V>>
//         K: K,
//     );

//     //pub async fn detach(&mut self
//     fn move_to_head(
//         &mut self,
//         K: K,
//     );
// }


impl<K: Hash + Eq + Debug,V> LRU<K,V> 
where K: std::cmp::Eq + std::hash::Hash + std::fmt::Debug + Clone + std::marker::Send
{  
    pub fn new(
        cap: usize,
        persist_submit_ch: tokio::sync::mpsc::Sender<(usize, K, Arc<tokio::sync::Mutex<V>>)>,
        waits : Waits
    ) -> Self {
        LRU{
            capacity: cap,
            cnt: 0,
            lookup: HashMap::new(),
            // send channels for persist requests
            persist_submit_ch: persist_submit_ch,
            // record stat waits
            waits,
            //
            head: None,
            tail: None,
        }
    }
    
    
    async fn print(&self,context : &str) {

        let mut entry = self.head.clone();
        println!("*** print LRU chain *** len - {} ",context);
        let mut i = 0;
        while let Some(entry_) = entry {
            i+=1;
            let k = entry_.lock().await.key.clone();
            println!("LRU Print [{}] {} {:?}",context, i, k);
            entry = entry_.lock().await.next.clone();      
        }
        println!("*** print LRU chain DONE ***");
    }

}
impl<K,V> LRU<K,V> 
where K: std::cmp::Eq + std::hash::Hash + std::fmt::Debug + Clone + std::marker::Send, V:  Clone + Debug
{    
    // prerequisite - lru_entry has been confirmed NOT to be in lru-cache.
    // note: can only execute methods on LRU if lock has been acquired via Arc<Mutex<LRU>>
    pub async fn attach(
        &mut self, // , cache_guard:  &mut tokio::sync::MutexGuard<'_, Cache::<K,V>>
        task : usize,
        key : K,
        mut cache: Cache<K,V>,
    ) {

        println!("{} LRU attach {:?}. ***********", task, key);  
        let mut lc = 0;   

        while self.cnt >= self.capacity && lc < 3  {
        
            lc += 1;
            // ================================
            // Evict the tail entry in the LRU 
            // ================================
            println!("{} LRU: attach reached LRU capacity - evict tail  lru.cnt {}  lc {}  key {:?}", task, self.cnt, lc,  key);
            // unlink tail lru_entry from lru and notify evict service.
            // Clone REntry as about to purge it from cache.
            let lru_entry = self.tail.as_ref().unwrap().clone();
            let mut evict_entry = lru_entry.lock().await;
            // ================================
            // Lock cache
            // ================================
            let before = Instant::now();
            let mut cache_guard = cache.0.lock().await;
            let Some(arc_evict_node_) = cache_guard.datax.get(&evict_entry.key)  
                        else { println!("{} LRU: PANIC - attach evict processing: expect entry in cache {:?}",task, evict_entry.key);
                               panic!("LRU: attach evict processing: expect entry in cache {:?} len {} ",evict_entry.key, cache_guard.datax.len());
                            };
            let arc_evict_node=arc_evict_node_.clone();
            // ==========================
            // acquire lock on evict node 
            // ==========================
            let tlock_result = arc_evict_node.try_lock();
            match tlock_result {

                Ok(mut evict_node_guard)  => {
                    // ============================
                    // check state of entry in cache
                    // ============================
                    if cache_guard.inuse(&evict_entry.key) {
                        println!("{} LRU attach evict -  cannot evict node as inuse set - abort eviction {:?}", task, evict_entry.key);
                        sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                    cache_guard.set_persisting(evict_entry.key.clone());
                    // ============================
                    // remove node from cache
                    // ============================
                    cache_guard.datax.remove(&evict_entry.key);   
                    // ===================================
                    // detach evict entry from tail of LRU
                    // ===================================
                    match evict_entry.prev {
                        None => {panic!("LRU attach - evict_entry - expected prev got None")}
                        Some(ref new_tail) => {
                            let mut tail_guard = new_tail.lock().await;
                            tail_guard.next = None;
                            self.tail = Some(new_tail.clone());
                        }
                    }
                    evict_entry.prev=None;
                    evict_entry.next=None;
                    self.cnt-=1;
                    // =====================
                    // remove from lru lookup 
                    // =====================
                    self.lookup.remove(&evict_entry.key);
                    // ================================
                    // release cache lock
                    // ================================
                    drop(cache_guard);  
                    drop(evict_node_guard); // required by persist 
                    // =====================
                    // notify persist service
                    // =====================
                    if let Err(err) = self
                        .persist_submit_ch
                        .send((task, evict_entry.key.clone(), arc_evict_node.clone()))
                        .await
                    {
                        println!("{} LRU Error sending on Evict channel: [{}]", task,err);
                    }
                    println!("{} LRU: attach evict - waiting on persist client_rx...{:?}",task, evict_entry.key);
                    //if let None= self.client_rx.recv().await {
                    //    panic!("LRU read from client_rx is None ");
                   // }
                    // =====================================================================
                    // cache lock released - now that submit persist has been sent (queued)
                    // =====================================================================
                }
                Err(err) =>  {
                    // Abort eviction - as node is being accessed.
                    // TODO check if error is "node locked"
                    println!("{} LRU attach - lock cannot be acquired - abort eviction {:?}",task, evict_entry.key);
                    break;
                }
            }
            self.waits.record(Event::LRUCacheLock, Instant::now().duration_since(before)).await;  
        }
        // ======================
        // attach to head of LRU
        // ======================
        {
        let arc_new_entry = Arc::new(Mutex::new(Entry::new(key.clone())));
        match self.head.clone() {
            None => { 
                // empty LRU   
                self.head = Some(arc_new_entry.clone());
                self.tail = Some(arc_new_entry.clone());
                }
            
            Some(e) => {
                let mut new_entry_guard: tokio::sync::MutexGuard<'_, Entry<K>> = arc_new_entry.lock().await;
                let mut old_head_guard = e.lock().await;
                // set old head prev to point to new entry
                old_head_guard.prev = Some(arc_new_entry.clone());
                // set new entry next to point to old head entry & prev to NOne   
                new_entry_guard.next = Some(e.clone());
                new_entry_guard.prev = None;
                // set LRU head to point to new entry
                self.head=Some(arc_new_entry.clone());

                if let None = new_entry_guard.next {
                    panic!("LRU INCONSISTENCY attach: expected Some for next but got NONE {:?}",key);
                }
                if let Some(_) = new_entry_guard.prev {
                    panic!("LRU INCONSISTENCY attach: expected None for prev but got NONE {:?}",key);
                }
                if let None = old_head_guard.prev {
                    panic!("LRU INCONSISTENCY attach: expected entry to have prev set to NONE {:?}",key);
                }
            }
        }
        if let Some(_) = self.lookup.get(&key) {
            println!("{} LRU INCONSISTENCY - attach entry exists in lookup {:?}",task, key);
            panic!("LRU INCONSISTENCY - attach entry exists in lookup {:?}",key)
        }
        self.lookup.insert(key, arc_new_entry);
        
        if let None = self.head {
                panic!("LRU INCONSISTENCY attach: expected LRU to have head but got NONE")
        }
        if let None = self.tail {
                panic!("LRU INCONSISTENCY attach: expected LRU to have tail but got NONE")
        }
        self.cnt+=1;
        println!("{} LRU: attach add cnt {}",task, self.cnt);
        }
        //self.print("attach").await;
    }
    
    
    // prerequisite - lru_entry has been confirmed to be in lru-cache.\/
    // to execute a method a lock has been taken out on the LRU
    pub async fn move_to_head(
        &mut self,
        task : usize,
        key: K,
    ) {  
         {
        //println!("--------------");
        println!("{} LRU move_to_head {:?} ********",task, key);
        // abort if lru_entry is at head of lru
        match self.head {
            None => {
                panic!("{} LRU empty - expected entries",task);
            }
            Some(ref v) => {
                let hd: K = v.lock().await.key.clone();
                if hd == key {
                    // k already at head
                    return
                }    
            }
        }
        // lookup entry in map
        let lru_entry = match self.lookup.get(&key) {
            None => {  
                    println!("{} PANIC LRU INCONSISTENCY no lookup entry durin move-to-head - Reason: task maybe running out of order. {:?}",task, key);
                    panic!("{} LRU INCONSISTENCY no lookup entry - tasks running out of order. {:?}",task, key);
                    }
            Some(v) => v.clone()
        };
        {
            // DETACH the entry before attaching to LRU head
            
            let lru_entry_guard = lru_entry.lock().await;
            // NEW CODE to fix eviction and new request at same time on a Node
            if let None = lru_entry_guard.prev {
                ////println!("LRU INCONSISTENCY move_to_head: expected entry to have prev but got NONE {:?}",k);
                if let None = lru_entry_guard.next {
                    // should not happend
                    panic!("{} LRU move_to_head : got a entry with no prev or next set (ie. a new node) - some synchronisation gone astray",task)
                }
            }

            // check if moving tail entry
            if let None = lru_entry_guard.next {
            
                println!("{} LRU move_to_head detach tail entry {:?}",task, key);
                let mut prev_guard = lru_entry_guard.prev.as_ref().unwrap().lock().await;
                prev_guard.next = None;
                self.tail = Some(lru_entry_guard.prev.as_ref().unwrap().clone());
                
            } else {
                
                let mut prev_guard = lru_entry_guard.prev.as_ref().unwrap().lock().await;
                let mut next_guard = lru_entry_guard.next.as_ref().unwrap().lock().await;

                prev_guard.next = Some(lru_entry_guard.next.as_ref().unwrap().clone());
                next_guard.prev = Some(lru_entry_guard.prev.as_ref().unwrap().clone());
       
            }
            println!("{} LRU move_to_head Detach complete...{:?}",task, key);
        }
        // ATTACH
        let mut lru_entry_guard = lru_entry.lock().await;
        lru_entry_guard.next = self.head.clone();
        
        // adjust old head entry previous pointer to new entry (aka LRU head)
        match self.head {
            None => {
                 panic!("LRU empty - expected entries");
            }
            Some(ref v) => {
                let mut hd = v.lock().await;
                hd.prev = Some(lru_entry.clone());
                println!("{} LRU move_to_head: attach old head {:?} prev to {:?} ",task, hd.key, lru_entry_guard.key);
            }
        }
        lru_entry_guard.prev = None;
        self.head = Some(lru_entry.clone());
        }
        //self.print("move2head").await;
        println!("{} LRU move_to_head complete...{:?}",task, key);
    }

    pub(crate) async fn flush(
        self
        ,cache : Cache<K,V>) {
                println!("LRU service - flush lru ");
                let mut entry = self.head;
                let mut lc = 0;
                let cache_guard = cache.0.lock().await;
                println!("LRU flush in progress..persist entries in LRU flust {} lru entries",self.cnt);
                while let Some(entry_) = entry {
                                            lc += 1;     
                                            let k = entry_.lock().await.key.clone();
                                            if let Some(arc_node) = cache_guard.datax.get(&k) {
                                                if let Err(err) = self.persist_submit_ch // persist_flush_ch
                                                            .send((0, k, arc_node.clone()))
                                                            .await {
                                                                println!("Error on persist_submit_ch channel [{}]",err);
                                                            }
                                            } 
                                            entry = entry_.lock().await.next.clone();      
                                    }
                                    //sleep(Duration::from_millis(2000)).await;
                                    println!("LRU Flush complete ");

}

}
