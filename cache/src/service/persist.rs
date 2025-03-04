use crate::Cache;
use crate::Persistence;

//extern crate event_stats;
use event_stats::{Waits,Event};

use crate::QueryMsg;

use std::collections::{HashMap,  VecDeque};
use std::sync::Arc;

use tokio::task;
use tokio::sync::Mutex;

const MAX_PRESIST_TASKS: u8 = 8;

struct Lookup<K,V>(HashMap<K, Arc<Mutex<V>>>);


impl<K,V> Lookup<K,V> {
    fn new() -> Lookup<K,V> {
        Lookup::<K,V>(HashMap::new())
    }
}

// Pending persistion queue
struct PendingQ<K>(VecDeque<K>);


impl<K: std::cmp::PartialEq> PendingQ<K> {
    fn new() -> Self {
        PendingQ::<K>(VecDeque::new())
    }

    fn remove(&mut self, K: &K) {
        let mut ri = 0;
        let mut found = false;

        for (i,v) in self.0.iter().enumerate() {
            if *v == *K {
                ri = i;
                found = true;
                break;
            }
        }
        if found {
            self.0.remove(ri);
        }
    }
}

//  container for clients querying persist service
struct QueryClient<K>(HashMap<K, VecDeque<tokio::sync::mpsc::Sender<bool>>>);

impl<K> QueryClient<K> {
    fn new() -> Self {
        QueryClient::<K>(HashMap::new())
    }
}

// struct Persisted(HashSet<K>);


// impl Persisted {

//     fn new() -> Arc<Mutex<Persisted>> {
//        Arc::new(Mutex::new(Persisted(HashSet::new())))
//     }
// }

pub(crate) fn start_service<K,V,D>(
    cache: Cache<K,V>,
    db : D,
    // channels
    mut submit_rx: tokio::sync::mpsc::Receiver<(usize, K, Arc<Mutex<V>>)>,
    mut client_query_rx: tokio::sync::mpsc::Receiver<QueryMsg<K>>,
    mut shutdown_rx: tokio::sync::mpsc::Receiver<u8>,
    //
    waits_ : Waits,
) -> task::JoinHandle<()> 
where K: Clone + std::fmt::Debug + Eq + std::hash::Hash + Send + 'static, 
      V: Clone + Persistence<K,D> + std::fmt::Debug +  'static,
      D: Clone + Send + Sync + 'static
{

    println!("PERSIST  starting persist service: table ");

    //let mut persisted = Persisted::new(); // temmporary - initialise to zero ovb metadata when first persisted
    let mut persisting_lookup = Lookup::new();
    let mut pending_q = PendingQ::new();
    let mut query_client = QueryClient::new();
    let mut tasks = 0;

    // persist channel used to acknowledge to a waiting client that the associated node has completed persistion.
    let (persist_completed_send_ch, mut persist_completed_rx) =
        tokio::sync::mpsc::channel::<(K,usize)>(MAX_PRESIST_TASKS as usize);

    let waits = waits_.clone();

    // persist service only handles
    let persist_server = tokio::spawn(async move {

        loop {
            //let persist_complete_send_ch_=persist_completed_send_ch.clone();
            tokio::select! {
                biased;         // removes random number generation - normal processing will determine order so select! can follow it.
                // note: recv() is cancellable, meaning select! can cancel a recv() without loosing data in the channel.
                // select! will be forced to cancel recv() if another branch event happens e.g. recv() on shutdown_rxannel.
                Some((task, key, arc_node )) = submit_rx.recv() => {

                    //  no locks acquired  - apart from Cache in async routine, which is therefore safe.

                        // persisting_lookup arc_node for given K
                        println!("{} PERSIST: submit persist for {:?} tasks [{}]",task, key, tasks);
                        persisting_lookup.0.insert(key.clone(), arc_node.clone());

                        if tasks >= MAX_PRESIST_TASKS {
                            // maintain a FIFO of evicted nodes
                            println!("{} PERSIST: submit - max tasks reached add {:?} pending_q {}",task , key, pending_q.0.len());
                            pending_q.0.push_front(key.clone());                         
    
                        } else {
                            // ==============================================
                            // lock arc node - to access type persist method
                            // ==============================================
                            let node_guard_=arc_node.lock().await;

                            let mut node_guard=node_guard_.clone();
                            // spawn async task to persist node
                            let persist_complete_send_ch_=persist_completed_send_ch.clone();
                            let waits=waits.clone();
                            let db=db.clone();
                            
                            tasks+=1;
    
                            tokio::spawn(async move {
    
                                // save Node data to db
                                node_guard.persist(
                                    task
                                    ,db
                                    ,waits
                                    ,persist_complete_send_ch_
                                ).await;
    
                            });
                        }
                        println!("{} PERSIST: submit - Exit",task);

                },

                Some((persist_key,task)) = persist_completed_rx.recv() => {

                    tasks-=1;
                    
                    //println!("{} PERSIST : completed msg:  key {:?}  tasks {}", task, persist_key, tasks);
                    persisting_lookup.0.remove(&persist_key);
                    cache.0.lock().await.unset_persisting(&persist_key);

                    // send ack to waiting client 
                    if let Some(client_chs) = query_client.0.get_mut(&persist_key) {
                        // send ack of completed persistion to waiting client
                        loop {
                            if let Some(v) = client_chs.pop_front() {
                                if let Err(err) = v.send(true).await {
                                    panic!("Error in sending to waiting client that K is evicited [{}]",err)
                                }
                            } else {
                                break
                            }
                        }
                        //
                        query_client.0.remove(&persist_key);
                    }
                    // // process next node in persist Pending Queue
                    if let Some(queued_Key) = pending_q.0.pop_back() {
                        //println!("{} PERSIST : persist next entry in pending_q.... {:?}", task, queued_Key);
                        // spawn async task to persist node
                        let persist_complete_send_ch_=persist_completed_send_ch.clone();

                        let Some(arc_node_) = persisting_lookup.0.get(&queued_Key) else {panic!("Persist service: expected arc_node in Lookup {:?}",queued_Key)};
                        let arc_node = arc_node_.clone();
                        let db=db.clone();
                        let waits=waits.clone();
                        let mut node_guard = arc_node_.lock().await.clone();
                        //let persisted_=persisted.clone();
                        tasks+=1;
       
                        tokio::spawn(async move {
                            // save Node data to db
                            node_guard.persist(
                                task
                                ,db
                                ,waits
                                ,persist_complete_send_ch_
                            ).await;
                        });
                    }
                    //println!("{} PERSIST finished completed msg:  key {:?}  tasks {} ", task, persist_key, tasks);
                },

                Some(query_msg) = client_query_rx.recv() => {

                    // ACK to client whether node is marked evicted
                    if let Some(_) = persisting_lookup.0.get(&query_msg.0) {
                        // register for notification of persist completion.
                        query_client.0
                        .entry(query_msg.0.clone())
                        .and_modify(|e| e.push_back(query_msg.1.clone()))
                        .or_insert_with(||{ let mut d = VecDeque::new(); d.push_back(query_msg.1.clone()); d});
                        
                        if let Some(client_chs) = query_client.0.get(&query_msg.0) {
                            println!("{} PERSIST : client query vecdeque len {} {:?}",query_msg.2, client_chs.len(), query_msg.0);  
                        }
                        // send ACK (true) to client 
                        println!("{} PERSIST : send ACK (true) to client {:?}",query_msg.2 , query_msg.0);
                        if let Err(err) = query_msg.1.send(true).await {
                            panic!("Error in sending query_msg [{}]",err)
                        };
                        
                    } else {
                        
                        // send ACK (false) to client 
                        if let Err(err) = query_msg.1.send(false).await {
                            panic!("Error in sending query_msg [{}]",err)
                        };                
                    }

                    println!("{} PERSIST :  client_query exit {:?}", query_msg.2 , query_msg.0);
                },

                _ = shutdown_rx.recv() => {
                        println!("PERSIST shutdown:  Waiting for remaining persist tasks [{}] pending_q {} to complete...",tasks as usize, pending_q.0.len());
                        while tasks > 0 || pending_q.0.len() > 0 {
                            if tasks > 0 {
                                let Some(persist_key) = persist_completed_rx.recv().await else {panic!("Inconsistency; expected task complete msg got None...")};
                                tasks-=1;
                                let task = persist_key.1;
                                // send to client if one is waiting on query channel. Does not block as buffer size is 1.
                                if let Some(client_chs) = query_client.0.get(&persist_key.0) {
                                    // send ack of completed persistion to waiting client
                                    for v in client_chs {
                                        if let Err(err) = v.send(true).await {
                                            panic!("Error in sending to waiting client that K is evicited [{}]",err)
                                        }
                                    }
                                    //
                                    query_client.0.remove(&persist_key.0);
                                }
                            }
                            if let Some(queued_Key) = pending_q.0.pop_back() {
  
                                let persist_complete_send_ch_=persist_completed_send_ch.clone();
                                let Some(arc_node_) = persisting_lookup.0.get(&queued_Key) else {panic!("Persist service: expected arc_node in Lookup")};
                                let waits=waits.clone();
                                let mut node_guard= arc_node_.lock().await.clone();
                                let db=db.clone();
                                tasks+=1;

                                //println!("PERSIST: shutdown  persist task tasks {} Pending-Q {}", tasks, pending_q.0.len() );
                                // save Node data to db
                                tokio::spawn(async move {
                                    // save Node data to db
                                    node_guard.persist(
                                        0
                                        ,db
                                        ,waits
                                        ,persist_complete_send_ch_
                                    ).await;
                            });
                            }
                        }
                        println!("PERSIST  shutdown completed. Tasks {}",tasks);

                        break;
                },
            }
        } // end-loop
    });
    persist_server
}


