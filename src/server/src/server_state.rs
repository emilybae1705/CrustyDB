use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crate::database_state::DatabaseState;
use crate::worker;
use crate::worker::Message;
use common::ids::LogicalTimeStamp;
use common::physical_plan::PhysicalPlan;
use common::traits::transaction_manager_trait::TransactionManagerTrait;
use common::CrustyError;
use std::sync::mpsc;
use std::sync::Mutex;

use crate::{StorageManager, StorageTrait, TransactionManager};

const DB_DIR: &str = "dbs";

pub struct ServerState {
    /// Path wher database files are stored.
    pub storage_path: PathBuf,

    // maps database id to DatabaseState
    pub id_to_db: RwLock<HashMap<u64, &'static DatabaseState>>,

    // runtime_information
    /// active connections indicates what client_id is connected to what db_id
    pub active_connections: RwLock<HashMap<u64, u64>>,

    // Queue for jobs for workers to pick up
    pub task_queue: Mutex<mpsc::Sender<Message>>,

    workers: Mutex<Vec<worker::Worker>>,

    pub storage_manager: &'static StorageManager,
    pub transaction_manager: &'static TransactionManager,
}

impl ServerState {
    pub(crate) fn new(
        storage_path_str: String,
        task_queue: mpsc::Sender<Message>,
    ) -> Result<Self, CrustyError> {
        let storage_path = PathBuf::from(&storage_path_str);
        if !storage_path.exists() {
            debug!(
                "Storage directory {:?} does not exist. Creating the base directory.",
                storage_path
            );
            // Create dirs if they do not exist.
            fs::create_dir_all(&storage_path)?;
        }
        // Create the storage manager. Leak so it has a static lifetime
        let sm_box = Box::new(StorageManager::new(storage_path.clone()));
        let sm: &'static StorageManager = Box::leak(sm_box);

        let tm_box = Box::new(TransactionManager::new(&storage_path));
        let tm: &'static TransactionManager = Box::leak(tm_box);

        // Create databases
        let mut db_map = HashMap::new();
        let mut db_storage_dir = storage_path.clone();
        db_storage_dir.push(DB_DIR);
        debug!("Looking for databases in {:?}", db_storage_dir);
        if db_storage_dir.exists() {
            let dbs = fs::read_dir(db_storage_dir).expect("Unable to read DB storage dir");
            {
                // for each path, create a DatabaseState
                for db in dbs {
                    let db = db.unwrap();
                    let db_path = db.path();
                    debug!("Creating DatabaseState from path {:?}", db_path);
                    // let db_struct: Database = Database::load(db);
                    let db_box = Box::new(DatabaseState::load(db_path, sm, tm)?);
                    let db_state: &'static DatabaseState = Box::leak(db_box);
                    db_map.insert(db_state.id, db_state);
                }
            }
        } else {
            //Create the directory for storing DB data
            fs::create_dir_all(&db_storage_dir).expect("Error creating storage directory for DB");
        }

        let server_state = ServerState {
            id_to_db: RwLock::new(db_map),
            active_connections: RwLock::new(HashMap::new()),
            /// Path to store database files.
            storage_path,
            task_queue: Mutex::new(task_queue),
            workers: Mutex::new(Vec::new()),
            storage_manager: sm,
            transaction_manager: tm,
        };

        Ok(server_state)
    }

    fn get_db_id_from_db_name(&self, db_name: &str) -> Result<u64, CrustyError> {
        let map_ref = self.id_to_db.read().unwrap();
        for (db_id, db_state) in map_ref.iter() {
            if db_state.name == db_name {
                return Ok(*db_id);
            }
        }
        Err(CrustyError::CrustyError(String::from("db_name not found!")))
    }

    #[allow(clippy::unnecessary_wraps)]
    pub(crate) fn shutdown(&self) -> Result<(), CrustyError> {
        info!("Shutting down");
        debug!("Sending terminate message to all workers.");

        let mut workers = self.workers.lock().unwrap();
        {
            //Send terminate to workers
            let task_queue = self.task_queue.lock().unwrap();

            for _ in 0..workers.len() {
                task_queue.send(Message::Terminate).unwrap();
            }
        }

        debug!("Shutting down all workers.");
        for worker in workers.iter_mut() {
            debug!("Shutting down worker {}", worker.id);

            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }

        // Shutdown/persist DB state
        let db_map = self.id_to_db.read().unwrap();
        let mut db_storage_dir = self.storage_path.clone();
        db_storage_dir.push(DB_DIR);
        if !db_storage_dir.exists() {
            fs::create_dir_all(&db_storage_dir)?;
        }
        debug!("Saving DB state to {:?}", db_storage_dir);
        for (_id, dbstate) in db_map.iter() {
            let name = &dbstate.name;
            let mut filename = db_storage_dir.clone();
            filename.push(name);
            serde_json::to_writer(
                fs::File::create(filename).expect("error creating file"),
                &dbstate.database,
            )
            .expect("error serializing db");
        }

        // call shutdown on SM to ensure stateful shutdown
        self.storage_manager.shutdown();
        error!("TODO no one is shutting down daemon properly");
        //debug!("Shutting down daemon.");
        //if let Some(thread) = daemon_thread.thread.take() {
        //    thread.join().unwrap();
        //}

        Ok(())
    }

    /// Resets database to an empty database.
    pub fn reset_database(&self) -> Result<String, CrustyError> {
        // Clear data structures state
        info!("Resetting database... [To implement]");

        // Clear out each DB state
        let mut db_states = self.id_to_db.write().unwrap();
        let mut conns = self.active_connections.write().unwrap();
        for db in db_states.values() {
            db.reset()?;
        }
        db_states.clear();

        // Reset active connections
        conns.clear();

        // Clear the storage manager
        self.storage_manager.reset()?;

        info!("Resetting database...DONE");
        Ok(String::from("Reset"))
    }

    pub fn close_client_connection(&self, client_id: u64) {
        // putting read/write grabs in separate scopes to avoid the same thread
        // from write-starving active_connections using different scopes to allow
        // for parallelism during portions of this function
        {
            // indicate DB this client is disconnecting
            let db_id_ref = self.active_connections.read().unwrap();
            match db_id_ref.get(&client_id) {
                Some(db_id) => {
                    let db_ref = self.id_to_db.read().unwrap();
                    let db = db_ref.get(db_id).unwrap();
                    db.close_client_connection(client_id);
                }
                None => {
                    debug!("Client was not connected to DB");
                }
            };
        }

        {
            // remove this client from active connections
            self.active_connections.write().unwrap().remove(&client_id);
            info!(
                "Shutting down client connection with ID: {:?}...",
                client_id
            );
        }
    }

    /// Add workers to the worker queue
    pub(crate) fn add_workers(&self, new_workers: Vec<worker::Worker>) {
        let mut workers = self.workers.lock().unwrap();
        workers.extend(new_workers);
    }

    /// Creates a new database with name.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the new database.
    ///
    /// # Notes
    ///
    /// * The database is currently in-memory.
    pub fn create_database(&self, name: String) -> Result<String, CrustyError> {
        // Create new DB
        // Represent newly created DB in server state
        if self
            .id_to_db
            .read()
            .unwrap()
            .contains_key(&DatabaseState::get_database_id(&name))
        {
            Err(CrustyError::CrustyError(format!(
                "database with name {:?} already exists",
                &name
            )))
        } else {
            let db_state_box = Box::new(
                DatabaseState::new_from_name(&name, self.storage_manager, self.transaction_manager)
                    .unwrap(),
            );
            let db_state: &'static DatabaseState = Box::leak(db_state_box);
            self.id_to_db.write().unwrap().insert(db_state.id, db_state);
            Ok(format!("Created database {:?}", &name))
        }
    }

    pub fn connect_to_db(&self, db_name: String, client_id: u64) -> Result<String, CrustyError> {
        let db_id = self.get_db_id_from_db_name(&db_name)?;
        let map_ref = self.id_to_db.read().unwrap();
        let db_state = map_ref.get(&db_id).unwrap();
        {
            let mut reference = self.active_connections.write().unwrap();
            reference.insert(client_id, db_state.id);
        }
        db_state.register_new_client_connection(client_id);
        Ok(format!("Connected to database {:?}", &db_name))
    }

    /// Get name and path from string.
    ///
    /// # Arguments
    ///
    /// * `input_string` - Input string containing name and path.
    pub fn parse_name_and_path(input_string: &str) -> (&str, &str) {
        let mut flag = false;

        // TODO: Use itertools to clean this up?
        let mut path = "";
        let mut name = "";
        for token in input_string.split_whitespace() {
            if flag {
                name = token;
            } else {
                path = token;
                flag = true;
            }
        }
        (name, path)
    }

    /// Register a query.
    ///
    /// # Arguments
    ///
    /// * `name_and_plan_path` - Name and path to the query plan json seperated by whitespace.
    pub fn register_query(
        &self,
        name_and_plan_path: String,
        client_id: u64,
    ) -> Result<String, CrustyError> {
        let (query_name, json_path) = ServerState::parse_name_and_path(&name_and_plan_path);

        if query_name.is_empty() {
            return Err(CrustyError::CrustyError(String::from(
                "Must give query a name",
            )));
        }

        // Read file.
        match fs::File::open(json_path) {
            Ok(mut file) => {
                let mut content = String::new();
                file.read_to_string(&mut content).unwrap();
                let query_plan = Arc::new(PhysicalPlan::from_json(&content)?);

                let db_id_ref = self.active_connections.read().unwrap();
                let db_id = db_id_ref.get(&client_id).unwrap();
                let db_state_ref = self.id_to_db.read().unwrap();
                let db_state = db_state_ref.get(db_id).unwrap();
                db_state.register_query(
                    query_name.to_string(),
                    json_path.to_string(),
                    query_plan,
                )?;
                Ok("Registered query".to_string())
            }
            Err(error) => Err(CrustyError::CrustyError(format!(
                "Error opening file {}: {}",
                json_path, error
            ))),
        }
    }

    /// Update metadata for beginning to run a registered query.
    ///
    /// # Arguments
    ///
    /// * `query_name` - Name of the query.
    /// * `start_timestamp` - Optional start timestamp.
    /// * `end_timestamp` - End timestamp.
    pub fn begin_query(
        &self,
        query_name: &str,
        start_timestamp: Option<LogicalTimeStamp>,
        end_timestamp: LogicalTimeStamp,
        client_id: u64,
    ) -> Result<Arc<PhysicalPlan>, CrustyError> {
        let db_id_ref = self.active_connections.read().unwrap();
        let db_id = db_id_ref.get(&client_id).unwrap();
        let db_state_ref = self.id_to_db.read().unwrap();
        let db_state = db_state_ref.get(db_id).unwrap();
        db_state.begin_query(query_name, start_timestamp, end_timestamp)
    }

    /// Update metadata at end of a query.
    ///
    /// # Arguments
    ///
    /// * `query_name` - Name of the query.
    pub fn finish_query(&self, query_name: &str, client_id: u64) -> Result<(), CrustyError> {
        let db_id_ref = self.active_connections.read().unwrap();
        let db_id = db_id_ref.get(&client_id).unwrap();
        let db_state_ref = self.id_to_db.read().unwrap();
        let db_state = db_state_ref.get(db_id).unwrap();
        db_state.finish_query(query_name)
    }
}
