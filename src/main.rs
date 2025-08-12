use std::{sync::Barrier, thread};

use redb::{Database, Error, ReadableDatabase, ReadableTable as _, TableDefinition};

const TABLE: TableDefinition<&str, u64> = TableDefinition::new("my_data");
const KEY: &str = "key";
const INITIAL_VALUE: u64 = 123;
const UPDATE_VALUE: u64 = 321;

fn main() -> Result<(), Error> {
    let file = tempfile::NamedTempFile::new().unwrap();
    let db = Database::create(file.path())?;

    // set initial data
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(TABLE)?;
        table.insert(KEY, &INITIAL_VALUE)?;
    }
    write_txn.commit()?;

    // set up a test to see what external read transasctions see while a write tx is in progress
    // the barrier lets us resume computation once the update has happened, but before the tx is committed
    let pre_read_barrier = Barrier::new(2);
    let post_commmit_barrier = Barrier::new(2);
    thread::scope(|s| {
        s.spawn(|| {
            // read txn: should see initial value even after update
            pre_read_barrier.wait();
            let read_txn = db.begin_read().expect("can set up read txn");
            let table = read_txn
                .open_table(TABLE)
                .expect("can get read-only view of table");
            match table
                .get(KEY)
                .expect("can read table")
                .expect("key was already set")
                .value()
            {
                INITIAL_VALUE => println!("concurrent read txn correctly sees initial value"),
                UPDATE_VALUE => println!("concurrent read txn INCORRECTLY sees updated value"),
                other => println!("concurrent read txn INCORRECTLY sees UNEXPECTED value: {other}"),
            };

            // after commit, value should update
            post_commmit_barrier.wait();
            match table
                .get(KEY)
                .expect("can read table")
                .expect("key was already set")
                .value()
            {
                INITIAL_VALUE => {
                    println!("post-commit existing read txn INCORRECTLY sees initial value")
                }
                UPDATE_VALUE => {
                    println!("post-commit existing read txn correctly sees updated value")
                }
                other => {
                    println!(
                        "post-commit existing read txn INCORRECTLY sees UNEXPECTED value: {other}"
                    )
                }
            };
        });
        s.spawn(|| {
            // write txn: should see updated value while tx is in progress and after commit

            // start by updating the kv
            let write_txn = db.begin_write().expect("can set up write txn");
            {
                let mut table = write_txn
                    .open_table(TABLE)
                    .expect("can get writeable view of table");
                table.insert(KEY, &UPDATE_VALUE).expect("can update table");

                // check what the write txn sees before committing
                pre_read_barrier.wait();
                match table
                    .get(KEY)
                    .expect("can read table")
                    .expect("key exists")
                    .value()
                {
                    INITIAL_VALUE => {
                        println!("pre-commit write txn sees old data before commit, unfortunately");
                    }
                    UPDATE_VALUE => println!("pre-commit write txn sees updated data, hooray!"),
                    other => {
                        println!("pre-commit write txn INCORRECTLY sees UNEXPECTED value: {other}")
                    }
                };
            }
            write_txn.commit().expect("can commit write txn");
            post_commmit_barrier.wait();

            let read_txn = db.begin_read().expect("can set up read txn");
            let table = read_txn
                .open_table(TABLE)
                .expect("can get new read-only view of table");
            match table
                .get(KEY)
                .expect("can read table")
                .expect("key exists")
                .value()
            {
                INITIAL_VALUE => {
                    println!("post-commit new read txn INCORRECTLY sees initial value")
                }
                UPDATE_VALUE => println!("post-commit new read txn correctly sees updated value"),
                other => {
                    println!("post-commit new read txn INCORRECTLY sees UNEXPECTED value: {other}")
                }
            }
        });
    });

    Ok(())
}
