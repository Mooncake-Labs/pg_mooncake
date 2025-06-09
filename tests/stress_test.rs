use tokio_postgres::{Config, NoTls, types::ToSql};
use rand::Rng;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::time::{Instant, Duration};
use tokio::task::JoinHandle;
const TARGET_DURATION: Duration = Duration::from_secs(30);
const MIN_ROWS: i32 = 100000; // Minimum rows to be inserted
const MIN_TPS: f64 = 100.0; // Minimum transactions per second


#[tokio::test]
async fn stress_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Configure database connection
    let mut config = Config::new();
    config
        .dbname("pg_mooncake".to_string())
        .user("vscode".to_string())
        .password("".to_string())
        .host("localhost".to_string())
        .port(28817);

    // Connect to the database
    let (client, connection) = config.connect(NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // clean up
    client
        .batch_execute("DROP TABLE IF EXISTS stress_test; 
            DROP EXTENSION IF EXISTS pg_mooncake CASCADE;
            CREATE EXTENSION IF NOT EXISTS pg_mooncake;")
        .await?;

    // Create regular table
    client
        .batch_execute(
            "CREATE TABLE stress_test (
                id INT PRIMARY KEY,
                value TEXT,
                score INT
            )",
        )
        .await?;

    // Create columnstore table
    client
        .execute(
            "CALL mooncake.create_table('stress_test_columnstore', 'stress_test')",
            &[],
        )
        .await?;
    

    // Run stress test
    let num_write_threads = 10;

    // Start concurrent writes
    let write_start_time = Instant::now();
    let mut write_handles = Vec::new();

    for thread_id in 0..num_write_threads {
        let config = config.clone();
        let handle: JoinHandle<Result<(i32, i32), Box<dyn std::error::Error + Send + Sync>>> = tokio::spawn(async move {
            let (client, connection) = config.connect(NoTls).await?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("connection error: {}", e);
                }
            });

            let mut rng = StdRng::from_entropy();
            // Two batch threads
            let batch_chance = match thread_id { 0 => 1.0, 1 => 0.1, _ => 0.0};
            let mut tx_count = 0;
            let mut batch_row_count = 0;
            
            while write_start_time.elapsed() < TARGET_DURATION {
                if rng.gen_bool(batch_chance) {
                    let batch_size = rng.gen_range(100000..500000);
                    let start_id = rng.gen_range(1..9000000);
                    client
                        .batch_execute(&format!(
                            "INSERT INTO stress_test (id, value, score)
                            SELECT 
                                i,
                                'batch_value_' || i::text,
                                (random() * 1000000)::int
                            FROM generate_series({}, {}) i
                            ON CONFLICT (id) DO UPDATE 
                            SET value = EXCLUDED.value,
                                score = EXCLUDED.score;",
                            start_id, start_id + batch_size
                        ))
                        .await?;
                    batch_row_count += batch_size;
                } else {
                    match rng.gen_range(0..3) {
                        0 => {
                            // Insert
                            let new_id = rng.gen_range(100001..200000);
                            client
                                .execute(
                                    "INSERT INTO stress_test (id, value, score) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                                    &[&new_id as &(dyn ToSql + Sync), &format!("new_value_{}", new_id) as &(dyn ToSql + Sync), &rng.gen_range(1..1000000) as &(dyn ToSql + Sync)],
                                )
                                .await?;
                        }
                        1 => {
                            // Update
                            let id = rng.gen_range(1..10000);
                            let new_value = format!("updated_{}", id);
                            client
                                .execute(
                                    "UPDATE stress_test SET value = $1, score = $2 WHERE id = $3",
                                    &[&new_value as &(dyn ToSql + Sync), &rng.gen_range(1..1000000) as &(dyn ToSql + Sync), &id as &(dyn ToSql + Sync)],
                                )
                                .await?;
                        }
                        2 => {
                            // Delete
                            let id = rng.gen_range(1..10000);
                            client
                                .execute(
                                    "DELETE FROM stress_test WHERE id = $1",
                                    &[&id as &(dyn ToSql + Sync)],
                                )
                                .await?;
                        }
                        _ => unreachable!(),
                    }
                    tx_count += 1;
                }
            }
            Ok((batch_row_count,tx_count))
        });
        write_handles.push(handle);
    }

    // Run analytical queries concurrently with writes using the main client
    let queries = vec![
        "SELECT COUNT(*) FROM stress_test_columnstore",
        "SELECT AVG(score) FROM stress_test_columnstore",
        "SELECT value, COUNT(*) FROM stress_test_columnstore GROUP BY value",
        "SELECT id, value, score FROM stress_test_columnstore WHERE score > 500",
        "SELECT value, AVG(score) FROM stress_test_columnstore GROUP BY value HAVING AVG(score) > 750",
    ];

    let read_start_time = Instant::now();
    let mut query_results = Vec::new();

    let mut read_id = 0;
    while read_start_time.elapsed() < TARGET_DURATION {
        let query = queries[rand::thread_rng().gen_range(0..queries.len())];
        let start_time = Instant::now();
        read_id -= 1;
        client.query("insert into stress_test (id, value, score) values ($1, 'test', 100)", &[&read_id as &(dyn ToSql + Sync)]).await?;
        let rows = client.query(query, &[]).await?;
        let duration = start_time.elapsed();
        query_results.push((query.to_string(), duration, rows.len()));
    }
    let read_duration = read_start_time.elapsed();

    let mut total_tx = 0;
    let mut total_rows = 0;
    // Collect results from all write operations
    for handle in write_handles {
        let (row_count, tx_count)= handle.await?.unwrap();
        println!("rows: {} tx: {}", row_count,tx_count);
        total_tx += tx_count;
        total_rows += row_count;
    }
    let write_duration = write_start_time.elapsed();

    let tps = total_tx as f64 / write_duration.as_secs_f64();

    // Print results
    println!("\nStress Test Results:");
    println!("Duration: {:.2?}", write_duration);
    print!("Total Rows Inserted: {}", total_rows);
    println!("Total Transactions: {}", total_tx);
    println!("Transactions per second: {:.2}", tps);
    println!("Read Queries: {}", query_results.len());
    println!("Read Duration: {:.2?}", read_duration);
    // Verify minimum throughput
    assert!(total_rows >= MIN_ROWS, "Total rows inserted {} is below minimum required {}", total_rows, MIN_ROWS);
    assert!(tps >= MIN_TPS, "Throughput {} TPS is below minimum required {} TPS", tps, MIN_TPS);

    Ok(())
} 