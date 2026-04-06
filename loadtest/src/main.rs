mod cluster;
mod engine;
mod network;
mod query;
mod report;

#[tokio::main]
async fn main() {
    println!("═══════════════════════════════════════════════════════");
    println!("  MidDB Load Test Suite");
    println!("═══════════════════════════════════════════════════════\n");

    println!("▶ Storage Engine Tests");
    println!("───────────────────────────────────────────────────────");
    engine::run_all();

    println!("\n▶ Query Engine Tests");
    println!("───────────────────────────────────────────────────────");
    query::run_all();

    println!("\n▶ Network Tests");
    println!("───────────────────────────────────────────────────────");
    network::run_all().await;

    println!("\n▶ Cluster Tests (3-node, RF=3, W=2, R=1)");
    println!("───────────────────────────────────────────────────────");
    cluster::run_all().await;

    println!("\n═══════════════════════════════════════════════════════");
    println!("  Load tests complete");
    println!("═══════════════════════════════════════════════════════");
}
