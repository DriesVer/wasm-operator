use tokio;

fn main() {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    rt.block_on(async {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                println!("Hello from block_in_place -> block_on");
            });
        });
    });
}
