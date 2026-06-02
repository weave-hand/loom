use clap::Parser;

/// Minimal CLI that exercises the third-party `clap` dependency, imported by
/// reindeer into //third-party.
#[derive(Parser)]
#[command(name = "hello", version)]
struct Args {
    /// Who to greet.
    #[arg(short, long, default_value = "world")]
    name: String,
}

fn main() {
    let args = Args::parse();
    println!("Hello, {}!", args.name);
}
