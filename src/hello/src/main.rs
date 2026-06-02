use clap::Parser;

/// A minimal CLI that exercises the third-party `clap` dependency, imported by
/// reindeer into //third-party. Run with `--help` to see the generated usage.
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
