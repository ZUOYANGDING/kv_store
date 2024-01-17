use std::process;

use clap::{Arg, Command};

fn main() {
    let command = Command::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .disable_help_subcommand(true)
        .subcommand_required(true)
        .subcommand(
            Command::new("set")
                .about("Set the value of a string key to a string")
                .arg(Arg::new("KEY").help("A string key").required(true))
                .arg(
                    Arg::new("VALUE")
                        .help("The value of the string key")
                        .required(true),
                ),
        )
        .subcommand(
            Command::new("get")
                .about("Get the string value of a given string key")
                .arg(Arg::new("KEY").help("A string key").required(true)),
        )
        .subcommand(
            Command::new("rm")
                .about("Remove a given key")
                .arg(Arg::new("KEY").help("A string key").required(true)),
        )
        .get_matches();
    match command.subcommand() {
        Some(("set", args)) => {
            println!(
                "KEY{:?}, VALUE{:?}",
                args.get_one::<String>("KEY").unwrap(),
                args.get_one::<String>("VALUE").unwrap()
            );
            eprintln!("unimplemented");
            process::exit(-1);
        }
        Some(("get", args)) => {
            println!("KEY{:?}", args.get_one::<String>("KEY").unwrap(),);
            eprintln!("unimplemented");
            process::exit(-1);
        }
        Some(("rm", args)) => {
            println!("KEY{:?}", args.get_one::<String>("KEY").unwrap(),);
            eprintln!("unimplemented");
            process::exit(-1);
        }
        _ => process::exit(-1),
    }
}
