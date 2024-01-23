use clap::{Arg, Command};
use on_disk::KVStore;
use on_disk::KVStoreError;
use on_disk::Result;
use std::env;
use std::process;

fn main() -> Result<()> {
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
    let mut db = KVStore::open(env::current_dir()?)?;
    match command.subcommand() {
        Some(("set", args)) => {
            let key = args.get_one::<String>("KEY").unwrap();
            let value = args.get_one::<String>("VALUE").unwrap();
            if let Err(err) = db.set(key.to_owned(), value.to_owned()) {
                println!("{:?}", err);
                process::exit(-1);
            };
        }
        Some(("get", args)) => {
            let key = args.get_one::<String>("KEY").unwrap();
            match db.get(key.to_owned()) {
                Ok(ret) => match ret {
                    Some(value) => println!("{}", value),
                    None => println!("Key not found"),
                },
                Err(err) => println!("{:?}", err),
            }
        }
        Some(("rm", args)) => {
            let key = args.get_one::<String>("KEY").unwrap();
            if let Err(KVStoreError::KeyNotFound) = db.remove(key.to_owned()) {
                println!("Key not found");
                process::exit(-1);
            }
        }
        _ => process::exit(-1),
    }
    Ok(())
}
