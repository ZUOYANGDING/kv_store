use log::error;
use serde::Deserialize;
use serde_json::Deserializer;

use crate::KVStoreEngine;
use crate::Request;
use crate::Response;
use crate::Result;
use std::io::BufReader;
use std::io::BufWriter;
use std::net::TcpStream;
use std::net::{TcpListener, ToSocketAddrs};

pub struct Server<E: KVStoreEngine> {
    pub engine: E,
}

impl<E: KVStoreEngine> Server<E> {
    /// `new` create a server
    pub fn new(engine: E) -> Self {
        Server { engine }
    }

    pub fn start<A: ToSocketAddrs>(mut self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            match (stream) {
                Ok(stream) => {
                    if let Err(err) = self.serve(stream) {
                        error!("Error on serving client: {}", err)
                    }
                }
                Err(err) => error!("Connection failed: {}", err),
            }
        }
        Ok(())
    }

    fn serve(&mut self, stream: TcpStream) -> Result<()> {
        let reader = BufReader::new(&stream);
        let writer = BufWriter::new(&stream);
        let request = Request::deserialize(&mut Deserializer::from_reader(reader))?;

        let response = match request {
            Request::Get { key } => match self.engine.get(key) {
                Ok(value) => Response::Ok(value),
                Err(err) => Response::Err(format!("{}", err)),
            },
            Request::Set { key, value } => match self.engine.set(key, value) {
                Ok(_) => Response::Ok(None),
                Err(err) => Response::Err(format!("{}", err)),
            },
            Request::Remove { key } => match self.engine.remove(key) {
                Ok(_) => Response::Ok(None),
                Err(err) => Response::Err(format!("{}", err)),
            },
        };
        serde_json::to_writer(writer, &response)?;
        Ok(())
    }
}
