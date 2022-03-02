/*
 *  Batch CSV payment system. It processes a CSV file and generates the balance per client
 * 
 *  Author:    Alberto Fernandez
 *  Date:      13/02/2021
 *  Version:   0.9
 */


use std::env;
use std::fs::File;
use std::io;
use std::process;
use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};
use csv::{Trim};


#[derive(Serialize)]

#[derive(Debug, Clone, Deserialize)]
struct Transaction {
    // Types can be; deposit, withdrawal, dispute, resolve, chargeback
    #[serde(rename = "type")]
    type_name:     String,
    #[serde(rename = "client")]
    client_id:     u16,
    #[serde(rename = "tx")]
    tx_id:         u32,
    amount:        f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ClientAccount {
    #[serde(rename = "client")]
    client_id:     u16,
    available:     f32,
    held:          f32,
    total:         f32,
    locked:        bool,
}

impl ClientAccount {
    pub fn new(in_client_id: u16) -> Self {
        ClientAccount {
            client_id:  in_client_id,
            available:  0.0,
            held:       0.0,
            total:      0.0,
            locked:     false,
        }
    }
}

// ---------------------------------------------------------------------

fn usage() {
    println!("Batch CSV Payment");
    println!("Usage:     csv_payment   input_transactions.csv");
    println!("");
    println!("   input_transactions.csv - CSV file containing the list of transactions");
    println!("                            Columns: type (string), client id (unsigned), transaction id(unsigned), amount (float)");
    println!("");
}

/**
 * Search a client. If it does not exist, it will add it to the list and return it
 */
fn get_add_client(in_id: u16, in_client_list: &mut HashMap<u16, ClientAccount>) -> Result<ClientAccount, String> {
    if in_client_list.contains_key(&in_id) == false {
        // Client does not exist. Create it
        let new_client = ClientAccount::new(in_id);
        in_client_list.insert( in_id, new_client.clone() );

        Ok(new_client)
    } else {
        match in_client_list.get_mut(&in_id) {
            Some(c) => Ok( c.clone() ),
            None =>    Err( format!("Unable to find client: {} ", in_id) ),
        }
    }
}

/**
 * Add the transaction to the list. Check if it does not exist
 */ 
fn add_transaction(in_current_tx: &Transaction, in_transaction_list: &mut HashMap<u32, Transaction>) -> Result<i32, String> {
    if in_transaction_list.contains_key(&in_current_tx.tx_id) == true {
       return Err( format!("ERROR: Transactin already exist: {} ", in_current_tx.tx_id) );
    }
    
    in_transaction_list.insert(in_current_tx.tx_id, in_current_tx.clone());
    Ok(0)
}

/**
 * Process a transaction and update clientś account
 * 
 */
fn process_transaction(in_current_tx: &Transaction, in_client_list: &mut HashMap<u16, ClientAccount>, in_transaction_list: &mut HashMap<u32, Transaction>) -> Result<i32, String> {

    match in_current_tx.type_name.as_str() {
        // -------------------------------------
        "deposit" => {
            // Search for client
            let mut the_client : ClientAccount;
            match get_add_client(in_current_tx.client_id, in_client_list) {
                Ok(c)  => the_client = c,
                Err(e) => { return Err(e); },
            };

            // Increase available and total funds of client
            the_client.available += in_current_tx.amount;
            the_client.total     += in_current_tx.amount;

            // Update the client
            if let Some(c) = in_client_list.get_mut(&in_current_tx.client_id) {
                *c = the_client;
            }

            // Add the Transaction
            if let Err(e) = add_transaction(in_current_tx, in_transaction_list) {
                return Err(e);
            }
        },

        // -------------------------------------
        "withdrawal" => {
            // Search for client
            let mut the_client : ClientAccount;
            match get_add_client(in_current_tx.client_id, in_client_list) {
                Ok(c)  => the_client = c,
                Err(e) => { return Err(e); },
            };

            if the_client.available > in_current_tx.amount {
                // Decrease available and total funds of client
                the_client.available -= in_current_tx.amount;
                the_client.total     -= in_current_tx.amount;
    
                // Update the client
                if let Some(c) = in_client_list.get_mut(&in_current_tx.client_id) {
                    *c = the_client;
                }
            } else {
                return Err( format!("ERROR: Client: {} has insufficient funds: {}", in_current_tx.client_id, the_client.available) );
            }

            // Add the Transaction
            if let Err(e) = add_transaction(in_current_tx, in_transaction_list) {
                return Err(e);
            }
        },

        // -------------------------------------
        "dispute" => {
            // Search for client
            let mut the_client : ClientAccount;
            match get_add_client(in_current_tx.client_id, in_client_list) {
                Ok(c)  => the_client = c,
                Err(e) => { return Err(e); },
            };

            // Get the previous transaction
            let previous_tx = in_transaction_list.get(&in_current_tx.tx_id);
            if let Some(p) = previous_tx {
                // Decrease client available fnds and increase held funds
                the_client.available -= p.amount;
                the_client.held      += p.amount;

                // Update the client
                if let Some(c) = in_client_list.get_mut(&in_current_tx.client_id) {
                    *c = the_client;
                }

                // Add the Transaction
                if let Err(e) = add_transaction(in_current_tx, in_transaction_list) {
                    return Err(e);
                }
            }

            // If previous transaction does not exist, it will be ignored
        },

        // -------------------------------------
        "resolve" => {
            // Search for client
            let mut the_client : ClientAccount;
            match get_add_client(in_current_tx.client_id, in_client_list) {
                Ok(c)  => the_client = c,
                Err(e) => { return Err(e); },
            };

            // Get the previous transaction
            let previous_tx = in_transaction_list.get(&in_current_tx.tx_id);
            if let Some(p) = previous_tx {
                // Check if prevous transaction was 'dispute'
                if p.type_name == "dispute" {
                    // Decrease client held funds and increase the available funds
                    the_client.available += p.amount;
                    the_client.held      -= p.amount;
    
                    // Update the client
                    if let Some(c) = in_client_list.get_mut(&in_current_tx.client_id) {
                        *c = the_client;
                    }
    
                    // Add the Transaction
                    if let Err(e) = add_transaction(in_current_tx, in_transaction_list) {
                        return Err(e);
                    }
                }
            }

            // If previous transaction does not exist or was not it "dispute", it will be ignored
        },

        // -------------------------------------
        "chargeback" => {
            // Search for client
            let mut the_client : ClientAccount;
            match get_add_client(in_current_tx.client_id, in_client_list) {
                Ok(c)  => the_client = c,
                Err(e) => { return Err(e); },
            };


            // Get the previous transaction
            let previous_tx = in_transaction_list.get(&in_current_tx.tx_id);
            if let Some(p) = previous_tx {
                 // Check if prevous transaction was 'dispute'
                 if p.type_name == "dispute" {
                    // Decrease client held funds and increase the available funds
                    the_client.held      -= p.amount;
                    the_client.total     -= p.amount;
                    // Lock the account
                    the_client.locked     = true;

                    // Update the client
                    if let Some(c) = in_client_list.get_mut(&in_current_tx.client_id) {
                        *c = the_client;
                    }

                    // Add the Transaction
                    if let Err(e) = add_transaction(in_current_tx, in_transaction_list) {
                        return Err(e);
                    }
                }

                // If previous transaction does not exist or was not it "dispute", it will be ignored
            }
        },

        _ => {
            // Error
            return Err( format!("ERROR: Unknown transaction type: {}", in_current_tx.type_name.as_str() ) );
        }
    }

    Ok(0)
}

/**
 * Write the final status of clients' accounts to the screen
 */
fn write_accounts(in_accounts: &HashMap<u16, ClientAccount>) -> Result<(), String> {
    if in_accounts.is_empty() == true {
        // Nothing to be done
        () 
    }

    // Write to screen
    let mut csv_writer = csv::Writer::from_writer( io::stdout() );
    // let mut csv_writer = csv::WriterBuilder::new()
    //                                 .has_headers(true)
    //                                 .from_writer( io::stdout() );
    
    csv_writer.write_record(&["client", "available", "held", "total", "locked"]).unwrap();

    for current_client in in_accounts {

        let tmp_available = format!("{:.4}", current_client.1.available);
        let tmp_held      = format!("{:.4}", current_client.1.held);
        let tmp_total     = format!("{:.4}", current_client.1.total);

        csv_writer.serialize((current_client.1.client_id, 
                              tmp_available, 
                              tmp_held,
                              tmp_total,
                              current_client.1.locked)).unwrap();
    
        // if let Err(e) = csv_writer.serialize( current_client.1 ) {
        //     return Err( e.to_string() );
        // }
    }
    csv_writer.flush().expect("ERROR: Writing data to screen");

    Ok(())        
}

/**
 * @return -  0 - No error
 *           -1 - Error. Insufficient parameters or other errors
 */
fn main() {
    let args: Vec<String> = env::args().collect();
 
    //println!("{:?}", args);

    // Check number arguments
    if args.len() <= 1 {
        usage();
        process::exit(-1);
    }

    // Read input CSV
    let input_csv_file = args[1].clone();

    if Path::new(&input_csv_file).exists() == false {
        println!("ERROR: CSV file does not exist: {}", input_csv_file);
        process::exit(-1);
    }

    let input_file = match File::open(input_csv_file) {
        Ok(f)  => f,
        Err(e)  => {
            println!("{}", e);
            process::exit(-1);
        },
    };

    let mut csv_reader = csv::ReaderBuilder::new()
    //                                 .ascii()
                                     // Remove spaces
                                     .trim(Trim::All)
                                     .from_reader( input_file ) ;   
   
    // Process all transactions and update client accounts
    let mut client_list : HashMap<u16, ClientAccount> = HashMap::new();
    let mut transaction_list : HashMap<u32, Transaction> = HashMap::new();

    for current_record in csv_reader.deserialize() {
        // Extract next transaction
        let current_tx: Transaction = match current_record {
            Ok(r) => {
                r
            },
            Err(e) => {
                println!("ERROR: Reading or decoding transaction: {}", e);
                process::exit(-1);
            },
            
        };
        
        //println!("{:?}", current_tx);
        // Process the transaction type and update client account
        if let Err(e) = process_transaction(&current_tx, &mut client_list, &mut transaction_list) {
            println!("{}", e);
            break;
        }
    }

    // Write output
    if let Err(e) = write_accounts(&client_list) {
        println!("{}", e);
        process::exit(-1);
    }

    // Return sucessfull
    process::exit(0);
}