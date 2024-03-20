use std::{
    fs::File,
    io::Result,
    path::Path,
};

use external_storage::{
    create_storage, make_local_backend, make_noop_backend,
    ExternalStorage,
};
use futures::executor::block_on;
use futures_util::io::{copy, AllowStdIo};
use structopt::clap::arg_enum;
use structopt::StructOpt;

arg_enum! {
    #[derive(Debug)]
    enum StorageType {
        Noop,
        Local,
    }
}

#[derive(StructOpt)]
#[structopt(rename_all = "kebab-case", name = "scli", version = "0.1")]
/// An example using storage to save and load a file.
pub struct Opt {
    /// Storage backend.
    #[structopt(short, long, possible_values = &StorageType::variants(), case_insensitive = true)]
    storage: StorageType,
    /// Local file to load from or save to.
    #[structopt(short, long)]
    file: String,
    /// Remote name of the file to load from or save to.
    #[structopt(short, long)]
    name: String,
    /// Path to use for local storage.
    #[structopt(short, long)]
    path: String,
    #[structopt(subcommand)]
    command: Command,
}

#[derive(StructOpt)]
#[structopt(rename_all = "kebab-case")]
enum Command {
    /// Save file to storage.
    Save,
    /// Load file from storage.
    Load,
}

fn process() -> Result<()> {
    let opt = Opt::from_args();
    let storage = match opt.storage {
        StorageType::Noop => create_storage(&make_noop_backend())?,
        StorageType::Local => create_storage(&make_local_backend(Path::new(&opt.path)))?,
    };

    match opt.command {
        Command::Save => {
            let file = File::open(&opt.file)?;
            let file_size = file.metadata()?.len();
            storage.write(&opt.name, Box::new(AllowStdIo::new(file)), file_size)?;
        }
        Command::Load => {
            let reader = storage.read(&opt.name);
            let mut file = AllowStdIo::new(File::create(&opt.file)?);
            block_on(copy(reader, &mut file))?;
        }
    }

    Ok(())
}

fn main() {
    match process() {
        Ok(()) => {
            println!("done");
        }
        Err(e) => {
            println!("error: {}", e);
        }
    }
}
