//! Initial executable scaffold. Network listener is implemented in task A-02.

#![forbid(unsafe_code)]

use std::ffi::OsString;
use std::fs;
use std::path::Path;

/// Exit status reserved for not-yet-implemented server execution.
const EXIT_UNAVAILABLE: i32 = 78;
const EXIT_USAGE: i32 = 2;

fn main() {
    std::process::exit(run(std::env::args_os().skip(1)));
}

fn run(args: impl IntoIterator<Item = OsString>) -> i32 {
    let args: Vec<OsString> = args.into_iter().collect();

    match args.as_slice() {
        [flag] if flag == "--version" => {
            println!(
                "rovqet {} (wire v{}, bootstrap only; no broker listener)",
                env!("CARGO_PKG_VERSION"),
                rovqet_protocol::WIRE_MAJOR_VERSION
            );
            0
        }
        [flag] if flag == "--help" || flag == "-h" => {
            println!(
                "rovqet {}\n\nUsage:\n  rovqet --version\n  rovqet --help\n  rovqet --config <FILE>\n\nStatus: Rust workspace bootstrap. Broker networking is not implemented.",
                env!("CARGO_PKG_VERSION")
            );
            0
        }
        [flag, path] if flag == "--config" => {
            let path = Path::new(path);
            match fs::read(path) {
                Ok(_) => {
                    eprintln!(
                        "rovqet: config file '{}' is readable but not parsed; server not implemented (task A-02). No listener started.",
                        path.display()
                    );
                    EXIT_UNAVAILABLE
                }
                Err(err) => {
                    eprintln!("rovqet: cannot read config '{}': {err}", path.display());
                    EXIT_USAGE
                }
            }
        }
        _ => {
            eprintln!("rovqet: expected --help, --version or --config <FILE>");
            EXIT_USAGE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }

    #[test]
    fn version_and_help_are_available() {
        assert_eq!(run(args(&["--version"])), 0);
        assert_eq!(run(args(&["--help"])), 0);
    }

    #[test]
    fn missing_argument_or_config_is_not_silent_success() {
        assert_eq!(run(args(&[])), EXIT_USAGE);
        assert_eq!(run(args(&["--config"])), EXIT_USAGE);
        assert_eq!(
            run(args(&["--config", "__missing_rovqet_file__.yaml"])),
            EXIT_USAGE
        );
    }

    #[test]
    fn readable_config_does_not_claim_that_broker_started() {
        let existing = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
        assert_eq!(
            run(vec![OsString::from("--config"), existing.into_os_string()]),
            EXIT_UNAVAILABLE
        );
    }
}
