use std::env;
use std::path::PathBuf;

// libIEC61850 sudah di-install di image dev (docker/Dockerfile.dev) ke /usr/local.
const INCLUDE_DIR: &str = "/usr/local/include/libiec61850";

fn main() {
    println!("cargo:rustc-link-search=native=/usr/local/lib");
    // Library statis: iec61850 bergantung pada hal (socket, thread, time)
    println!("cargo:rustc-link-lib=static=iec61850");
    println!("cargo:rustc-link-lib=static=hal");
    println!("cargo:rustc-link-lib=pthread");
    println!("cargo:rustc-link-lib=m");
    println!("cargo:rustc-link-lib=rt");
    println!("cargo:rerun-if-changed=wrapper.h");

    let bindings = bindgen::Builder::default()
        .header("wrapper.h")
        .clang_arg(format!("-I{INCLUDE_DIR}"))
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        // Konstanta enum pakai nama aslinya dari C, mis. IED_ERROR_OK, IEC61850_FC_MX
        .prepend_enum_name(false)
        .allowlist_function("IedConnection_.*|IedClientError_.*|MmsValue_.*|LinkedList_.*")
        .wrap_unsafe_ops(true)
        .generate()
        .expect("gagal generate binding libIEC61850");

    let out = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"));
    bindings
        .write_to_file(out.join("bindings.rs"))
        .expect("gagal menulis bindings.rs");
}
