// build.rs
use std::env;
use std::fs::File;
use std::io::Write;
use std::path::Path;

fn main() {
    // Tell Cargo to rerun this build script if the DUMMY_CODE_SIZE environment variable changes.
    println!("cargo:rerun-if-env-changed=DUMMY_CODE_SIZE");

    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("wasm_bloat.rs");
    let mut f = File::create(&dest_path).unwrap();

    let dummy_code_size: usize = env::var("DUMMY_CODE_SIZE")
        .unwrap_or_default()
        .parse()
        .unwrap_or(0);

    if dummy_code_size == 0 {
        writeln!(f, "pub fn touch_bloat_code(val: u64) -> u64 {{ val }}").unwrap();
        return;
    }

    writeln!(f, "use std::hint::black_box;\n").unwrap();

    // Every 100 functions is 0,5 MB of code
    let number_of_functions = 200 * dummy_code_size;
    let number_of_operations_per_function = 110;

    // Generate distinct functions with unique constant offsets
    for i in 0..number_of_functions {
        writeln!(f, "#[inline(never)]").unwrap();
        writeln!(f, "pub fn pad_code_section_{}(mut x: u64) -> u64 {{", i).unwrap();

        // Generate unique arithmetic operations per function to prevent LLVM folding
        for j in 0..number_of_operations_per_function {
            let seed = (i * 1000 + j + 1) as u64;
            writeln!(f, "    x = black_box(x.wrapping_add({}));", seed).unwrap();
            writeln!(f, "    x = black_box(x ^ {});", seed.rotate_left(13)).unwrap();
        }
        writeln!(f, "    x").unwrap();
        writeln!(f, "}}\n").unwrap();
    }

    // Generate a caller function so dead code elimination doesn't strip them
    writeln!(f, "pub fn touch_bloat_code(mut val: u64) -> u64 {{").unwrap();
    for i in 0..number_of_functions {
        writeln!(f, "    val = pad_code_section_{}(val);", i).unwrap();
    }
    writeln!(f, "    val").unwrap();
    writeln!(f, "}}").unwrap();
}
