use std::env;
use std::path::PathBuf;
#[cfg(target_os = "windows")]
use which::which;

fn main() {

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    #[cfg(feature = "run_bindgen")]
    let mimerapi_inc: String;

    #[cfg(target_os = "macos")]
    {
        println!("cargo:rustc-link-lib=mimerapi");
        #[cfg(feature = "run_bindgen")] {
            mimerapi_inc = "/usr/local/include/mimerapi.h".to_string();
        }
    }

    #[cfg(target_os = "linux")]
    {
        println!("cargo:rustc-link-lib=mimerapi");
        #[cfg(feature = "run_bindgen")] {
            mimerapi_inc = "/usr/include/mimerapi.h".to_string();
        }
    }

    #[cfg(target_os = "windows")]
    {
        let path = which("bsql").expect("BSQL not found in path");
        let dir = path.parent().expect("Could not get Mimer SQL installation dir");
        let mimer_install_dir = dir.to_str().expect("Could not extract Mimer SQL installation dir string").to_string();
        println!("Using Mimer SQL in: {}", mimer_install_dir);
        #[cfg(feature = "run_bindgen")] {
            mimerapi_inc = String::from(format!("{}\\dev\\include\\mimerapi.h", mimer_install_dir));
        }

        #[cfg(target_pointer_width = "64")]
        let lib = "mimapi64";
        #[cfg(target_pointer_width = "32")]
        let lib = "mimapi32";

        println!("cargo:rustc-link-lib={}", lib);        
        
    }
    #[cfg(feature = "run_bindgen")]
    {
    let bindings = bindgen::Builder::default()
        .header(mimerapi_inc)
        // Tell cargo to invalidate the built crate whenever any of the
        // included header files changed.
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");
        

        // Write the bindings to the $OUT_DIR/bindings.rs file.
        bindings
            .write_to_file(out_path.join("bindings.rs"))
            .expect("Couldn't write bindings!");

    }
    
    #[cfg(not(feature = "run_bindgen"))]
    {
        let bindings_file = if cfg!(target_os = "windows") {
            "bindings_win.rs"
          } else if cfg!(target_os = "macos") {
            "bindings_macos.rs"
          } else {
            "bindings.rs"
          };

        println!("Copying pre-generated {}  to {}", bindings_file, out_path.display());
        std::fs::copy(format!("bindings/{}",bindings_file), out_path.join("bindings.rs")).expect("Could not copy pre-generated bindings to output directory");
    }
}