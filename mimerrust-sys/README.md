# Mimer SQL C API Bindings

The `mimerrust-sys` crate is responsible for the low-level wrapping of the Mimer SQL C API. It's not meant to be used directly. Instead use the [mimerrust](https://crates.io/crates/mimerrust) crate. 
To reduce build time and avoid requirements on LLVM and Clang on Windows a pre-generated binding is used by default. To generate and use a new binding, pass the `--features run_bindgen` when building.


## Resources
- [Documentation](https://docs.rs/mimerrust/latest/mimerrust/)
- [Mimer Information Technology](https://www.mimer.com)
- [Mimer SQL Developer site](https://developer.mimer.com)

## Credits
The following persons have contributed to the initial version of Mimer SQL Rust API:
- Edvard Axelman
- Edvin Bruce
- Simin Eriksson
- William Forslund
- Fredrik Hammarberg
- Viktor Wallsten

