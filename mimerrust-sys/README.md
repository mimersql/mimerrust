# Mimer SQL C API Bindings

The `mimerrust-sys` crate handles low-level wrapping of the C library into Rust-compatible concepts. 
It is not intended for direct use, but rather as an intermediary wrapping step. Instead use the [mimerrust](https://crates.io/crates/mimerrust) crate. To reduce build time and avoid requiring LLVM and Clang on Windows, a pre-generated binding is used by default. To generate and use a new binding, pass the `--features run_bindgen` flag when building.

## Resources
- [Documentation](https://docs.rs/mimerrust/latest/mimerrust/)
- [Mimer Information Technology](https://www.mimer.com)
- [Mimer SQL Developer site](https://developer.mimer.com)

## Credits
The following persons have contributed to the initial version of Mimer SQL Rust API:
- [Edvard Axelman](https://github.com/popfumo)
- [Edvin Bruce](https://github.com/Bruce1887)
- [Simon Eriksson](https://github.com/sier8161)
- [William Forslund](https://github.com/Forslund16)
- [Fredrik Hammarberg](https://github.com/efreham1)
- [Viktor Wallsten](https://github.com/viwa3399)
