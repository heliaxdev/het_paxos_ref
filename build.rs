use std::env;
use std::path::PathBuf;

fn main() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let descriptor_path = out_dir.join("het_paxos_ref_descriptor.bin");
    tonic_build::configure()
        .file_descriptor_set_path(&descriptor_path)
        .compile_well_known_types(true) // necessary for pbjson
        .extern_path(".google.protobuf", "::pbjson_types")
        .compile(&["proto/hetpaxosref.proto", "proto/hetpaxosrefconfig.proto"], &["proto"])
        .unwrap();

    let descriptor_set = std::fs::read(descriptor_path).unwrap();
    pbjson_build::Builder::new()
        .register_descriptors(&descriptor_set).unwrap()
        .build(&[".hetpaxosref", ".hetpaxosrefconfig"]).unwrap();
}
