fn main() {
	println!("cargo:rerun-if-changed=build.rs");

	nightly();
}

#[rustversion::nightly]
fn nightly() {
	println!("cargo:rustc-cfg=nightly");
}
#[rustversion::not(nightly)]
fn nightly() {}
