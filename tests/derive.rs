use amadeus::prelude::*;

#[derive(Data, Clone, PartialEq, Debug)]
struct GenericRow<G> {
	t: G,
}

mod no_prelude {
	#![no_implicit_prelude]

	#[derive(::amadeus::prelude::Data, Clone, PartialEq, Debug)]
	struct GenericRow<G> {
		t: G,
	}
}
