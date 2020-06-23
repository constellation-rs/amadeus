use amadeus::prelude::*;

#[derive(Data, Clone, PartialEq, Debug)]
struct GenericRow<G> {
	t: G,
}
