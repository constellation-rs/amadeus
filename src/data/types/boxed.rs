//! Implement [`Record`] for `Box<T> where T: Record`.

use std::collections::HashMap;

use amadeus_parquet::{
    basic::Repetition,
    column::reader::ColumnReader,
    errors::Result,
    record::{reader::BoxReader, schemas::BoxSchema, Record},
    schema::types::{ColumnPath, Type},
};

// Enables Rust types to be transparently boxed, for example to avoid overflowing the
// stack. This is marked as `default` so that `Box<[u8; N]>` can be specialized.
default impl<T: ?Sized> Record for Box<T>
where
    T: Record,
{
    type Reader = BoxReader<T::Reader>;
    type Schema = BoxSchema<T::Schema>;

    fn parse(
        schema: &Type,
        repetition: Option<Repetition>,
    ) -> Result<(String, Self::Schema)> {
        T::parse(schema, repetition)
            .map(|(name, schema)| (name, unsafe { known_type(BoxSchema(schema)) }))
    }

    fn reader(
        schema: &Self::Schema,
        path: &mut Vec<String>,
        def_level: i16,
        rep_level: i16,
        paths: &mut HashMap<ColumnPath, ColumnReader>,
        batch_size: usize,
    ) -> Self::Reader {
        let schema =
            unsafe { known_type::<&Self::Schema, &BoxSchema<T::Schema>>(schema) };
        let ret = BoxReader(T::reader(
            &schema.0, path, def_level, rep_level, paths, batch_size,
        ));
        unsafe { known_type(ret) }
    }
}

/// This is used until specialization can handle groups of items together
unsafe fn known_type<A, B>(a: A) -> B {
    use std::mem;
    assert_eq!(
        (mem::size_of::<A>(), mem::align_of::<A>()),
        (mem::size_of::<B>(), mem::align_of::<B>())
    );
    let ret = mem::transmute_copy(&a);
    mem::forget(a);
    ret
}
