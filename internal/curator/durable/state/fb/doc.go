// Copyright (c) 2019 David Reiss. All rights reserved.
// SPDX-License-Identifier: MIT

package fb

// You must have flatc installed to regenerate these files. Get it here:
// https://google.github.io/flatbuffers/
//go:generate flatc --go -o ../../../../.. state.fbs

/*

The curator stores metadata in BoltDB, encoded with FlatBuffers.

Why FlatBuffers? They encode somewhat larger than another obvious choice,
Protocol Buffers (30% in informal testing), but they have the large advantage
that decoding doesn't require allocating any memory. For reading, this works
very nicely with BoltDB, which returns pointers directly into an mmaped database
file: reads are essentially "zero-copy" and even don't need to parse fields that
aren't being read. Since a lot of the curator's work involves concurrent
read-only scans through the database, this saves a lot of allocation and GC
work.

Besides the larger encoding, another downside is that the API is somewhat
clunky: FlatBuffers' Go support doesn't yet include an "object" API or
easy-to-use builders, so we have to write some additional code to make things
usable. For now, we'll adopt these conventions:

- Each FlatBuffer type should have a corresponding plain Go struct (in
structs.go). The Go struct is named as usual, and the FlatBuffer type is named
with an "F" suffix.

- Each type has a ToStruct method (in unbuilders.go) that returns a new struct
type from the FlatBuffer type.

- Each root type (i.e. type that is the root of an encoded buffer), there's a
Build___ function (in builders.go) that takes the struct and returns an encoded
FlatBuffer as a []byte.

Hopefully, eventually the FlatBuffers compiler will be able to generate most of
that code from the schema. For now, if you add a new type, you'll have to write
a struct and builders and unbuilders manually.


When working with curator database code, keep these guildelines in mind:

- When possible, work with the FlatBuffers objects directly and don't call
ToStruct. This maximizes the benefit of the direct access model. In general,
anything that's just reading should use the FlatBuffers objects.

- Similarly, try to provide a stack-allocated value to struct/table accessors to
prevent unnecessary allocation.

- When creating new values, create them as a struct and call the Build function.

- When modifiying existing values, if the change is "trivial" (i.e. it's
changing a field that is definitely already present to another value), you can
use the "fast path": call CloneBuffer on the root object, use the FlatBuffers
mutators, then write the cloned and modified buffer back to the database.

- If the change isn't trivial (i.e. it can add or removes fields, or changes the
length of a vector), use the "slow path": call ToStruct, change the struct
accordingly, then call Build and write it to the database.

- If performance requires it, you could try the fast path and fall back to the
slow path if the fields to be modified aren't present. Consider whether the
additional code complexity is worth it though.


To keep the size of common values down, we use a few tricks:

- We store TractIDs as five 2-byte values in a "struct".
- We pack three 20-bit TractserverID values in a 64 bit field in TractF.
- We pack several small fields into one 32-bit value in BlobF.
- We store times at one second precision instead of nanosecond.

The code to encapsulate these tricks is in extras.go.

*/
