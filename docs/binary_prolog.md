# Binary Prolog (Version 1.0)

PrologDB uses a binary representation of prolog for storage and transport
over the network. These benefits are to be gained:

* parsing one-pass with few branches (for speed)

Both for networking and storage MSB first endianess is used.

## Versioning

This format is versioned; newer versions of PrologDB may use extended versions
of this format.

### Scheme

The number is formatted like so: `Major.Minor`

When a change is made to the format, the version number MUST be changed:

* If binary prolog of the previous version is semantically equal under the changed
  version: increment the minor version by 1.
* Else: increment the major version by 1 and set the minor version to 0

## Integer encoding

Integers needed for meta-information (e.g. predicate arity, list length), this
encoding is used. Its very similar to UTF-8:

Every integer has at least one byte. The 7 least significant bits of that byte
count towards the number; the most significant bit defines whether more bytes
must be considered for the number: bit set = this is the last byte of the number,
bit not set = consider the following byte.

The bits that count towards the number occur in big endian encoding: The 7 bits
from the first byte are the most significant, the 7 bits from the last byte are
the least significant. The more significant a bit is in the byte it was taken
from, the more significant it is for the final number.

For numbers that do not take a multiple of 7 bits, 0 are padded at the left
(in the first byte).

For example, 59<sub>10</sub> = 111011<sub>2</sub> encoded looks like this:

    Byte 0
    MSB                LSB
    1  0  1  1  1  0  1  1
    
And this is 287<sub>10</sub> = 100011111<sub>2</sub> encoded:

    Byte 0                 | Byte 1
    MSB                LSB | MSB                LSB
    0  0  0  0  0  0  1  0 | 1  0  0  1  1  1  1  1

## Term encoding

The first byte of each term defines its type. Everything that follows is specific
to that term type.

|First byte value | Term Type         |
|-----------------|-------------------|
|            0x10 | integer           |
|            0x11 | decimal           |
|            0x12 | *reserved*        |
|            0x20 | variable          |
|            0x22 | atom              |
|            0x23 | *reserved*        |
|            0x24 | string            |
|            0x25 | *reserved*        |
|            0x26 | *reserved*        |
|            0x27 | *reserved*        |
|            0x30 | predicate         |
|            0x31 | list with tail    |
|            0x32 | list without tail |
|            0x40 | dict with tail    |
|            0x41 | dict without tail |

### Integers

Following the type bit is the size of the number encoded in bytes encoded with
the integer encoding. Currently, PrologDB supports only integers up to 8 bytes
in size but future versions might include arbitrary integer math code and thus
support arbitrarily large integers.

Example: `975692`: `0x10 0x03 0x0E 0xE3 0x4C`  
975692<sub>10</sub> = EE34C<sub>16</sub>

Note that these are also valid binary representations for the same number:

* `0x10 0x04 0x00 0x0E 0xE3 0x4C`
* `0x10 0x05 0x00 0x00 0x0E 0xE3 0x4C`
* `0x10 0x08 0x00 0x00 0x00 0x00 0x00 0x0E 0xE3 0x4C`

### Decimals

Following the type byte is the number of bits that make up the decimal encoded
with the integer encoding. Decimals are encoded using the IEEE-754 format.

The number of bytes required for a number is always the least in order to
fit the IEEE-754 encoded bits.

Currently, PrologDB uses only 64bit decimals. It can also read 32bit decimals
but those will be instantly converted to 64 bit for internal use.

**Examples:**

`3.1415928` as a 32bit decimal: `0x11 0x20 0x40 0x49 0x0F 0xDB`
3.1415928 as IEEE-754 32bit float: `0x40490FDB`

`0.00000000000000016` as a 64bit decimal: `0x11 0x40 0x3C 0xA7 0x0E 0xF5 0x46 0x46 0xD4 0x97`  
0.00000000000000016 as IEEE-754 64bit float: `0x3CA70EF54646D497`

### Variables, Atoms and Strings

Variables, atoms and strings are stored in UTF-8 format. Following the type
byte is the number of bytes the name/content of the term occupies in UTF-8
encoding in the integer encoding.

**Examples:**

| Term             | Binary                                                    |
|------------------|-----------------------------------------------------------|
| `Avariable`      | `0x20 0x09 0x41 0x76 0x61 0x72 0x69 0x61 0x62 0x6C 0x65`  |
| `atom`           | `0x22 0x04 0x61 0x74 0x6F 0x6D`                           |
| `"String"`       | `0x24 0x06 0x53 0x74 0x72 0x69 0x6E 0x67`                 |
| `"➩🙊"`          | `0x24 0x07 0xE2 0x9E 0xA9 0xF0 0x9F 0x99 0x8A`            |

### Predicates

Layout:

| Order # | Element                                                        |
|---------|----------------------------------------------------------------|
| 1       | Type byte                                                      |
| 2       | Arity encoded with the integer encoding                        |
| 3       | Predicate name encoded as an atom without the type byte (s.a.) |
| 4...    | The arguments to the predicate in order                        |
 
**Examples:**

| Term                  | Binary                                            |
|-----------------------|---------------------------------------------------|
|`a(x)`                 | `0x30 0x01 0x01 0x61 0x22 0x01 0x78`              |
|`foo(1, "bar", z)`     | `0x30 0x03 0x03 0x66 0x6F 0x6F 0x10 0x01 0x01 0x24 0x03 0x62 0x61 0x72 0x22 0x01 0x7A` |

### Lists with tail

In the Prolog dialect used by PrologDB, list tails can only ever be variables.

Layout:

| Order # | Element                                                          |
|---------|------------------------------------------------------------------|
| 1       | Type byte                                                        |
| 2       | Tail encoded as a variable without the type byte (s.a.)          |
| 3       | Number of elements in the list encoded with the integer encoding |
| 4...    | The list elements in order                                       |

**Examples:**

| Term                        | Binary                                       |
|-----------------------------|----------------------------------------------|
|<code>[a, 2 &#124; T]</code> | `0x31 0x01 0x54 0x02 0x22 0x01 0x61 0x10 0x01 0x02` |

### Lists without tail

Layout:

| Order # | Element                                                          |
|---------|------------------------------------------------------------------|
| 1       | Type byte                                                        |
| 2       | Number of elements in the list encoded with the integer encoding |
| 3...    | The list elements in order                                       |

**Examples:**

| Term                 | Binary                                              |
|----------------------|-----------------------------------------------------|
|`[a, 2]`              | `0x32 0x02 0x22 0x01 0x61 0x10 0x01 0x02`           |

### Dictionaries

**Layout of the entries (key-value-pairs):**

| Order # | Element                                                          |
|---------|------------------------------------------------------------------|
| 1       | The key encoded as an atom without the type byte                 |
| 2       | The value, encoded using the binary encoding                     |

**Layout of dicts with tail:**

| Order # | Element                                                          |
|---------|------------------------------------------------------------------|
| 1       | Type byte                                                        |
| 2       | Tail encoded as a variable without the type byte (s.a.)          |
| 3       | Number of keys in the dict encoded with the integer encoding     |
| 4...    | The dict entries in order                                        |

*In the Prolog dialect used by PrologDB, dict tails can only ever be variables.*

**Layout of dicts without tail:**

| Order # | Element                                                          |
|---------|------------------------------------------------------------------|
| 1       | Type byte                                                        |
| 2       | Number of keys in the dict encoded with the integer encoding     |
| 3...    | The dict entries in order                                        |

**Examples:**

| Term                      | Binary                                         |
|---------------------------|------------------------------------------------|
|`{f:"b", x:2}`             |`0x41 0x02 0x01 0x66 0x24 0x01 0x62 0x01 0x78 0x10 0x01 0x02`|
|<code>{a:b &#124; X}</code>|`0x40 0x01 0x58 0x01 0x01 0x61 0x22 0x01 0x62`  |