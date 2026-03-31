# add-accessions

`add-accessions` is a script for adding (dbSNP) IDs to a whitespace separated
table of variants, based on table of IDs indexed using keys in the form
`chrom:pos:ref:alt`.

## Examples

For a set of keys/IDs:

```console
$ cat my-keys.txt
1:10019:TA:T rs775809821
1:10039:A:C rs978760828
1:10043:T:A rs1008829651
```

Create an index/database file:

```console
python3 add-accessions.py --action index my-keys.database my-keys.txt
```

Add variants to a table of variants:

```console
$ cat my-variants.txt
CHROM  POS    REF  ALT
1      10039  A    C
$ python3 add-accessions.py --action lookup my-keys.database my-variants.txt
CHROM  POS    REF  ALT rsID
1      10039  A    C   rs978760828
Found IDs for 1 of 1 records (100.0%), 0 not found
```

## Usage

```text
usage: add-accessions.py [-h] [--action X] [--key-column KEY_COLUMN]
       [--missing-value X] [--no-header] [--unordered-alleles] database source

positional arguments:
  database              Path to SQLite3 database
  source                Input file

options:
  -h, --help            show this help message and exit
  --action X            Either 'index' a containing keys and IDs, or 'lookup'
                        positions and add IDs to a whitespace separated table
                        (default: lookup)
  --key-column KEY_COLUMN
                        Column number (1-based) or name containing allele keys
                        in the form chr:pos:alt:ref (may use '_' as the
                        separator). If not set,
                        the script will look for columns 'CHROM', 'POS', 'REF',
                        and 'ALT', or columns 'MarkerName' (containing
                        chr:pos), 'Allele1',
                        'Allele2'. Column names are case-insensitive. For
                        lookup only (default: None)
  --missing-value X     Value used when no IDs were found. For lookup only
                        (default: NA)
  --no-header           If set, the columns are assumed to not have names.
                        --key-column must be set to a number. For lookup only
                        (default: False)
  --unordered-alleles   When enabled, this script will look up IDs for alleles
                        chrom:pos:A:B and chrom:pos:B:A, i.e. making no
                        assumption about which allele is the reference allele
                        and which is the alternative allele. (default: False)
```
