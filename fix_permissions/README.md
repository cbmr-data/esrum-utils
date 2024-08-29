# Fix permissions for shared data

This script fixes/cleans up permissions for data that is intended to be shared with other users.

More specifically, by default the script

 - Updates group ownership of files/folders/links to that specified on the command-line
 - Sets the group-bit (S_ISGID) on folders, so that new files inherit their group from the parent folder
 - Ensures that only the owner has write access
 - Ensures that only the owner and group has read access

Note that symlinks are not followed.

## Requirements

This script can optionally use the `tqdm` python module to print progress, but it is not required.

## Usage

	$ fix_permissions.py comp-prj-denmark-audit ./my_data --commit
	chmod ./my_data to 0o2750 since mode is 0o755
	chmod ./my_data/README.txt to 0o640 since mode is 0o644
	chmod ./my_data/data to 0o2750 since mode is 0o755
	[...]

Without the `--commit` option, the script only prints the changes that will be applied