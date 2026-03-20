# tabulate-files

This script generates a tab-separated table of files and folders at a location, to allow comparison between two locations on different systems.

Note that some characters (`\n`, `\r`, `\`) are escaped, to prevent filenames from breaking the output.

## Example usage

```console
$ python3 tabulate-files.py /etc/ | column -t -s$'\t'
Mode    User    Group   Size  MTimeNS              Path                Link
40755   colord  colord  0     1733752633930304362  /etc/colord
100644  root    root    2398  1773870610337019331  /etc/fstab
40755   root    root    114   1733757855778866315  /etc/zsh
100644  root    root    3900  1708723868000000000  /etc/zsh/zshrc
120777  root    root    16    1733754493181100901  /etc/vconsole.conf  default/keyboard
...
```
