# Simulator for LDAP group membership changes

To use, create configuration TOML for monitoring script and run

```bash
$ uv run monitor_members monitor simulation.toml --kinit-exe true --ldapsearch-exe ./simulator/ldapsearch.py
```
