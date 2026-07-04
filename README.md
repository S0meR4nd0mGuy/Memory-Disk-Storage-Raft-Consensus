# Memory-Disk-Storage-Raft-Consensus
A In-Memory and Disk Storage with Raft consensus, hosted through localhost normally, if ran through main.py. Also completely local if run through client.

## How to run:
**For LocalHost on Network**
```bash
python -m src.main
```

**For completely local**
```bash
python -m src.client.cli
```

**Before running, install requirements.txt**
```bash
python -m pip install -r requirements.txt
```

## Special Dependencies:
**This package also requires advanced_logging, which is a custom module by me. For that refer to [The Repository](https://github.com/S0meR4nd0mGuy/Custom-CLiParse-and-Logging), you only need though the advanced logging module. If you don't want to use it, then change the code for yourself so it uses the stdlib logging module.**