Reproduce:

```
nix run nixpkgs#python3 -- -m venv .venv --copies
pip install -r requirements.txt
python -m test.test_file_based_datasource
```
