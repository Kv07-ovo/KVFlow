"""Point the copied CLI test at KVFlow's own product version and brand."""

import pathlib

root = pathlib.Path(r"C:\Users\90428\Desktop\KVStock-restored\kvflow")
p = root / "tests" / "core" / "test_cli_v1.py"
text = p.read_text(encoding="utf-8")
text = text.replace('assert "1.0.0" in version.stdout', 'assert "kvflow 0.1.0" in version.stdout')
text = text.replace('assert "KVStock Agent OS" in body', 'assert "KVFlow" in body')
p.write_text(text, encoding="utf-8")
print("patched", p)
