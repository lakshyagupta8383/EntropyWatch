import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Ensure collectors that do `import config` resolve to source/config.py.
try:
    import source.config as _config
except Exception:
    _config = None

if _config is not None:
    sys.modules.setdefault("config", _config)
