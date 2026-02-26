import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

config_stub = types.SimpleNamespace(
    API_URL="http://example.com/health",
    WINDOW_SIZE=3,
)

sys.modules.setdefault("config", config_stub)
