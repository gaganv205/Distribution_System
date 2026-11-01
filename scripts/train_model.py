"""Train the model and save it. Run once before starting workers (Docker does this at build)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from app.config import load_settings  # noqa: E402
from app.model import train_and_save  # noqa: E402

if __name__ == "__main__":
    path = load_settings().model_path
    acc = train_and_save(path)
    print(f"saved model to {path} (test accuracy {acc:.3f})")
