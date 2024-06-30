from .managing import build_manager
from .settings import Settings


if __name__ == '__main__':
    settings = Settings()
    manager = build_manager(settings)
    manager.run()
