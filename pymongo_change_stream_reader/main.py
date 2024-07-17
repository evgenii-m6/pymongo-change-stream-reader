from pymongo_change_stream_reader.managing import build_manager
from pymongo_change_stream_reader.settings import Settings


if __name__ == '__main__':
    settings = Settings()
    manager = build_manager(settings)
    manager.run()
