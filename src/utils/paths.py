from pathlib import Path


def get_project_root() -> Path:
    """Get the absolute path to the project root directory"""
    return Path(__file__).parent.parent.parent


def get_logs_dir() -> Path:
    """Get the absolute path to the logs directory"""
    logs_dir = get_project_root() / 'logs'
    logs_dir.mkdir(parents=True, exist_ok=True)
    return logs_dir


def get_log_file_path() -> Path:
    """Get the absolute path to the log file"""
    return get_logs_dir() / 'tradeguard_health_log'
