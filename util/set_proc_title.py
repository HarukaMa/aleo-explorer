
# noinspection PyPackageRequirements
def set_proc_title(name: str):
    try:
        import setproctitle
    except ImportError:
        return

    setproctitle.setproctitle(name)