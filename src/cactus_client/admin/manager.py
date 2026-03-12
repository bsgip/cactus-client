import apluggy
from . import project_name
from . import hookspecs
from . import default_impl


def get_plugin_manager() -> apluggy.PluginManager:
    """Produces the plugin manager for the project."""
    pm = apluggy.PluginManager(project_name)
    pm.add_hookspecs(hookspecs.Spec)
    pm.register(default_impl.DefaultPlugin())

    # Discover and load plugins via entrypoints
    pm.load_setuptools_entrypoints(project_name)
    pm.check_pending()

    return pm
