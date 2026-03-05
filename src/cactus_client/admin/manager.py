import apluggy
from . import project_name
from . import hookspecs
from . import default_impl

pm = apluggy.PluginManager(project_name)
pm.add_hookspecs(hookspecs.Spec)
pm.register(default_impl.DefaultPlugin())


def get_plugin_manager() -> apluggy.PluginManager:
    return pm
