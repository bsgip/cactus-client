from functools import lru_cache

import apluggy

from cactus_client.admin.plugins import AdminSpec, DefaultAdminPlugin, project_name


@lru_cache(maxsize=None)
def get_plugin_manager() -> apluggy.PluginManager:
    """Returns the cached plugin manager for the admin plugin system."""
    pm = apluggy.PluginManager(project_name)
    pm.add_hookspecs(AdminSpec)
    pm.register(DefaultAdminPlugin())
    pm.load_setuptools_entrypoints(project_name)
    pm.check_pending()
    return pm
