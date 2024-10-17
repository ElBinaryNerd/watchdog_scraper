import os
import importlib
from app.plugins.plugin_base import PluginBase

class PluginManager:
    def __init__(self, plugins_directory="app/plugins"):
        self.plugins_directory = plugins_directory
        self.plugins = []
        self.load_plugins()

    def load_plugins(self):
        # Load plugins dynamically from the specified directory
        for plugin_name in os.listdir(self.plugins_directory):
            plugin_path = os.path.join(self.plugins_directory, plugin_name)
            if os.path.isdir(plugin_path):
                entry_file = os.path.join(plugin_path, f"{plugin_name}.py")
                if os.path.isfile(entry_file):
                    module_name = f"app.plugins.{plugin_name}.{plugin_name}"
                    module = importlib.import_module(module_name)
                    plugin_class = getattr(module, plugin_name.capitalize(), None)
                    if plugin_class and issubclass(plugin_class, PluginBase):
                        self.plugins.append(plugin_class())

    def process_html(self, html):
        # Process the HTML through all loaded plugins
        data = {}
        for plugin in self.plugins:
            result = plugin.process(html)
            if result:
                data.update(result)
        return data
