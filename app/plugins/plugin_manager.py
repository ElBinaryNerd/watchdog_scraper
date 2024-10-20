import os
import importlib
from dotenv import load_dotenv
from app.plugins.plugin_base import PluginBase

class PluginManager:
    def __init__(self, plugins_directory="app/plugins"):
        load_dotenv() 
        self.plugins_directory = plugins_directory
        self.plugins = []
        
        # Get the list of enabled plugins from the .env file
        enabled_plugins = os.getenv('ENABLED_PLUGINS', '')
        self.enabled_plugins = [p.strip() for p in enabled_plugins.split(',')] if enabled_plugins else []
        self.load_plugins()

    def load_plugins(self):
        # Load plugins dynamically from the specified directory
        for plugin_name in os.listdir(self.plugins_directory):
            plugin_path = os.path.join(self.plugins_directory, plugin_name)
            if os.path.isdir(plugin_path):
                # Check if the plugin is enabled in the .env file
                if plugin_name not in self.enabled_plugins:
                    continue

                entry_file = os.path.join(plugin_path, f"{plugin_name}.py")
                if os.path.isfile(entry_file):
                    module_name = f"app.plugins.{plugin_name}.{plugin_name}"
                    module = importlib.import_module(module_name)
                    
                    # Capitalize the class name and ensure it's a subclass of PluginBase
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
