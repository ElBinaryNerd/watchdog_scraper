# global_vars.py

from app.plugins.plugin_manager import PluginManager

# Declare and instantiate the required global vars
# This prevents redoing unnecessary operations, like 
# Reading files on each iteration and thread.
plugin_manager = PluginManager()
