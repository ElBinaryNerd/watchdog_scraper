# global_vars.py

from app.plugins.plugin_manager import PluginManager
from app.proxies.proxy_manager import ProxyManager
from app.proxies.proxy_list import proxy_list

# Declare and instantiate the required global vars
# This prevents redoing unnecessary operations, like 
# Reading files on each iteration and thread.
plugin_manager = PluginManager()

# Instantiates the central hub for smart rotation of 
# proxies depending on their performance.
proxy_manager = ProxyManager(proxy_list)