# app/plugins/plugin_sample/plugin_sample.py
from app.plugins.plugin_base import PluginBase

class Plugin_sample(PluginBase):
    def process(self, html):
        # Returning a static dummy value, ignoring the HTML input
        return {"plugin_sample_output": "Hello world!"}
